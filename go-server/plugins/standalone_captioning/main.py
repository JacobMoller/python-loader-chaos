import json
import logging
import multiprocessing
import os
import time

import torch

import media_downloader_client
import rabbitMQ_standalone

from lavis.models import load_model_and_preprocess
from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PLUGIN_NAME = "captioning"
PROCESSING_TIMEOUT = 120  # seconds
MAX_RETRIES = 2
MAX_IMAGE_SIZE = 1024  # max pixels on longest side

# Up to BATCH_SIZE media are captioned with one generate call, which keeps the
# GPU busy instead of feeding it one image at a time. After the first media
# arrives, the plugin waits at most BATCH_MAX_WAIT seconds for more.
# BATCH_SIZE=1 gives the unbatched behaviour, for comparison.
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "8"))
BATCH_MAX_WAIT = float(os.environ.get("BATCH_MAX_WAIT", "2"))

# "spawn" instead of the default "fork": forking a process that runs other
# threads (the RabbitMQ connection thread) can deadlock the child
mp_context = multiprocessing.get_context("spawn")

device_str = "cuda" if torch.cuda.is_available() else "cpu"
device = torch.device(device_str)
model = None
vis_processors = None


def safe_load_image(media_path):
    """Load and resize image to prevent VRAM overflow."""
    raw_image = Image.open(media_path)
    raw_image.verify()
    raw_image = Image.open(media_path).convert("RGB")

    if max(raw_image.size) > MAX_IMAGE_SIZE:
        raw_image.thumbnail((MAX_IMAGE_SIZE, MAX_IMAGE_SIZE))

    return raw_image


def run_inference(media_path, device_str, result_queue):
    """Runs in a separate process so it can be killed on timeout."""
    try:
        device = torch.device(device_str)
        model, vis_processors, _ = load_model_and_preprocess(
            name="blip_caption", model_type="base_coco", is_eval=True, device=device
        )
        raw_image = safe_load_image(media_path)
        image_tensor = vis_processors["eval"](raw_image).unsqueeze(0).to(device)

        with torch.no_grad():
            caption = model.generate(
                {"image": image_tensor},
                max_length=40,
                num_beams=3,
            )[0]

        result_queue.put({"ok": True, "caption": caption})
    except Exception as e:
        result_queue.put({"ok": False, "error": str(e)})
    finally:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()


def process_image_subprocess(media_path):
    """Run inference in a subprocess that can be killed on timeout."""
    result_queue = mp_context.Queue()
    proc = mp_context.Process(
        target=run_inference, args=(media_path, device_str, result_queue)
    )
    proc.start()
    proc.join(timeout=PROCESSING_TIMEOUT)

    if proc.is_alive():
        logger.warning("Subprocess timed out, killing it.")
        proc.kill()
        proc.join()
        raise Exception("Processing timed out (subprocess killed)")

    if result_queue.empty():
        raise Exception("Subprocess exited without result")

    result = result_queue.get()
    if not result["ok"]:
        raise Exception(result["error"])

    return result["caption"]


def process_image_inline(media_path):
    """Fast path: run inference in the main process."""
    raw_image = safe_load_image(media_path)
    image_tensor = vis_processors["eval"](raw_image).unsqueeze(0).to(device)

    with torch.no_grad():
        caption = model.generate(
            {"image": image_tensor},
            max_length=40,
            num_beams=3,
        )[0]

    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    return caption


def caption_image(media_id, media_path):
    # Attempt 1: inline (fast, no subprocess overhead)
    try:
        return process_image_inline(media_path)
    except Exception as e:
        logger.warning("Inline attempt failed for ID=%s: %s", media_id, e)

    # Retries in subprocess (killable on timeout)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Subprocess attempt %d/%d for ID=%s", attempt, MAX_RETRIES, media_id)
            return process_image_subprocess(media_path)
        except Exception as e:
            logger.warning("Subprocess attempt %d/%d failed for ID=%s: %s", attempt, MAX_RETRIES, media_id, e)
    return None


def caption_batch(images: list):
    """Caption several images with one generate call. The eval processor resizes
    every image to the same input size, so the tensors can be stacked."""
    image_tensors = torch.stack([vis_processors["eval"](image) for image in images]).to(device)

    with torch.no_grad():
        captions = model.generate(
            {"image": image_tensors},
            max_length=40,
            num_beams=3,
        )

    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    return captions


def handle_batch(messages: list[rabbitMQ_standalone.Message]):
    medias = []
    for message in messages:
        # media.* messages are published by the server as {"ID", "MediaURI", "ThumbnailURI"}
        data = json.loads(message.body)
        media_id = data.get("ID")
        media_uri = data.get("MediaURI")
        if not media_id or not media_uri:
            logger.warning("Malformed message, skipping: %s", data)
            continue
        medias.append((media_id, media_uri))

    t0 = time.time()
    captions = {}  # media_id -> caption
    with media_downloader_client.local_medias(medias) as available:
        # Load the images one by one, so an unreadable image only fails itself
        loaded = []  # (media_id, media_path, image)
        for media_id, _, media_path in available:
            try:
                loaded.append((media_id, media_path, safe_load_image(media_path)))
            except Exception as e:
                logger.warning("Could not load image ID=%s: %s", media_id, e)

        if loaded:
            logger.info("Processing batch of %d: IDs=%s", len(loaded), [m[0] for m in loaded])
            try:
                batch_captions = caption_batch([image for _, _, image in loaded])
                captions = {media_id: caption for (media_id, _, _), caption in zip(loaded, batch_captions)}
            except Exception as e:
                # e.g. out of GPU memory: fall back to the per-image path with its retries
                logger.warning("Batch inference failed (%s), captioning the %d media one by one", e, len(loaded))
                for media_id, media_path, _ in loaded:
                    captions[media_id] = caption_image(media_id, media_path)

    for media_id, _ in medias:
        caption = captions.get(media_id)
        if caption is None:
            logger.error("No caption for ID=%s, skipping.", media_id)
            continue
        rabbitMQ_standalone.publish(
            "tagging.not_added.1.Caption",
            json.dumps({"taggingValue": caption, "mediaID": media_id}),
        )
        logger.info("Caption: '%s' for ID=%s", caption, media_id)

    logger.info("Batch of %d done in %ss", len(medias), round(time.time() - t0, 2))


def main():
    global model, vis_processors
    logger.info("Starting standalone Captioning Plugin (GPU-safe)...")
    logger.info("Using device: %s", device)

    logger.info("Loading BLIP model...")
    model, vis_processors, _ = load_model_and_preprocess(
        name="blip_caption", model_type="base_coco", is_eval=True, device=device
    )
    logger.info("Model loaded on %s, batch size %d, max wait %ss.", device, BATCH_SIZE, BATCH_MAX_WAIT)

    rabbitMQ_standalone.consume_batch(
        PLUGIN_NAME, ["media.*"], handle_batch, batch_size=BATCH_SIZE, max_wait=BATCH_MAX_WAIT
    )


if __name__ == "__main__":
    main()
