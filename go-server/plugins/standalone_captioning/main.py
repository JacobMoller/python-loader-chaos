import json
import logging
import multiprocessing

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


def handle_media(body: bytes, routing_key: str):
    # media.* messages are published by the server as {"ID", "MediaURI", "ThumbnailURI"}
    data = json.loads(body)
    media_id = data.get("ID")
    media_uri = data.get("MediaURI")
    if not media_id or not media_uri:
        logger.warning("Malformed message, skipping: %s", data)
        return

    logger.info("Processing media ID=%s URI=%s", media_id, media_uri)

    with media_downloader_client.local_media(media_uri) as media_path:
        caption = caption_image(media_id, media_path)

    if caption is None:
        logger.error("All attempts failed for ID=%s, skipping.", media_id)
        return

    rabbitMQ_standalone.publish(
        "tagging.not_added.1.Caption",
        json.dumps({"taggingValue": caption, "mediaID": media_id}),
    )
    logger.info("Caption: '%s' for ID=%s", caption, media_id)


def main():
    global model, vis_processors
    logger.info("Starting standalone Captioning Plugin (GPU-safe)...")
    logger.info("Using device: %s", device)

    logger.info("Loading BLIP model...")
    model, vis_processors, _ = load_model_and_preprocess(
        name="blip_caption", model_type="base_coco", is_eval=True, device=device
    )
    logger.info("Model loaded on %s.", device)

    rabbitMQ_standalone.consume(PLUGIN_NAME, ["media.*"], handle_media)


if __name__ == "__main__":
    main()
