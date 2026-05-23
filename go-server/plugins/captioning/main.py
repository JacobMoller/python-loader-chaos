import json
import logging
import multiprocessing

import torch

import rabbitMQ_helpers

from lavis.models import load_model_and_preprocess
from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PLUGIN_NAME = "caption"
PROCESSING_TIMEOUT = 120  # seconds
MAX_RETRIES = 2
MAX_IMAGE_SIZE = 1024  # max pixels on longest side

# Publish channel state
pub_channel = None
pub_connection = None


def get_pub_channel():
    """Return a valid publish channel, reconnecting if necessary."""
    global pub_channel, pub_connection
    if pub_channel is not None and pub_channel.is_open:
        return pub_channel
    logger.info("Pub channel closed or missing, reconnecting...")
    try:
        if pub_connection is not None and pub_connection.is_open:
            pub_connection.close()
    except Exception:
        pass
    pub_channel, pub_connection = rabbitMQ_helpers.producer_connection_init()
    logger.info("Pub channel reconnected.")
    return pub_channel


def safe_publish(routing_key: str, message: str):
    """Publish a message, reconnecting the channel if needed."""
    global pub_channel
    try:
        channel = get_pub_channel()
        rabbitMQ_helpers.publish_message(channel, routing_key, message)
    except Exception as e:
        logger.warning("Publish failed (%s), retrying with fresh connection...", e)
        pub_channel = None
        channel = get_pub_channel()
        rabbitMQ_helpers.publish_message(channel, routing_key, message)


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


def process_image_subprocess(media_path, device_str):
    """Run inference in a subprocess that can be killed on timeout."""
    result_queue = multiprocessing.Queue()
    proc = multiprocessing.Process(
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


def main():
    logger.info("Starting Captioning Plugin (GPU-safe)...")

    device_str = "cuda" if torch.cuda.is_available() else "cpu"
    device = torch.device(device_str)
    logger.info("Using device: %s", device)

    logger.info("Loading BLIP model...")
    model, vis_processors, _ = load_model_and_preprocess(
        name="blip_caption", model_type="base_coco", is_eval=True, device=device
    )
    logger.info("Model loaded on %s.", device)

    get_pub_channel()
    logger.info("Publish connection established.")

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

    def callback(ch, method, properties, body):
        data = json.loads(body)
        media_id = data["ID"]
        media_path = data["MediaPath"]

        logger.info("Processing media ID=%s path=%s", media_id, media_path)

        caption = None

        # Attempt 1: inline (fast, no subprocess overhead)
        try:
            caption = process_image_inline(media_path)
        except Exception as e:
            logger.warning("Inline attempt failed for ID=%s: %s", media_id, e)

        # Retries in subprocess (killable on timeout)
        if caption is None:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    logger.info(
                        "Subprocess attempt %d/%d for ID=%s",
                        attempt, MAX_RETRIES, media_id,
                    )
                    caption = process_image_subprocess(media_path, device_str)
                    break
                except Exception as e:
                    logger.warning(
                        "Subprocess attempt %d/%d failed for ID=%s: %s",
                        attempt, MAX_RETRIES, media_id, e,
                    )

        if caption is not None:
            safe_publish(
                "tagging.not_added.1.Caption",
                json.dumps({"taggingValue": caption, "mediaID": media_id}),
            )
            logger.info("Caption: '%s' for ID=%s", caption, media_id)
        else:
            logger.error(
                "All attempts failed for ID=%s, skipping.", media_id
            )

        send_ack(media_id)
        logger.info("Done with media ID=%s", media_id)

    logger.info("Listening on 'process.caption'...")
    rabbitMQ_helpers.listen("process.caption", callback)


def send_ack(media_id):
    safe_publish(
        f"process.done.{media_id}",
        json.dumps({"mediaID": media_id, "plugin": PLUGIN_NAME}),
    )


if __name__ == "__main__":
    main()
