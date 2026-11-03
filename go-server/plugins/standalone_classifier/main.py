import json
import logging
import os
import time

import torch

import media_downloader_client
import rabbitMQ_standalone

from ultralytics import YOLO

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
)
logger = logging.getLogger(__name__)

# =========================
# CONFIG
# =========================
PLUGIN_NAME    = "classifier"
MODEL_PATH     = "yolov8s-world.pt"
CONF_THRESHOLD = 0.40

if torch.cuda.is_available():
    DEVICE = 0
    logger.info("GPU detected: %s (CUDA %s)", torch.cuda.get_device_name(0), torch.version.cuda)
else:
    DEVICE = "cpu"
    logger.info("No CUDA GPU available – falling back to CPU.")

RABBITMQ_PUBLISH_KEY = "tagging.not_added.1.Image Classification"

# Labels that make the plugin publish a people_detected message,
# which the face recognition plugin consumes
HUMAN_LABELS = {"person", "man", "woman", "child", "face", "hand"}
PEOPLE_DETECTED_KEY = "people_detected"

# =========================
# TEXT LABELS
# =========================
LABELS_FILE = os.path.join(os.path.dirname(__file__), "text_labels.json")


def load_text_labels(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        labels = json.load(f)
    if not isinstance(labels, list) or not all(isinstance(l, str) for l in labels):
        raise ValueError("text_labels.json must contain a JSON array of strings.")
    logger.info("Loaded %d labels from %s", len(labels), path)
    return labels


TEXT_LABELS = load_text_labels(LABELS_FILE)

# =========================
# BATCHING
# =========================
# Up to BATCH_SIZE media are run through YOLO in one inference call, which keeps
# the GPU busy instead of feeding it one image at a time. After the first media
# arrives, the plugin waits at most BATCH_MAX_WAIT seconds for more.
# BATCH_SIZE=1 gives the unbatched behaviour, for comparison.
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "8"))
BATCH_MAX_WAIT = float(os.environ.get("BATCH_MAX_WAIT", "2"))

model = None


def predict(media_paths: list[str]):
    """Run YOLO on all paths in one batch, returns one result per path."""
    return model(
        media_paths,
        batch=len(media_paths),
        show=False,
        save=False,
        stream=False,
        device=DEVICE,
        conf=CONF_THRESHOLD,
        verbose=False,
    )


# =========================
# HANDLER
# =========================
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
    with media_downloader_client.local_medias(medias) as available:
        if not available:
            return
        logger.info("Processing batch of %d: IDs=%s", len(available), [m[0] for m in available])

        try:
            results = predict([media_path for _, _, media_path in available])
        except Exception as e:
            # One unreadable image fails the whole batch, so retry the media
            # one by one to only lose the broken one
            logger.warning("Batch inference failed (%s), retrying the %d media one by one", e, len(available))
            results = []
            for media_id, _, media_path in available:
                try:
                    results.append(predict([media_path])[0])
                except Exception as e:
                    logger.error("Inference failed for ID=%s: %s", media_id, e)
                    results.append(None)

    for (media_id, media_uri, _), result in zip(available, results):
        if result is not None:
            publish_detections(media_id, media_uri, result)

    logger.info("Batch of %d done in %ss", len(available), round(time.time() - t0, 2))


def publish_detections(media_id, media_uri, result):
    seen_labels = set()
    for box in result.boxes:
        name = result.names[int(box.cls[0])]
        if name not in seen_labels:
            seen_labels.add(name)
            rabbitMQ_standalone.publish(
                RABBITMQ_PUBLISH_KEY,
                json.dumps({"taggingValue": name, "mediaID": media_id}),
            )

    logger.info("ID=%s: %d detections – labels=%s", media_id, len(result.boxes), list(seen_labels))

    # ── Produce work for the face recognition plugin ─────────────────────
    if seen_labels & HUMAN_LABELS:
        rabbitMQ_standalone.publish(
            PEOPLE_DETECTED_KEY,
            json.dumps({"ID": media_id, "MediaURI": media_uri}),
        )
        logger.info("Human detected in ID=%s (labels=%s), published %s",
                    media_id, seen_labels & HUMAN_LABELS, PEOPLE_DETECTED_KEY)


# =========================
# MAIN
# =========================
def main():
    global model
    logger.info("Starting standalone YOLO-World Plugin...")

    model = YOLO(MODEL_PATH)
    model.set_classes(TEXT_LABELS)
    logger.info("Model loaded, batch size %d, max wait %ss.", BATCH_SIZE, BATCH_MAX_WAIT)

    rabbitMQ_standalone.consume_batch(
        PLUGIN_NAME, ["media.*"], handle_batch, batch_size=BATCH_SIZE, max_wait=BATCH_MAX_WAIT
    )


if __name__ == "__main__":
    main()
