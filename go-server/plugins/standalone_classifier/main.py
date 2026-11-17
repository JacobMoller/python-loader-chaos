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

model = None


# =========================
# HANDLER
# =========================
def handle_media(body: bytes, routing_key: str):
    # media.* messages are published by the server as {"ID", "MediaURI", "ThumbnailURI"}
    data = json.loads(body)
    media_id = data.get("ID")
    media_uri = data.get("MediaURI")
    if not media_id or not media_uri:
        logger.warning("Malformed message, skipping: %s", data)
        return

    t0 = time.time()
    logger.info("Processing ID=%s URI=%s", media_id, media_uri)

    with media_downloader_client.local_media(media_uri) as media_path:
        results = model(
            media_path,
            show=False,
            save=False,
            stream=False,
            device=DEVICE,
            conf=CONF_THRESHOLD,
            verbose=False,
        )

    num_detections = 0
    seen_labels = set()
    for result in results:
        for box in result.boxes:
            num_detections += 1
            name = result.names[int(box.cls[0])]
            if name not in seen_labels:
                seen_labels.add(name)
                rabbitMQ_standalone.publish(
                    RABBITMQ_PUBLISH_KEY,
                    json.dumps({"taggingValue": name, "mediaID": media_id}),
                )

    logger.info(
        "ID=%s: %d detections in %ss – labels=%s",
        media_id, num_detections, round(time.time() - t0, 2), list(seen_labels),
    )

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
    logger.info("Model loaded.")

    rabbitMQ_standalone.consume(PLUGIN_NAME, ["media.*"], handle_media)


if __name__ == "__main__":
    main()
