import json
import time
import threading
import queue
import logging
from collections import defaultdict
import os

import torch
import pika

import rabbitMQ_helpers

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
MODEL_PATH     = "yolov8s-world.pt"
CONF_THRESHOLD = 0.40
PLUGIN_NAME    = "yolo"

if torch.cuda.is_available():
    DEVICE = 0
    logger.info("GPU detected: %s (CUDA %s)", torch.cuda.get_device_name(0), torch.version.cuda)
else:
    DEVICE = "cpu"
    logger.info("No CUDA GPU available – falling back to CPU.")

RABBITMQ_PUBLISH_KEY = "tagging.not_added.1.Image Classification"
NUM_WORKERS = 1

# Labels that trigger face recognition dispatch
HUMAN_LABELS = {"person", "man", "woman", "child", "face", "hand"}

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
# GLOBAL STATE
# =========================
stats_lock = threading.Lock()
global_stats = {
    "processed_images": 0,
    "total_detections": 0,
    "detections_by_label": defaultdict(int),
    "faces_requested": 0,
}

work_queue: queue.Queue = queue.Queue()


# =========================
# YOLO WORKER
# =========================
def yolo_worker(worker_id: int):
    logger.info("Worker %d started.", worker_id)

    pub_channel, pub_connection = rabbitMQ_helpers.producer_connection_init()
    logger.info("Worker %d – publish connection established.", worker_id)

    model = YOLO(MODEL_PATH)
    model.set_classes(TEXT_LABELS)
    logger.info("Worker %d – model loaded.", worker_id)

    def safe_publish(channel, conn, routing_key, payload):
        """Publish with reconnect on failure. Returns (channel, connection)."""
        nonlocal pub_channel, pub_connection
        try:
            rabbitMQ_helpers.publish_message(channel, routing_key, payload)
            return channel, conn
        except Exception as err:
            logger.warning("Worker %d – publish failed (%s), reconnecting: %s", worker_id, routing_key, err)
            try:
                rabbitMQ_helpers.connection_end(conn, channel)
            except Exception:
                pass
            new_ch, new_conn = rabbitMQ_helpers.producer_connection_init()
            rabbitMQ_helpers.publish_message(new_ch, routing_key, payload)
            return new_ch, new_conn

    while True:
        try:
            item = work_queue.get(timeout=5)
        except queue.Empty:
            continue

        if item is None:
            logger.info("Worker %d – shutting down.", worker_id)
            work_queue.task_done()
            break

        media_id = item["media_id"]
        media_path = item["media_path"]

        t0 = time.time()
        logger.info("Worker %d – processing ID=%s", worker_id, media_id)

        try:
            # ── YOLO-World inference ─────────────────────────────────────
            results = model(
                media_path,
                show=False,
                save=False,
                stream=False,
                device=DEVICE,
                conf=CONF_THRESHOLD,
                verbose=False,
            )

            detections = []
            seen_labels = set()

            for result in results:
                for box in result.boxes:
                    prob = float(box.conf[0])
                    class_id = int(box.cls[0])
                    name = result.names[class_id]
                    xyxy = box.xyxy[0].tolist()

                    detection = {
                        "label": name,
                        "score": round(prob, 6),
                        "percentage": round(prob * 100, 2),
                        "box": [round(float(x), 2) for x in xyxy],
                    }
                    detections.append(detection)

                    if name not in seen_labels:
                        seen_labels.add(name)
                        tag_payload = json.dumps({
                            "taggingValue": name,
                            "mediaID": media_id,
                        })
                        pub_channel, pub_connection = safe_publish(
                            pub_channel, pub_connection, RABBITMQ_PUBLISH_KEY, tag_payload
                        )

            elapsed = round(time.time() - t0, 2)
            logger.info(
                "Worker %d – ID=%s: %d detections in %ss – labels=%s",
                worker_id, media_id, len(detections), elapsed, list(seen_labels),
            )

            with stats_lock:
                global_stats["processed_images"] += 1
                global_stats["total_detections"] += len(detections)
                for d in detections:
                    global_stats["detections_by_label"][d["label"]] += 1

            # ── Request face recognition if human detected ───────────────
            # IMPORTANT: must be published BEFORE the ACK so the orchestrator
            # increments expected_acks before processing our done signal.
            if seen_labels & HUMAN_LABELS:
                request_payload = json.dumps({"mediaID": media_id})
                pub_channel, pub_connection = safe_publish(
                    pub_channel, pub_connection,
                    f"process.request_faces.{media_id}",
                    request_payload,
                )
                with stats_lock:
                    global_stats["faces_requested"] += 1
                logger.info(
                    "Worker %d – human detected in ID=%s (labels=%s), requested face recognition",
                    worker_id, media_id, seen_labels & HUMAN_LABELS,
                )

            # ── ACK to orchestrator ──────────────────────────────────────
            ack_payload = json.dumps({
                "mediaID": media_id,
                "plugin": PLUGIN_NAME,
            })
            pub_channel, pub_connection = safe_publish(
                pub_channel, pub_connection,
                f"process.done.{media_id}",
                ack_payload,
            )

            logger.info("Worker %d – done with ID=%s", worker_id, media_id)

        except Exception as exc:
            logger.error("Worker %d – unexpected error for ID=%s: %s", worker_id, media_id, exc)
            # Still ACK on error so orchestrator doesn't hang
            try:
                ack_payload = json.dumps({"mediaID": media_id, "plugin": PLUGIN_NAME})
                pub_channel, pub_connection = safe_publish(
                    pub_channel, pub_connection,
                    f"process.done.{media_id}",
                    ack_payload,
                )
            except Exception:
                logger.error("Worker %d – could not send error ACK for ID=%s", worker_id, media_id)

        finally:
            work_queue.task_done()


# =========================
# CONSUME CONNECTION
# =========================
HEALTH_CHECK_INTERVAL = 30
RECONNECT_DELAY = 5


def create_consume_connection():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitMQ_helpers.RABBITMQ_HOST,
            port=int(rabbitMQ_helpers.RABBITMQ_PORT),
            credentials=pika.PlainCredentials(
                rabbitMQ_helpers.RABBITMQ_USER,
                rabbitMQ_helpers.RABBITMQ_PASS,
            ),
            heartbeat=60,
            blocked_connection_timeout=300,
        )
    )
    channel = connection.channel()
    channel.exchange_declare(
        exchange=rabbitMQ_helpers.EXCHANGE_NAME,
        exchange_type="topic",
        durable=True,
    )
    result = channel.queue_declare("", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(
        exchange=rabbitMQ_helpers.EXCHANGE_NAME,
        queue=queue_name,
        routing_key="process.yolo",
    )
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=rabbitmq_callback,
        auto_ack=True,
    )
    logger.info("Consume connection established (routing_key=process.yolo)")
    return connection, channel


def rabbitmq_callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        media_id = data.get("ID")
        media_path = data.get("MediaPath")

        if not media_id or not media_path:
            logger.warning("Malformed message, skipping: %s", data)
            return

        work_queue.put({
            "media_id": media_id,
            "media_path": media_path,
        })
    except Exception as exc:
        logger.error("Error in callback: %s", exc)


# =========================
# MAIN
# =========================
def main():
    logger.info("Starting YOLO-World Plugin (with conditional face dispatch)...")

    workers = []
    for wid in range(NUM_WORKERS):
        t = threading.Thread(
            target=yolo_worker, args=(wid,), name=f"YoloWorker-{wid}", daemon=True
        )
        t.start()
        workers.append(t)

    consume_connection = None
    consume_channel = None

    try:
        while True:
            if consume_connection is None or consume_connection.is_closed:
                try:
                    consume_connection, consume_channel = create_consume_connection()
                    last_health = time.time()
                    logger.info("Listening on 'process.yolo'...")
                except pika.exceptions.AMQPConnectionError as e:
                    logger.error("RabbitMQ connect failed: %s – retrying in %ds", e, RECONNECT_DELAY)
                    time.sleep(RECONNECT_DELAY)
                    continue

            try:
                consume_connection.process_data_events(time_limit=1)
            except pika.exceptions.StreamLostError:
                logger.warning("Stream lost – reconnecting...")
                consume_connection = None
                continue
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning("Connection error: %s – reconnecting...", e)
                consume_connection = None
                continue
            except Exception as e:
                logger.error("Unexpected error: %s – reconnecting...", e)
                consume_connection = None
                time.sleep(RECONNECT_DELAY)
                continue

            now = time.time()
            if now - last_health >= HEALTH_CHECK_INTERVAL:
                logger.info(
                    "Health: processed=%d, detections=%d, faces_requested=%d, work_queue=%d",
                    global_stats["processed_images"],
                    global_stats["total_detections"],
                    global_stats["faces_requested"],
                    work_queue.qsize(),
                )
                last_health = now

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        for _ in workers:
            work_queue.put(None)
        for t in workers:
            t.join(timeout=30)

        if consume_channel and not consume_channel.is_closed:
            try:
                consume_channel.close()
            except Exception:
                pass
        if consume_connection and not consume_connection.is_closed:
            try:
                consume_connection.close()
            except Exception:
                pass

        logger.info(
            "Done – %d images, %d detections, %d face requests.",
            global_stats["processed_images"],
            global_stats["total_detections"],
            global_stats["faces_requested"],
        )


if __name__ == "__main__":
    main()
