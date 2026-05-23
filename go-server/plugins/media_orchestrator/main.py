import json
import time
import threading
import logging
import queue
import os

import pika
import grpc

import media_downloader_pb2
import media_downloader_pb2_grpc
import rabbitMQ_helpers

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
BASE_EXPECTED_ACKS = int(os.environ.get("BASE_EXPECTED_ACKS", "3"))  # exif + caption + yolo
ACK_TIMEOUT        = int(os.environ.get("ACK_TIMEOUT", "300"))       # seconds before giving up on a media

GRPC_TARGET      = os.environ.get("GRPC_TARGET", "media-downloader:50052")
MAX_GRPC_RETRIES = 3
GRPC_RETRY_DELAY = 2

RECONNECT_DELAY       = 5
HEALTH_CHECK_INTERVAL = 30

# ── Standalone plugin switches ──────────────────────────────────────
# When a plugin runs in standalone mode it listens directly on media.*
# and handles download/release itself.  The orchestrator must NOT
# dispatch to it and must expect one fewer ACK.
EXIF_STANDALONE = os.environ.get("EXIF_STANDALONE", "false").lower() in ("true", "1", "yes")

# Build dispatch keys dynamically based on which plugins are orchestrated
_ALL_DISPATCH_KEYS = ["process.exif", "process.caption", "process.yolo"]
DISPATCH_KEYS = [k for k in _ALL_DISPATCH_KEYS if not (k == "process.exif" and EXIF_STANDALONE)]

# Adjust expected ACKs: subtract 1 for each standalone plugin
_standalone_count = sum([EXIF_STANDALONE])
EFFECTIVE_BASE_ACKS = max(BASE_EXPECTED_ACKS - _standalone_count, 0)

# =========================
# gRPC
# =========================
_grpc_stub = None


def get_grpc_stub() -> media_downloader_pb2_grpc.MediaDownloaderStub:
    global _grpc_stub
    if _grpc_stub is None:
        channel = grpc.insecure_channel(GRPC_TARGET)
        _grpc_stub = media_downloader_pb2_grpc.MediaDownloaderStub(channel)
        logger.info("gRPC stub created -> %s", GRPC_TARGET)
    return _grpc_stub


def reset_grpc_stub():
    global _grpc_stub
    _grpc_stub = None


def request_media(media_uri: str) -> str | None:
    stub = get_grpc_stub()
    for attempt in range(1, MAX_GRPC_RETRIES + 1):
        try:
            resp = stub.RequestMedia(
                media_downloader_pb2.RequestMediaRequest(media_uri=media_uri),
                timeout=30,
            )
            return resp.media_path
        except grpc.RpcError as e:
            code = e.code() if hasattr(e, "code") else None
            if code == grpc.StatusCode.NOT_FOUND:
                logger.warning("NOT_FOUND for URI=%s – skipping", media_uri)
                return None
            logger.warning(
                "gRPC error (attempt %d/%d) for URI=%s: %s",
                attempt, MAX_GRPC_RETRIES, media_uri, e,
            )
            if attempt == MAX_GRPC_RETRIES:
                reset_grpc_stub()
            else:
                time.sleep(GRPC_RETRY_DELAY * attempt)
    return None


def release_media(media_uri: str):
    stub = get_grpc_stub()
    try:
        stub.ReleaseMedia(
            media_downloader_pb2.ReleaseMediaRequest(media_uri=media_uri)
        )
        logger.info("Released media: %s", media_uri)
    except grpc.RpcError as e:
        logger.warning("ReleaseMedia failed for URI=%s: %s", media_uri, e)


# =========================
# THREAD-SAFE QUEUES
# =========================
# Incoming media messages buffered here
media_queue: queue.Queue = queue.Queue()

# ACK signals from plugins
ack_queue: queue.Queue = queue.Queue()

# Face recognition requests from YOLO
face_request_queue: queue.Queue = queue.Queue()


# =========================
# RABBITMQ CALLBACKS (run in consumer thread)
# =========================
def on_media_message(ch, method, properties, body):
    try:
        data = json.loads(body)
        media_id = data.get("ID")
        media_uri = data.get("MediaURI")
        if media_id and media_uri:
            media_queue.put({"ID": media_id, "MediaURI": media_uri})
        else:
            logger.warning("Malformed media message: %s", data)
    except Exception as e:
        logger.error("Error in on_media_message: %s", e)


def on_ack_message(ch, method, properties, body):
    try:
        data = json.loads(body)
        ack_queue.put(data)
    except Exception as e:
        logger.error("Error in on_ack_message: %s", e)


def on_request_faces(ch, method, properties, body):
    try:
        data = json.loads(body)
        face_request_queue.put(data)
    except Exception as e:
        logger.error("Error in on_request_faces: %s", e)


# =========================
# CONSUMER THREAD
# =========================
def consumer_thread_fn():
    """Runs pika consume loop in a dedicated thread."""
    while True:
        try:
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

            # media.* queue
            result = channel.queue_declare("", exclusive=True)
            q = result.method.queue
            channel.queue_bind(exchange=rabbitMQ_helpers.EXCHANGE_NAME, queue=q, routing_key="media.*")
            channel.basic_consume(queue=q, on_message_callback=on_media_message, auto_ack=True)

            # process.done.* queue
            result2 = channel.queue_declare("", exclusive=True)
            q2 = result2.method.queue
            channel.queue_bind(exchange=rabbitMQ_helpers.EXCHANGE_NAME, queue=q2, routing_key="process.done.*")
            channel.basic_consume(queue=q2, on_message_callback=on_ack_message, auto_ack=True)

            # process.request_faces.* queue
            result3 = channel.queue_declare("", exclusive=True)
            q3 = result3.method.queue
            channel.queue_bind(exchange=rabbitMQ_helpers.EXCHANGE_NAME, queue=q3, routing_key="process.request_faces.*")
            channel.basic_consume(queue=q3, on_message_callback=on_request_faces, auto_ack=True)

            logger.info("Consumer thread connected (media.* + process.done.* + process.request_faces.*)")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("Consumer thread connection error: %s – retrying in %ds", e, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)
        except Exception as e:
            logger.error("Consumer thread error: %s – retrying in %ds", e, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)


# =========================
# SEQUENTIAL PROCESSING LOOP (main thread)
# =========================
def process_loop():
    """Main loop: process one media at a time, sequentially."""
    pub_channel, pub_connection = rabbitMQ_helpers.producer_connection_init()
    logger.info("Publish connection established.")

    stats = {
        "dispatched": 0,
        "completed": 0,
        "skipped": 0,
        "timed_out": 0,
        "faces_requested": 0,
    }
    last_health = time.time()

    while True:
        # ── Health check while idle ──────────────────────────────────
        now = time.time()
        if now - last_health >= HEALTH_CHECK_INTERVAL:
            logger.info(
                "Health: dispatched=%d, completed=%d, skipped=%d, timed_out=%d, faces=%d, queued=%d",
                stats["dispatched"], stats["completed"], stats["skipped"],
                stats["timed_out"], stats["faces_requested"], media_queue.qsize(),
            )
            last_health = now

        # ── Get next media (blocking with timeout for health checks) ─
        try:
            item = media_queue.get(timeout=5)
        except queue.Empty:
            continue

        media_id = item["ID"]
        media_uri = item["MediaURI"]

        logger.info("Processing media ID=%s URI=%s (queued=%d)", media_id, media_uri, media_queue.qsize())

        # ── 1. Download ──────────────────────────────────────────────
        media_path = request_media(media_uri)
        if media_path is None:
            logger.warning("Skipping media ID=%s (download failed)", media_id)
            stats["skipped"] += 1
            continue

        # ── 2. Dispatch to base plugins ──────────────────────────────
        dispatch_payload = json.dumps({
            "ID": media_id,
            "MediaURI": media_uri,
            "MediaPath": media_path,
        })
        for key in DISPATCH_KEYS:
            try:
                rabbitMQ_helpers.publish_message(pub_channel, key, dispatch_payload)
            except Exception as e:
                logger.warning("Publish failed (%s), reconnecting: %s", key, e)
                try:
                    rabbitMQ_helpers.connection_end(pub_connection, pub_channel)
                except Exception:
                    pass
                pub_channel, pub_connection = rabbitMQ_helpers.producer_connection_init()
                rabbitMQ_helpers.publish_message(pub_channel, key, dispatch_payload)

        expected_acks = EFFECTIVE_BASE_ACKS
        received_acks = 0
        ack_plugins = set()
        stats["dispatched"] += 1

        logger.info("Dispatched media ID=%s to %d plugins %s, waiting for %d ACKs...",
                     media_id, len(DISPATCH_KEYS), DISPATCH_KEYS, expected_acks)

        # ── 3. Drain stale messages from previous cycles ─────────────
        _drain_queue(ack_queue)
        _drain_queue(face_request_queue)

        # ── 4. Wait for all ACKs ─────────────────────────────────────
        deadline = time.time() + ACK_TIMEOUT

        while received_acks < expected_acks:
            remaining = deadline - time.time()
            if remaining <= 0:
                logger.error(
                    "TIMEOUT: media ID=%s got %d/%d ACKs (from %s) after %ds – releasing anyway.",
                    media_id, received_acks, expected_acks, ack_plugins, ACK_TIMEOUT,
                )
                stats["timed_out"] += 1
                break

            # Check for face recognition requests from YOLO
            try:
                face_req = face_request_queue.get_nowait()
                req_media_id = str(face_req.get("mediaID", ""))
                if req_media_id == str(media_id):
                    expected_acks += 1
                    stats["faces_requested"] += 1
                    # Dispatch face recognition
                    try:
                        rabbitMQ_helpers.publish_message(pub_channel, "process.faces", dispatch_payload)
                    except Exception as e:
                        logger.warning("Publish process.faces failed, reconnecting: %s", e)
                        try:
                            rabbitMQ_helpers.connection_end(pub_connection, pub_channel)
                        except Exception:
                            pass
                        pub_channel, pub_connection = rabbitMQ_helpers.producer_connection_init()
                        rabbitMQ_helpers.publish_message(pub_channel, "process.faces", dispatch_payload)
                    logger.info(
                        "Face recognition requested for ID=%s (expected_acks now %d)",
                        media_id, expected_acks,
                    )
            except queue.Empty:
                pass

            # Check for ACKs
            try:
                ack = ack_queue.get(timeout=min(0.5, remaining))
                ack_media_id = str(ack.get("mediaID", ""))
                ack_plugin = ack.get("plugin", "unknown")

                if ack_media_id == str(media_id):
                    received_acks += 1
                    ack_plugins.add(ack_plugin)
                    logger.info(
                        "ACK %d/%d for media ID=%s from plugin=%s",
                        received_acks, expected_acks, media_id, ack_plugin,
                    )
                else:
                    # Stale ACK from a previous media – discard
                    logger.debug("Discarding stale ACK for media ID=%s (current=%s)", ack_media_id, media_id)
            except queue.Empty:
                pass

        # ── 5. Release ───────────────────────────────────────────────
        release_media(media_uri)

        if received_acks >= expected_acks:
            stats["completed"] += 1
            logger.info(
                "All %d ACKs received for media ID=%s from %s – released.",
                expected_acks, media_id, ack_plugins,
            )
        else:
            logger.warning(
                "Released media ID=%s with only %d/%d ACKs (from %s).",
                media_id, received_acks, expected_acks, ack_plugins,
            )


def _drain_queue(q: queue.Queue):
    """Empty a queue of any leftover messages."""
    drained = 0
    while True:
        try:
            q.get_nowait()
            drained += 1
        except queue.Empty:
            break
    if drained > 0:
        logger.debug("Drained %d stale messages from queue", drained)


# =========================
# MAIN
# =========================
def main():
    logger.info(
        "Starting Media Orchestrator (sequential, base ACKs=%d, effective ACKs=%d, EXIF_STANDALONE=%s)...",
        BASE_EXPECTED_ACKS, EFFECTIVE_BASE_ACKS, EXIF_STANDALONE,
    )

    # Start consumer thread (fills media_queue, ack_queue, face_request_queue)
    consumer = threading.Thread(
        target=consumer_thread_fn, name="ConsumerThread", daemon=True
    )
    consumer.start()

    # Give the consumer a moment to connect
    time.sleep(2)

    # Run sequential processing on the main thread
    try:
        process_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()
