import json
import logging
import os

import grpc
import rabbitMQ_helpers
import dataloader_pb2
import dataloader_pb2_grpc

from exif import Image
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PLUGIN_NAME = "exif"

GRPC_HOST = os.getenv("GRPC_HOST", "go-server")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")

# ── Standalone mode ──────────────────────────────────────────────────
# When STANDALONE_MODE=true, the plugin listens directly on media.*,
# downloads/releases media itself via the media-downloader gRPC service,
# and does NOT send ACKs to the orchestrator.
STANDALONE_MODE = os.getenv("STANDALONE_MODE", "false").lower() in ("true", "1", "yes")

# media-downloader gRPC target (only used in standalone mode)
MEDIA_DOWNLOADER_TARGET = os.getenv("MEDIA_DOWNLOADER_TARGET", "media-downloader:50052")
MAX_GRPC_RETRIES = 3
GRPC_RETRY_DELAY = 2

# Publish channel state
pub_channel = None
pub_connection = None

# media-downloader stub (standalone mode only)
_downloader_stub = None


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


def get_grpc_stub():
    """Create and return a gRPC stub for the DataLoader service."""
    channel = grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
    stub = dataloader_pb2_grpc.DataLoaderStub(channel)
    return stub, channel


def get_tag_type_id(stub, tagset_name):
    """
    Query the gRPC server to get the tagTypeId for a given tagset name.
    Returns the tagTypeId if found, None otherwise.
    """
    try:
        response = stub.getTagSetByName(
            dataloader_pb2.GetTagSetRequestByName(name=tagset_name)
        )
        return response.tagTypeId
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            logger.warning("Tagset '%s' not found in database, skipping.", tagset_name)
        else:
            logger.error("gRPC error while fetching tagset '%s': %s", tagset_name, e)
        return None


# =========================
# MEDIA-DOWNLOADER gRPC (standalone mode only)
# =========================
def _get_downloader_stub():
    """Lazy-init a gRPC stub for the media-downloader service."""
    global _downloader_stub
    if _downloader_stub is None:
        import media_downloader_pb2 as md_pb2
        import media_downloader_pb2_grpc as md_grpc
        channel = grpc.insecure_channel(MEDIA_DOWNLOADER_TARGET)
        _downloader_stub = md_grpc.MediaDownloaderStub(channel)
        logger.info("media-downloader stub created -> %s", MEDIA_DOWNLOADER_TARGET)
    return _downloader_stub


def _reset_downloader_stub():
    global _downloader_stub
    _downloader_stub = None


def request_media(media_uri: str) -> str | None:
    """Download media via media-downloader gRPC. Returns local path or None."""
    import media_downloader_pb2 as md_pb2
    stub = _get_downloader_stub()
    for attempt in range(1, MAX_GRPC_RETRIES + 1):
        try:
            resp = stub.RequestMedia(
                md_pb2.RequestMediaRequest(media_uri=media_uri),
                timeout=30,
            )
            return resp.media_path
        except grpc.RpcError as e:
            code = e.code() if hasattr(e, "code") else None
            if code == grpc.StatusCode.NOT_FOUND:
                logger.warning("NOT_FOUND for URI=%s – skipping", media_uri)
                return None
            logger.warning(
                "gRPC download error (attempt %d/%d) URI=%s: %s",
                attempt, MAX_GRPC_RETRIES, media_uri, e,
            )
            if attempt == MAX_GRPC_RETRIES:
                _reset_downloader_stub()
            else:
                import time
                time.sleep(GRPC_RETRY_DELAY * attempt)
    return None


def release_media(media_uri: str):
    """Release media via media-downloader gRPC."""
    import media_downloader_pb2 as md_pb2
    stub = _get_downloader_stub()
    try:
        stub.ReleaseMedia(md_pb2.ReleaseMediaRequest(media_uri=media_uri))
        logger.info("Released media: %s", media_uri)
    except grpc.RpcError as e:
        logger.warning("ReleaseMedia failed for URI=%s: %s", media_uri, e)


def main():
    mode_label = "STANDALONE" if STANDALONE_MODE else "ORCHESTRATED"
    logger.info("Starting EXIF Extractor Plugin (mode=%s)...", mode_label)

    get_pub_channel()
    logger.info("Publish connection established.")

    # Connect to gRPC server (dataloader)
    grpc_stub, grpc_channel = get_grpc_stub()
    logger.info("Connected to gRPC server at %s:%s", GRPC_HOST, GRPC_PORT)

    def process_exif(media_id, media_path):
        """Core EXIF extraction logic (shared by both modes)."""
        try:
            with open(media_path, "rb") as f:
                image = Image(f.read())
        except Exception as e:
            logger.error("Error reading image ID=%s: %s", media_id, e)
            return

        if not image.has_exif:
            return

        if "datetime" in image.list_all():
            try:
                exif_datetime = image.datetime
                exif_datetime = datetime.strptime(
                    exif_datetime, "%Y:%m:%d %H:%M:%S"
                ).strftime("%Y-%m-%d %H:%M:%S")

                tag_type_id = get_tag_type_id(grpc_stub, "Timestamp UTC")
                if tag_type_id is not None:
                    safe_publish(
                        f"tagging.not_added.{tag_type_id}.Timestamp UTC",
                        json.dumps({"taggingValue": exif_datetime, "mediaID": media_id}),
                    )
                    logger.info("Published EXIF datetime: %s for ID=%s", exif_datetime, media_id)
                else:
                    logger.warning("Skipping datetime for ID=%s: tagset 'Timestamp UTC' not found", media_id)
            except Exception as e:
                logger.warning("Failed to parse EXIF datetime for ID=%s: %s", media_id, e)

        all_tags = image.list_all()
        if all(t in all_tags for t in ["gps_latitude", "gps_longitude", "gps_latitude_ref", "gps_longitude_ref"]):
            try:
                def dms_to_float(dms, ref):
                    deg, min_, sec = dms
                    value = deg + min_ / 60 + sec / 3600
                    if ref in ["S", "W"]:
                        value = -value
                    return value

                gps_lat = dms_to_float(image.gps_latitude, image.gps_latitude_ref)
                gps_lon = dms_to_float(image.gps_longitude, image.gps_longitude_ref)

                tag_type_id = get_tag_type_id(grpc_stub, "Location")
                if tag_type_id is not None:
                    safe_publish(
                        f"tagging.not_added.{tag_type_id}.Location",
                        json.dumps({"taggingValue": f"{gps_lat} {gps_lon}", "mediaID": media_id}),
                    )
                    logger.info("Published GPS: %s, %s for ID=%s", gps_lat, gps_lon, media_id)
                else:
                    logger.warning("Skipping GPS for ID=%s: tagset 'Location' not found", media_id)
            except Exception as e:
                logger.warning("Failed to parse GPS for ID=%s: %s", media_id, e)

    def callback(ch, method, properties, body):
        data = json.loads(body)

        if STANDALONE_MODE:
            # ── Standalone: message comes from media.*, contains ID + MediaURI
            media_id = data.get("ID")
            media_uri = data.get("MediaURI")
            if not media_id or not media_uri:
                logger.warning("Malformed media message: %s", data)
                return

            logger.info("[STANDALONE] Processing media ID=%s URI=%s", media_id, media_uri)

            # Download media ourselves
            media_path = request_media(media_uri)
            if media_path is None:
                logger.warning("[STANDALONE] Skipping media ID=%s (download failed)", media_id)
                return

            # Extract EXIF
            process_exif(media_id, media_path)

            # Release media ourselves
            release_media(media_uri)
            logger.info("[STANDALONE] Done with media ID=%s", media_id)

        else:
            # ── Orchestrated: message comes from process.exif, contains ID + MediaPath
            media_id = data["ID"]
            media_path = data["MediaPath"]

            logger.info("Processing media ID=%s path=%s", media_id, media_path)

            process_exif(media_id, media_path)

            # Send ACK to orchestrator
            send_ack(media_id)
            logger.info("Done with media ID=%s", media_id)

    listen_key = "media.*" if STANDALONE_MODE else "process.exif"
    logger.info("Listening on '%s'...", listen_key)
    rabbitMQ_helpers.listen(listen_key, callback)

    grpc_channel.close()


def send_ack(media_id):
    safe_publish(
        f"process.done.{media_id}",
        json.dumps({"mediaID": media_id, "plugin": PLUGIN_NAME}),
    )


if __name__ == "__main__":
    main()
