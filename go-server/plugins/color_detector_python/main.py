import json
import logging
import os
import time

import grpc
from PIL import Image, ImageStat

import rabbitMQ_helpers
import dataloader_pb2
import dataloader_pb2_grpc
import media_downloader_pb2
import media_downloader_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TAGSET = "Colors (python)"

DEFAULT_TAG_TYPE_ID = 1

GRPC_HOST = os.getenv("GRPC_HOST", "go-server")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")
MEDIA_DOWNLOADER_TARGET = os.getenv("MEDIA_DOWNLOADER_TARGET", "media-downloader:50052")

MAX_GRPC_RETRIES = 3
GRPC_RETRY_DELAY = 2   # seconds
DOWNLOAD_TIMEOUT = 30  # seconds

downloader = None
tag_type_id = None


def get_tag_type_id(stub, tagset_name):
    """Ask the dataloader which tag type the tagset has. If it does not exist yet,
    use the default type and let the server create the tagset with it."""
    try:
        tagset = stub.getTagSetByName(
            dataloader_pb2.GetTagSetRequestByName(name=tagset_name), timeout=5
        )
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            logger.info("Tagset '%s' not found, using default tag type %d", tagset_name, DEFAULT_TAG_TYPE_ID)
            return DEFAULT_TAG_TYPE_ID
        raise
    logger.info("Tagset '%s' has tag type %d", tagset_name, tagset.tagTypeId)
    return tagset.tagTypeId


def request_media(media_uri):
    """Ask the media-downloader for a local copy of the media and return its path.
    Local URIs are returned as-is, remote ones are downloaded. Returns None on failure."""
    for attempt in range(1, MAX_GRPC_RETRIES + 1):
        try:
            resp = downloader.RequestMedia(
                media_downloader_pb2.RequestMediaRequest(media_uri=media_uri),
                timeout=DOWNLOAD_TIMEOUT,
            )
            return resp.media_path
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                logger.warning("Media not found: %s", media_uri)
                return None
            logger.warning("RequestMedia failed (attempt %d/%d) for %s: %s", attempt, MAX_GRPC_RETRIES, media_uri, e)
            if attempt < MAX_GRPC_RETRIES:
                time.sleep(GRPC_RETRY_DELAY * attempt)
    return None


def release_media(media_uri):
    """Tell the media-downloader we are done with the media so it can evict it from its cache."""
    try:
        downloader.ReleaseMedia(media_downloader_pb2.ReleaseMediaRequest(media_uri=media_uri), timeout=5)
    except grpc.RpcError as e:
        logger.warning("ReleaseMedia failed for %s: %s", media_uri, e)


# like our process function in go
def callback(ch, method, properties, body):
    logger.info("new message")
    try:
        message = json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse message body: %s", e)
        return

    # media.* messages are published by the server as {"ID", "MediaURI", "ThumbnailURI"}
    media_id = message.get("ID")
    if media_id is None:
        logger.warning("Missing ID in message body")
        return

    media_uri = message.get("MediaURI")
    if media_uri is None:
        logger.warning("Missing MediaURI in message body")
        return

    media_path = request_media(media_uri)
    if media_path is None:
        logger.warning("Skipping media %s: could not get %s", media_id, media_uri)
        return

    try:
        color = extract_color(media_path)
    except Exception as e:
        logger.warning("Failed to extract color for media %s: %s", media_id, e)
        return
    finally:
        release_media(media_uri)

    # Publish
    topic = f"tagging.not_added.{tag_type_id}.{TAGSET}"
    rabbitMQ_helpers.publish_message(ch, topic, json.dumps({"taggingValue": color, "mediaID": media_id}))
    logger.info("[%s] Sent %s for media %s", topic, color, media_id)


# Domain specific code
def extract_color(media_path):
    with Image.open(media_path) as img:
        saturation = img.convert("RGB").convert("HSV").getchannel("S")

    average_saturation = ImageStat.Stat(saturation).mean[0] / 255

    if average_saturation < 0.05:
        return "Black&White"
    return "Color"


def main():
    global downloader, tag_type_id

    # setup gRPC client to media-downloader as we need to analyze the image
    downloader = media_downloader_pb2_grpc.MediaDownloaderStub(grpc.insecure_channel(MEDIA_DOWNLOADER_TARGET))
    logger.info("media-downloader client created -> %s", MEDIA_DOWNLOADER_TARGET)

    # setup gRPC client to the dataloader (go-server) to look up the tag type of our tagset
    dataloader_channel = grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
    tag_type_id = get_tag_type_id(dataloader_pb2_grpc.DataLoaderStub(dataloader_channel), TAGSET)
    # The dataloader is only needed for this lookup
    dataloader_channel.close()

    rabbitMQ_helpers.listen("media.*", callback)


if __name__ == "__main__":
    main()
