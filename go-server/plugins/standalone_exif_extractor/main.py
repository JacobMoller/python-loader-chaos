import json
import logging
import os

import grpc
import dataloader_pb2
import dataloader_pb2_grpc
import media_downloader_client
import rabbitMQ_standalone

from exif import Image
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PLUGIN_NAME = "exif_extractor"

GRPC_HOST = os.getenv("GRPC_HOST", "go-server")
GRPC_PORT = os.getenv("GRPC_PORT", "50051")

grpc_stub = None


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


def process_exif(media_id, media_path):
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
                rabbitMQ_standalone.publish(
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
                rabbitMQ_standalone.publish(
                    f"tagging.not_added.{tag_type_id}.Location",
                    json.dumps({"taggingValue": f"{gps_lat} {gps_lon}", "mediaID": media_id}),
                )
                logger.info("Published GPS: %s, %s for ID=%s", gps_lat, gps_lon, media_id)
            else:
                logger.warning("Skipping GPS for ID=%s: tagset 'Location' not found", media_id)
        except Exception as e:
            logger.warning("Failed to parse GPS for ID=%s: %s", media_id, e)


def handle_media(body: bytes, routing_key: str):
    # media.* messages are published by the server as {"ID", "MediaURI", "ThumbnailURI"}
    data = json.loads(body)
    media_id = data.get("ID")
    media_uri = data.get("MediaURI")
    if not media_id or not media_uri:
        logger.warning("Malformed media message: %s", data)
        return

    logger.info("Processing media ID=%s URI=%s", media_id, media_uri)

    with media_downloader_client.local_media(media_uri) as media_path:
        process_exif(media_id, media_path)

    logger.info("Done with media ID=%s", media_id)


def main():
    global grpc_stub
    logger.info("Starting standalone EXIF Extractor Plugin...")

    # Connect to gRPC server (dataloader)
    grpc_stub = dataloader_pb2_grpc.DataLoaderStub(grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}"))
    logger.info("Connected to gRPC server at %s:%s", GRPC_HOST, GRPC_PORT)

    rabbitMQ_standalone.consume(PLUGIN_NAME, ["media.*"], handle_media)


if __name__ == "__main__":
    main()
