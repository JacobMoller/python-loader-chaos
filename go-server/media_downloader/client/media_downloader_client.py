"""
Client for the media-downloader gRPC service, used by standalone plugins that
need the media file itself.

    with media_downloader_client.local_media(media_uri) as media_path:
        ...

Local URIs are returned as-is by the media-downloader, remote ones are
downloaded into its cache (shared through the downloaded-medias volume) and
released again when the block exits, so the cache can evict them.
"""
import logging
import os
import time
from contextlib import contextmanager

import grpc

import media_downloader_pb2
import media_downloader_pb2_grpc

logger = logging.getLogger(__name__)

MEDIA_DOWNLOADER_TARGET = os.getenv("MEDIA_DOWNLOADER_TARGET", "media-downloader:50052")
MAX_GRPC_RETRIES = 3
GRPC_RETRY_DELAY = 2   # seconds
DOWNLOAD_TIMEOUT = 30  # seconds

_stub = None


class MediaUnavailableError(Exception):
    pass


def _get_stub():
    global _stub
    if _stub is None:
        _stub = media_downloader_pb2_grpc.MediaDownloaderStub(grpc.insecure_channel(MEDIA_DOWNLOADER_TARGET))
        logger.info("media-downloader client created -> %s", MEDIA_DOWNLOADER_TARGET)
    return _stub


def request_media(media_uri: str) -> str:
    """Return a local path for the media, raises MediaUnavailableError on failure."""
    for attempt in range(1, MAX_GRPC_RETRIES + 1):
        try:
            resp = _get_stub().RequestMedia(
                media_downloader_pb2.RequestMediaRequest(media_uri=media_uri),
                timeout=DOWNLOAD_TIMEOUT,
            )
            return resp.media_path
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise MediaUnavailableError(f"media not found: {media_uri}") from e
            logger.warning("RequestMedia failed (attempt %d/%d) for %s: %s", attempt, MAX_GRPC_RETRIES, media_uri, e)
            if attempt < MAX_GRPC_RETRIES:
                time.sleep(GRPC_RETRY_DELAY * attempt)
    raise MediaUnavailableError(f"could not download {media_uri}")


def release_media(media_uri: str):
    """Tell the media-downloader we are done with the media so it can evict it from its cache."""
    try:
        _get_stub().ReleaseMedia(media_downloader_pb2.ReleaseMediaRequest(media_uri=media_uri), timeout=5)
    except grpc.RpcError as e:
        logger.warning("ReleaseMedia failed for %s: %s", media_uri, e)


@contextmanager
def local_media(media_uri: str):
    """Request the media, yield its local path and release it afterwards."""
    media_path = request_media(media_uri)
    try:
        yield media_path
    finally:
        release_media(media_uri)
