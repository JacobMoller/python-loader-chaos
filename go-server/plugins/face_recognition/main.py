import face_recognition
import glob
import json
import os
import logging
import threading

import rabbitMQ_helpers

from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
)
logger = logging.getLogger(__name__)

PLUGIN_NAME = "faces"

KNOWN_FACE_DIR = os.environ.get("KNOWN_FACE_DIR", "/app/known_faces")
FACE_TOLERANCE = float(os.environ.get("FACE_TOLERANCE", "0.6"))

# Thread-safe access to known faces
faces_lock = threading.Lock()
known_faces_encodings: dict[str, any] = {}

# Thread-safe access to publish channel
pub_lock = threading.Lock()
pub_channel = None
pub_connection = None


# =========================
# PUBLISH CHANNEL MANAGEMENT
# =========================
def get_pub_channel():
    """Return a valid publish channel, reconnecting if necessary."""
    global pub_channel, pub_connection
    with pub_lock:
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
    try:
        channel = get_pub_channel()
        rabbitMQ_helpers.publish_message(channel, routing_key, message)
    except Exception as e:
        logger.warning("Publish failed (%s), retrying with fresh connection...", e)
        # Force reconnect on next get_pub_channel call
        global pub_channel
        with pub_lock:
            pub_channel = None
        channel = get_pub_channel()
        rabbitMQ_helpers.publish_message(channel, routing_key, message)


# =========================
# FACE LOADING
# =========================
def load_known_faces():
    extensions = ["*.jpg", "*.png", "*.jpeg", "*.JPG", "*.PNG", "*.JPEG"]
    known_face_files = []
    for ext in extensions:
        known_face_files.extend(glob.glob(os.path.join(KNOWN_FACE_DIR, ext)))

    logger.info("Found %d known face files in %s", len(known_face_files), KNOWN_FACE_DIR)

    for path in known_face_files:
        try:
            image = face_recognition.load_image_file(path)
            encoding = face_recognition.face_encodings(image)
            if encoding:
                name = os.path.splitext(os.path.basename(path))[0]
                with faces_lock:
                    known_faces_encodings[name] = encoding[0]
                logger.info("Loaded known face: %s", name)
        except Exception as e:
            logger.warning("Failed to load face from %s: %s", path, e)


def save_face_crop(image_path: str, face_location: tuple, face_name: str):
    try:
        img = Image.open(image_path)
        top, right, bottom, left = face_location
        width, height = img.size
        margin_x = int((right - left) * 0.2)
        margin_y = int((bottom - top) * 0.2)
        img_cropped = img.crop((
            max(left - margin_x, 0),
            max(top - margin_y, 0),
            min(right + margin_x, width),
            min(bottom + margin_y, height),
        ))
        img_cropped.save(os.path.join(KNOWN_FACE_DIR, face_name + ".jpg"))
        logger.info("Saved face crop: %s", face_name)
    except Exception as e:
        logger.warning("Failed to save face crop for %s: %s", face_name, e)


# =========================
# TAG UPDATE CALLBACK
# =========================
def tag_update_callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        old_name = data["OldName"]
        new_name = data["NewName"]

        logger.info("Tag update: %s -> %s", old_name, new_name)

        with faces_lock:
            if old_name in known_faces_encodings:
                known_faces_encodings[new_name] = known_faces_encodings.pop(old_name)
                old_path = os.path.join(KNOWN_FACE_DIR, old_name + ".jpg")
                new_path = os.path.join(KNOWN_FACE_DIR, new_name + ".jpg")
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                logger.info("Renamed face: %s -> %s", old_name, new_name)
            else:
                logger.warning("Face %s not found in known faces", old_name)
    except Exception as e:
        logger.error("Error in tag_update_callback: %s", e)


# =========================
# MAIN
# =========================
def main():
    logger.info("Starting Face Recognition Plugin...")

    os.makedirs(KNOWN_FACE_DIR, exist_ok=True)
    load_known_faces()

    # Initialize publish connection (will be lazily reconnected if needed)
    get_pub_channel()
    logger.info("Publish connection established.")

    # Tag update listener in daemon thread (uses rabbitMQ_helpers.listen)
    tag_thread = threading.Thread(
        target=rabbitMQ_helpers.listen,
        args=("tag_update.faces", tag_update_callback),
        name="TagUpdateListener",
        daemon=True,
    )
    tag_thread.start()

    # Process callback
    def callback(ch, method, properties, body):
        data = json.loads(body)
        media_id = data["ID"]
        media_path = data["MediaPath"]

        logger.info("Processing media ID=%s path=%s", media_id, media_path)

        try:
            unknown_image = face_recognition.load_image_file(media_path)
            unknown_face_locations = face_recognition.face_locations(unknown_image)
            num_faces = len(unknown_face_locations)

            # Publish face count
            safe_publish(
                "tagging.not_added.5.Number of faces",
                json.dumps({"taggingValue": str(num_faces), "mediaID": media_id}),
            )
            logger.info("Found %d face(s) in media ID=%s", num_faces, media_id)

            for face_location in unknown_face_locations:
                unknown_encoding = face_recognition.face_encodings(
                    unknown_image, [face_location]
                )[0]

                with faces_lock:
                    num_known = len(known_faces_encodings)

                    if num_known <= 1:
                        face_name = "Unknown" + str(num_known + 1)
                        known_faces_encodings[face_name] = unknown_encoding
                        _publish_face_tag(face_name, media_id)
                        save_face_crop(media_path, face_location, face_name)
                        continue

                    known_list = list(known_faces_encodings.values())
                    names = list(known_faces_encodings.keys())

                results = face_recognition.compare_faces(
                    known_list, unknown_encoding, tolerance=FACE_TOLERANCE
                )

                matched = False
                for i, result in enumerate(results):
                    if result:
                        _publish_face_tag(names[i], media_id)
                        logger.info("Matched face: %s in ID=%s", names[i], media_id)
                        matched = True
                        break

                if not matched:
                    with faces_lock:
                        face_name = "Unknown" + str(len(known_faces_encodings) + 1)
                        known_faces_encodings[face_name] = unknown_encoding
                    _publish_face_tag(face_name, media_id)
                    save_face_crop(media_path, face_location, face_name)
                    logger.info("New face: %s in ID=%s", face_name, media_id)

        except Exception as e:
            logger.error("Error processing faces for ID=%s: %s", media_id, e)

        send_ack(media_id)
        logger.info("Done with media ID=%s", media_id)

    # Blocking consume via helper
    logger.info("Listening on 'process.faces'...")
    rabbitMQ_helpers.listen("process.faces", callback)


def _publish_face_tag(face_name: str, media_id):
    safe_publish(
        "tagging.not_added.1.faces",
        json.dumps({"taggingValue": face_name, "mediaID": media_id}),
    )


def send_ack(media_id):
    safe_publish(
        f"process.done.{media_id}",
        json.dumps({"mediaID": media_id, "plugin": PLUGIN_NAME}),
    )


if __name__ == "__main__":
    main()
