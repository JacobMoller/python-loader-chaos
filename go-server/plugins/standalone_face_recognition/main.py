import face_recognition
import glob
import json
import os
import logging

import media_downloader_client
import rabbitMQ_standalone

from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
)
logger = logging.getLogger(__name__)

PLUGIN_NAME = "face_recognition"

# Published by the classifier when it detects people: {"ID", "MediaURI"}
PEOPLE_DETECTED_KEY = "people_detected"
# Published by the server when a tag of the faces tagset is renamed: {"OldName", "NewName"}
TAG_UPDATE_KEY = "tag_update.faces"

KNOWN_FACE_DIR = os.environ.get("KNOWN_FACE_DIR", "/app/known_faces")
FACE_TOLERANCE = float(os.environ.get("FACE_TOLERANCE", "0.6"))

# Both message types are handled on the same worker thread (see
# rabbitMQ_standalone.consume), so no lock is needed around the known faces
known_faces_encodings: dict[str, any] = {}


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
# HANDLERS
# =========================
def handle_message(body: bytes, routing_key: str):
    if routing_key == TAG_UPDATE_KEY:
        handle_tag_update(json.loads(body))
    else:
        handle_people_detected(json.loads(body))


def handle_tag_update(data):
    old_name = data["OldName"]
    new_name = data["NewName"]

    logger.info("Tag update: %s -> %s", old_name, new_name)

    if old_name in known_faces_encodings:
        known_faces_encodings[new_name] = known_faces_encodings.pop(old_name)
        old_path = os.path.join(KNOWN_FACE_DIR, old_name + ".jpg")
        new_path = os.path.join(KNOWN_FACE_DIR, new_name + ".jpg")
        if os.path.exists(old_path):
            os.rename(old_path, new_path)
        logger.info("Renamed face: %s -> %s", old_name, new_name)
    else:
        logger.warning("Face %s not found in known faces", old_name)


def handle_people_detected(data):
    media_id = data["ID"]
    media_uri = data["MediaURI"]

    logger.info("Processing media ID=%s URI=%s", media_id, media_uri)

    with media_downloader_client.local_media(media_uri) as media_path:
        recognize_faces(media_id, media_path)

    logger.info("Done with media ID=%s", media_id)


def recognize_faces(media_id, media_path):
    unknown_image = face_recognition.load_image_file(media_path)
    unknown_face_locations = face_recognition.face_locations(unknown_image)
    num_faces = len(unknown_face_locations)

    # Publish face count
    rabbitMQ_standalone.publish(
        "tagging.not_added.5.Number of faces",
        json.dumps({"taggingValue": str(num_faces), "mediaID": media_id}),
    )
    logger.info("Found %d face(s) in media ID=%s", num_faces, media_id)

    for face_location in unknown_face_locations:
        unknown_encoding = face_recognition.face_encodings(
            unknown_image, [face_location]
        )[0]

        num_known = len(known_faces_encodings)
        if num_known <= 1:
            face_name = "Unknown" + str(num_known + 1)
            known_faces_encodings[face_name] = unknown_encoding
            _publish_face_tag(face_name, media_id)
            save_face_crop(media_path, face_location, face_name)
            continue

        names = list(known_faces_encodings.keys())
        results = face_recognition.compare_faces(
            list(known_faces_encodings.values()), unknown_encoding, tolerance=FACE_TOLERANCE
        )

        matched = False
        for i, result in enumerate(results):
            if result:
                _publish_face_tag(names[i], media_id)
                logger.info("Matched face: %s in ID=%s", names[i], media_id)
                matched = True
                break

        if not matched:
            face_name = "Unknown" + str(len(known_faces_encodings) + 1)
            known_faces_encodings[face_name] = unknown_encoding
            _publish_face_tag(face_name, media_id)
            save_face_crop(media_path, face_location, face_name)
            logger.info("New face: %s in ID=%s", face_name, media_id)


def _publish_face_tag(face_name: str, media_id):
    rabbitMQ_standalone.publish(
        "tagging.not_added.1.faces",
        json.dumps({"taggingValue": face_name, "mediaID": media_id}),
    )


# =========================
# MAIN
# =========================
def main():
    logger.info("Starting standalone Face Recognition Plugin...")

    os.makedirs(KNOWN_FACE_DIR, exist_ok=True)
    load_known_faces()

    rabbitMQ_standalone.consume(PLUGIN_NAME, [PEOPLE_DETECTED_KEY, TAG_UPDATE_KEY], handle_message)


if __name__ == "__main__":
    main()
