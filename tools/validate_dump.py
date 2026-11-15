import argparse
import csv
import io
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

# the dumps are restored into temporary databases in the db container (see docker-compose)
DB = "db"
EXPECTED_DB = "validate_expected"
ACTUAL_DB = "validate_actual"

# tagsets whose tags must be exactly the same per image
# classifier, color detector and face recognition (number of faces)
EXACT_TAGSETS = ["Image Classification", "Colors", "Number of faces"]
# face recognition names unknown faces Unknown1, Unknown2, ... in the order it sees them, and the
# pipeline processes medias in parallel, so the names differ between runs. Instead we check that the
# same images are grouped as the same person
FACES_TAGSET = "faces"
# captioning is not deterministic, so we only check that an image has a caption
CAPTION_TAGSET = "Caption"

# every tagging with its image, tagset and value. The value is in one of the *_tags tables
# depending on the tag type, all with the same id as tags
TAGGINGS_QUERY = """
COPY (
    SELECT m.file_uri, ts.name,
           COALESCE(a.name, n.name::text, d.name::text, ti.name::text, tst.name::text)
    FROM taggings g
    JOIN medias m ON m.id = g.object_id
    JOIN tags t ON t.id = g.tag_id
    JOIN tagsets ts ON ts.id = t.tagset_id
    LEFT JOIN alphanumerical_tags a ON a.id = t.id
    LEFT JOIN numerical_tags n ON n.id = t.id
    LEFT JOIN date_tags d ON d.id = t.id
    LEFT JOIN time_tags ti ON ti.id = t.id
    LEFT JOIN timestamp_tags tst ON tst.id = t.id
) TO STDOUT WITH CSV
"""


def run(command, **kwargs):
    result = subprocess.run(command, capture_output=True, **kwargs)
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace") if isinstance(result.stderr, bytes) else result.stderr
        raise RuntimeError(f"{' '.join(command)} failed: {stderr.strip()}")
    return result.stdout


def psql(database, query):
    return run(["docker", "exec", DB, "psql", "-U", "postgres", "-d", database, "-At", "-c", query],
               text=True, encoding="utf-8", errors="replace")


# restore a dump made by e2e_benchmark.py (pg_dump -Fc) into a fresh database
def restore(dump, database):
    run(["docker", "exec", DB, "dropdb", "-U", "postgres", "--if-exists", database])
    run(["docker", "exec", DB, "createdb", "-U", "postgres", database])
    with open(dump, "rb") as f:
        result = subprocess.run(["docker", "exec", "-i", DB, "pg_restore", "-U", "postgres", "-d", database,
                                 "--no-owner", "--no-privileges"], stdin=f, capture_output=True)
    # pg_restore also exits with 1 for errors it ignored, so only warn. A broken restore fails in the queries
    if result.returncode != 0:
        print(f"warning: pg_restore of {dump}: {result.stderr.decode(errors='replace').strip()}", file=sys.stderr)


def drop(database):
    subprocess.run(["docker", "exec", DB, "dropdb", "-U", "postgres", "--if-exists", database], capture_output=True)


# -> (all images, {image: {tagset: set of values}}, total number of taggings)
def load(database):
    medias = set(psql(database, "SELECT file_uri FROM medias").splitlines())
    tags = defaultdict(lambda: defaultdict(set))
    output = psql(database, TAGGINGS_QUERY)
    for file_uri, tagset, value in csv.reader(io.StringIO(output)):
        tags[file_uri][tagset].add(value)
    total = int(psql(database, "SELECT count(*) FROM taggings").strip())
    return medias, tags, total


# person -> the images they are on, as a multiset of image groups (names left out)
def face_groups(tags):
    images_per_face = defaultdict(set)
    for file_uri, tagsets in tags.items():
        for name in tagsets.get(FACES_TAGSET, ()):
            images_per_face[name].add(file_uri)
    return Counter(frozenset(images) for images in images_per_face.values()), images_per_face


def validate(expected, actual, show):
    expected_medias, expected_tags, expected_total = expected
    actual_medias, actual_tags, actual_total = actual
    failures = {}  # check -> list of messages

    def fail(check, message):
        failures.setdefault(check, []).append(message)

    for file_uri in sorted(expected_medias - actual_medias):
        fail("medias", f"missing {file_uri}")
    for file_uri in sorted(actual_medias - expected_medias):
        fail("medias", f"extra {file_uri}")

    images = sorted(expected_medias | actual_medias)
    for tagset in EXACT_TAGSETS:
        for file_uri in images:
            want = expected_tags[file_uri].get(tagset, set())
            got = actual_tags[file_uri].get(tagset, set())
            if want != got:
                fail(tagset, f"{file_uri}: expected {sorted(want)}, got {sorted(got)}")

    expected_groups, expected_names = face_groups(expected_tags)
    actual_groups, actual_names = face_groups(actual_tags)
    names_of = lambda names, group: sorted(n for n, imgs in names.items() if imgs == group)
    for group in sorted(expected_groups - actual_groups, key=sorted):
        fail(FACES_TAGSET, f"expected person {names_of(expected_names, group)} on {sorted(group)}, "
                           f"no person is on exactly these images")
    for group in sorted(actual_groups - expected_groups, key=sorted):
        fail(FACES_TAGSET, f"unexpected person {names_of(actual_names, group)} on {sorted(group)}")

    for file_uri in images:
        want = bool(expected_tags[file_uri].get(CAPTION_TAGSET))
        got = bool(actual_tags[file_uri].get(CAPTION_TAGSET))
        if want != got:
            fail(CAPTION_TAGSET, f"{file_uri}: expected {'a' if want else 'no'} caption, "
                                 f"got {'one' if got else 'none'}")

    if expected_total != actual_total:
        fail("total taggings", f"expected {expected_total}, got {actual_total}")

    checks = ["medias", *EXACT_TAGSETS, FACES_TAGSET, CAPTION_TAGSET, "total taggings"]
    for check in checks:
        messages = failures.get(check, [])
        print(f"{'OK  ' if not messages else 'FAIL'} {check}" + (f" ({len(messages)} mismatches)" if messages else ""))
        for message in messages[:show]:
            print(f"     {message}")
        if len(messages) > show:
            print(f"     ... {len(messages) - show} more (use --show)")
    print(f"images: {len(expected_medias)} expected, {len(actual_medias)} checked. "
          f"taggings: {expected_total} expected, {actual_total} checked")
    return not failures


def main():
    parser = argparse.ArgumentParser(description="Check that a db dump from e2e_benchmark.py has the same tags "
                                                 "as a dump that is known to be correct")
    parser.add_argument("expected", type=Path, help="dump of the correct database")
    parser.add_argument("actual", type=Path, help="dump of the database to check")
    parser.add_argument("--show", type=int, default=20, help="mismatches to print per check")
    args = parser.parse_args()

    for dump in (args.expected, args.actual):
        if not dump.is_file():
            parser.error(f"{dump} does not exist")

    try:
        restore(args.expected, EXPECTED_DB)
        restore(args.actual, ACTUAL_DB)
        ok = validate(load(EXPECTED_DB), load(ACTUAL_DB), args.show)
    finally:
        drop(EXPECTED_DB)
        drop(ACTUAL_DB)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
