import math
import json
import os
import requests
import csv
# import sys
import time
import threading
import queue
from collections import defaultdict
from requests.exceptions import ConnectionError, Timeout, RequestException

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "rabbitMQ")))
import rabbitMQ_helpers

# import grpc

production = True  # Set to True in production to enable the connection to the Grpc server
test = False        # Set to True to use fake data instead of making real API calls

EARTH_R = 6371000.0  # meters
DEFAULT_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "database/gps_metadatas.json")

QUEUE_MAXSIZE = 0  # 0 = unlimited
STOP_SENTINEL = object()

# ─── Global state ────────────────────────────────────────────────────────────
# Single-process, single worker thread: no concurrency on in-memory structures.
# The only real race is between the worker thread writing to the JSON file and
# an external caller invoking update_entry() at the same time.
# One lock covers both sides of that race.
_db_lock = threading.Lock()

gps_points = []
existing_database = []
reference_entries = []
spatial_grid = defaultdict(list)
initialized = False
id_start_of_new_points = 0
current_id = 0

# Set of place_ids already published to RabbitMQ during this session.
# Used to avoid republishing the same location data when multiple media
# share the same geographic location.
_published_place_ids: set = set()
_published_place_ids_lock = threading.Lock()

metadata_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
worker_thread = None
# ─────────────────────────────────────────────────────────────────────────────


def extract_lat_lon(csv_path):
    result = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        header = [h.lower() for h in next(reader)]
        if "lat" not in header or "lon" not in header:
            i1 = header.index("latitude")
            i2 = header.index("longitude")
        else:
            i1 = header.index("lat")
            i2 = header.index("lon")

        for row in reader:
            if (i1 < len(row) and i2 < len(row)
                    and row[i1] not in ("NULL", "")
                    and row[i2] not in ("NULL", "")):
                result.append({"lat": float(row[i1]), "lon": float(row[i2])})
    return result


def haversine(point1, point2):
    lat1, lon1 = point1
    lat2, lon2 = point2
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_R * c


def get_place(point, id, radius_m=20):
    if test:
        data = {
            "display_name": f"Fake Place at {point[0]:.5f}, {point[1]:.5f}",
            "address": {"fake": "data"},
            "nearby_pois": []
        }
        return [data["display_name"], data["address"]], data

    lat, lon = point
    wait_seconds = 5
    max_wait = 60
    min_cycle_seconds = 8.0
    headers = {"User-Agent": "LSC-geodatas/1.0"}

    overpass_endpoints = [
        "https://z.overpass-api.de/api/interpreter",
        "https://overpass-api.de/api/interpreter",
        "https://lz4.overpass-api.de/api/interpreter"
    ]

    while True:
        cycle_start = time.time()
        try:
            # 1) Reverse geocoding via Nominatim
            nominatim_url = (
                f"https://nominatim.openstreetmap.org/reverse"
                f"?lat={lat}&lon={lon}&format=jsonv2"
            )
            r = requests.get(nominatim_url, headers=headers, timeout=10)
            r.raise_for_status()
            address_data = r.json()

            # 2) Nearby POIs via Overpass
            overpass_query = f"""
            [out:json][timeout:20];
            (
            node(around:{radius_m},{lat},{lon})["tourism"];
            way(around:{radius_m},{lat},{lon})["tourism"];
            relation(around:{radius_m},{lat},{lon})["tourism"];

            node(around:{radius_m},{lat},{lon})["historic"];
            way(around:{radius_m},{lat},{lon})["historic"];
            relation(around:{radius_m},{lat},{lon})["historic"];

            node(around:{radius_m},{lat},{lon})["amenity"];
            way(around:{radius_m},{lat},{lon})["amenity"];
            relation(around:{radius_m},{lat},{lon})["amenity"];

            node(around:{radius_m},{lat},{lon})["shop"];
            way(around:{radius_m},{lat},{lon})["shop"];
            relation(around:{radius_m},{lat},{lon})["shop"];
            );
            out center tags;
            """

            poi_data = None
            last_error = None

            for endpoint in (overpass_endpoints[id % 3:] + overpass_endpoints[:id % 3]):
                try:
                    r_poi = requests.post(
                        endpoint,
                        data=overpass_query.encode("utf-8"),
                        headers=headers,
                        timeout=25
                    )
                    r_poi.raise_for_status()
                    poi_data = r_poi.json()
                    print(f"[INFO] Overpass OK via {endpoint}")
                    break
                except RequestException as e:
                    last_error = e
                    print(f"[WARN] Overpass failed on {endpoint}: {e}")

            if poi_data is None:
                raise last_error

            nearby_pois = []
            seen = set()

            for el in poi_data.get("elements", []):
                tags    = el.get("tags", {})
                poi_lat = el.get("lat", el.get("center", {}).get("lat"))
                poi_lon = el.get("lon", el.get("center", {}).get("lon"))

                if poi_lat is None or poi_lon is None:
                    continue

                osm_type   = el.get("type")
                osm_id     = el.get("id")
                unique_key = (osm_type, osm_id)

                if unique_key in seen:
                    continue
                seen.add(unique_key)

                distance_m = round(haversine(point, [poi_lat, poi_lon]), 1)
                if distance_m > radius_m:
                    continue

                category = (
                    tags.get("amenity")
                    or tags.get("tourism")
                    or tags.get("historic")
                    or tags.get("shop")
                    or tags.get("leisure")
                )

                nearby_pois.append({
                    "name":       tags.get("name"),
                    "category":   category,
                    "osm_type":   osm_type,
                    "osm_id":     osm_id,
                    "lat":        poi_lat,
                    "lon":        poi_lon,
                    "distance_m": distance_m,
                    "tags":       tags
                })

            nearby_pois.sort(key=lambda x: x["distance_m"])

            data = {**address_data, "nearby_pois": nearby_pois}

            elapsed = time.time() - cycle_start
            if elapsed < min_cycle_seconds:
                time.sleep(min_cycle_seconds - elapsed)

            print(
                f"id : {id} - address for point ({lat:.5f}, {lon:.5f}) is "
                f"'{data.get('display_name')} --- {len(nearby_pois)} nearby POIs found'"
            )
            return [data.get("display_name"), data.get("address")], data

        except (ConnectionError, Timeout) as e:
            print(f"[WARN] Connection lost or timeout: {e}")
            print(f"[INFO] Retrying in {wait_seconds} seconds...")
            time.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2, max_wait)

        except RequestException as e:
            print(f"[WARN] HTTP error: {e}")
            print(f"[INFO] Retrying in {wait_seconds} seconds...")
            time.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2, max_wait)

        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
            print(f"[INFO] Retrying in {wait_seconds} seconds...")
            time.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2, max_wait)


def _build_rmq_payload(lat, lon, media_id, db_entry, is_known_place):
    """Build a precise RabbitMQ message body.

    If the place was already published this session (is_known_place=True),
    only the place_id and media_id are sent to avoid redundant data transfer.
    Otherwise the full enriched payload is sent.

    Parameters
    ----------
    lat, lon        : coordinates of the media
    media_id        : ID of the media in the database
    db_entry        : dict with place_id, display_name, address, data
    is_known_place  : True if this place_id was already published this session

    Returns
    -------
    routing_key : str
    payload     : dict (to be JSON-serialised before publishing)
    """
    place_id = db_entry.get("place_id")
    address  = db_entry.get("address") or {}
    data     = db_entry.get("data") or {}

    if is_known_place:
        # Minimal payload — the consumer can look up full details by place_id
        return "geodatas.known", {
            "media_id": media_id,
            "lat":      lat,
            "lon":      lon,
            "place_id": place_id,
        }

    # Full payload — only the fields that are actually useful to consumers
    return "geodatas.new", {
        "media_id":      media_id,
        "lat":           lat,
        "lon":           lon,
        "place_id":      place_id,
        "display_name":  db_entry.get("display_name"),
        "country":       address.get("country"),
        "country_code":  address.get("country_code"),
        "state":         address.get("state"),
        "county":        address.get("county"),
        "city":          address.get("city") or address.get("town") or address.get("village"),
        "postcode":      address.get("postcode"),
        "road":          address.get("road"),
        # OSM category of the location itself (e.g. "residential", "park")
        "location_type": data.get("type") or data.get("category"),
        "nearby_pois":   [
            {
                "name":       poi.get("name"),
                "category":   poi.get("category"),
                "distance_m": poi.get("distance_m"),
            }
            for poi in data.get("nearby_pois", [])[:5]  # top 5 closest POIs only
        ],
    }


def _build_address_hierarchy(db_entry):
    """Build a nested address hierarchy from a db_entry's address fields.

    Structure: Location → Country → State → City → Road
    Levels with missing/empty values are skipped — the chain stops at the
    deepest non-empty level.  Returns None if country is missing (nothing
    useful to publish).

    Parameters
    ----------
    db_entry : dict with at least an "address" key (Nominatim address dict)

    Returns
    -------
    str or None : JSON string of the hierarchy, or None if not enough data.
    """
    address = db_entry.get("address") or {}

    country = address.get("country", "")
    state   = address.get("state", "")
    city    = (address.get("city")
               or address.get("town")
               or address.get("village")
               or "")
    road    = address.get("road", "")

    if not country:
        return None

    # Build from deepest level up, skipping empty levels
    child = None

    if road:
        child = {
            "tagTypeId": 4,
            "tag":       road,
            "tagset":    "Road",
            "child":     {},
        }

    if city:
        child = {
            "tagTypeId": 1,
            "tag":       city,
            "tagset":    "City",
            "child":     child or {},
        }

    if state:
        child = {
            "tagTypeId": 1,
            "tag":       state,
            "tagset":    "State",
            "child":     child or {},
        }

    country_node = {
        "tagTypeId": 5,
        "tag":       country,
        "tagset":    "Country",
        "child":     child or {},
    }

    hierarchy = {
        "hierarchy": "Location",
        "tagset":    "Location",
        "tagTypeId": 1,
        "tag":       "Location",
        "child":     country_node,
    }

    return json.dumps(hierarchy, ensure_ascii=False)


def _publish_with_reconnect(ch_ref, routing_key, body_str):
    """Publish a message using the worker's own producer channel.

    ch_ref is a single-element list [channel] so that reconnections are
    persisted across calls without needing a nonlocal declaration everywhere.
    If the channel is stale, reconnects once and updates ch_ref[0] in place.
    """
    import pika.exceptions

    try:
        rabbitMQ_helpers.publish_message(ch_ref[0], routing_key, body=body_str)
    except (pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPChannelError) as e:
        print(f"[WARN] Producer channel lost ({e}), reconnecting...")
        try:
            ch_ref[0].close()
        except Exception:
            pass
        ch_ref[0], _ = rabbitMQ_helpers.producer_connection_init()
        rabbitMQ_helpers.publish_message(ch_ref[0], routing_key, body=body_str)
        print(f"[INFO] Reconnected and published on {routing_key}")


def _append_to_db_file(db_file, entry):
    """Append one JSON entry to the already-open database file."""
    serialized = json.dumps(entry, ensure_ascii=False)

    db_file.seek(0, os.SEEK_END)
    end_pos = db_file.tell()

    if end_pos == 0:
        db_file.write("[]")
        db_file.flush()
        end_pos = 2

    db_file.seek(end_pos - 1)

    if end_pos == 2:
        db_file.write(serialized + "]")
    else:
        db_file.write("," + serialized + "]")

    db_file.truncate()
    db_file.flush()


def meters_to_lat_deg(meters):
    return meters / 111320.0


def meters_to_lon_deg(meters, lat):
    cos_lat = math.cos(math.radians(lat))
    if abs(cos_lat) < 1e-12:
        return meters / 111320.0
    return meters / (111320.0 * cos_lat)


def get_cell(point, cell_size_m):
    lat, lon = point
    lat_step = meters_to_lat_deg(cell_size_m)
    lon_step = meters_to_lon_deg(cell_size_m, lat)
    return (int(lat / lat_step), int(lon / lon_step))


# ─── Field routing for update_entry ──────────────────────────────────────
# Fields coming from the geodata bridge (via listenForTaggingUpdateMessage)
# are flat keys like "city", "country", etc.  They must be routed into the
# correct nested sub-dict of the database entry so that the rest of the
# pipeline (_build_rmq_payload, _build_address_hierarchy) sees them.
_ADDRESS_FIELDS = frozenset({
    "country", "country_code", "state", "county", "city",
    "town", "village", "postcode", "road",
})
_DATA_FIELDS = frozenset({"location_type", "poi_category"})
_DATA_FIELD_MAP = {
    "location_type": ("type", "category"),   # stored as data.type AND data.category
    "poi_category":  ("poi_category",),
}


def _apply_fields(entry: dict, fields: dict):
    """Apply *fields* to *entry*, routing each key to the correct sub-dict.

    - Keys in _ADDRESS_FIELDS      → entry["address"][key]
    - Keys in _DATA_FIELDS          → entry["data"][mapped_key(s)]
    - "display_name"                → entry["display_name"]
    - "address" / "data" (full dict)→ merged into the existing sub-dict
    - Anything else                 → entry[key]  (top-level fallback)
    """
    for key, value in fields.items():
        if key in _ADDRESS_FIELDS:
            if "address" not in entry or not isinstance(entry.get("address"), dict):
                entry["address"] = {}
            entry["address"][key] = value

        elif key in _DATA_FIELDS:
            if "data" not in entry or not isinstance(entry.get("data"), dict):
                entry["data"] = {}
            for target_key in _DATA_FIELD_MAP[key]:
                entry["data"][target_key] = value

        elif key == "address" and isinstance(value, dict):
            # Full address replacement / merge (e.g. from a direct call)
            if not isinstance(entry.get("address"), dict):
                entry["address"] = {}
            entry["address"].update(value)

        elif key == "data" and isinstance(value, dict):
            # Full data replacement / merge
            if not isinstance(entry.get("data"), dict):
                entry["data"] = {}
            entry["data"].update(value)

        else:
            # display_name, or any future top-level key
            entry[key] = value


def update_entry(place_id: int, fields: dict, output_path: str = DEFAULT_OUTPUT_PATH):
    """
    Update address-related fields of an existing entry identified by its
    Nominatim place_id, both in the JSON database file and in the in-memory
    lists (existing_database / reference_entries).

    place_id is the OSM identifier returned by Nominatim in the reverse-geocoding
    response and stored at the top level of each db entry. It is guaranteed unique
    per geographic object, regardless of how many GPS points map to the same address.

    Parameters
    ----------
    place_id    : Nominatim place_id of the entry to update.
    fields      : dict of fields to overwrite, e.g.
                  {"display_name": "...", "address": {...}, "data": {...}}.
                  Only keys present in ``fields`` are modified; others are
                  left untouched.
    output_path : path to the JSON database file (defaults to DEFAULT_OUTPUT_PATH).

    Returns
    -------
    True  if an entry was found and updated.
    False if no matching entry was found.

    Raises
    ------
    ValueError  if the JSON file is missing or malformed.
    """
    global existing_database, reference_entries, id_start_of_new_points

    with _db_lock:
        # ── 1. Update the JSON file ──────────────────────────────────────────
        if not os.path.exists(output_path):
            raise ValueError(f"Database file not found: '{output_path}'")

        with open(output_path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                raise ValueError(f"Database file is empty: '{output_path}'")
            try:
                db_list: list = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Malformed JSON database file '{output_path}': {exc}"
                ) from exc

        if not isinstance(db_list, list):
            raise ValueError(
                f"Expected a JSON array in '{output_path}', got {type(db_list).__name__}."
            )

        file_index = None
        for i, entry in enumerate(db_list):
            if entry.get("place_id") == place_id:
                file_index = i
                break

        if file_index is None:
            print(f"[WARN] update_entry: no entry found for place_id={place_id}")
            return False

        _apply_fields(db_list[file_index], fields)

        # Rewrite the file atomically via a temp file + rename
        tmp_path = output_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(db_list, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, output_path)

        print(f"[INFO] update_entry: file updated at index {file_index} for place_id={place_id}")

        mem_index = None
        for idx, entry in enumerate(existing_database):
            if entry.get("place_id") == place_id:
                mem_index = idx
                break
        if mem_index is None:
            for idx, entry in enumerate(reference_entries):
                if entry.get("place_id") == place_id:
                    mem_index = id_start_of_new_points + idx
                    break

        if mem_index is not None:
            if mem_index < id_start_of_new_points:
                _apply_fields(existing_database[mem_index], fields)
                print(f"[INFO] update_entry: existing_database[{mem_index}] updated in memory")
            else:
                rel = mem_index - id_start_of_new_points
                _apply_fields(reference_entries[rel], fields)
                print(f"[INFO] update_entry: reference_entries[{rel}] updated in memory")
        else:
            print(
                f"[WARN] update_entry: place_id={place_id} not found in memory "
                f"— file was updated, memory was not"
            )

    return True


def generate_metadatas(file_path_or_metadata, max_distance_m=5, output_path=None, ch=None, media_id=None):
    global gps_points, existing_database, reference_entries
    global spatial_grid, initialized, id_start_of_new_points, current_id

    # ── Resolve output path ──────────────────────────────────────────────────
    if production:
        gps_metadatas = [file_path_or_metadata]
        _output_path  = output_path or DEFAULT_OUTPUT_PATH
    else:
        gps_metadatas = extract_lat_lon(file_path_or_metadata)
        if not gps_metadatas:
            raise ValueError("No valid GPS coordinates found in the file.")
        base_name    = os.path.abspath(__file__)
        _output_path = output_path or os.path.join(
            os.path.dirname(file_path_or_metadata),
            f"{base_name}_gps_metadatas.json"
        )

    grid_cell_size_m = max_distance_m

    def _add_point_to_index(point, point_id):
        gps_points.append(point)
        cell = get_cell(point, grid_cell_size_m)
        spatial_grid[cell].append(point_id)

    def _nearest(point):
        if not gps_points:
            return None
        base_cell     = get_cell(point, grid_cell_size_m)
        candidate_ids = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                cell = (base_cell[0] + dx, base_cell[1] + dy)
                candidate_ids.extend(spatial_grid.get(cell, []))
        if not candidate_ids:
            return None
        return min(candidate_ids, key=lambda idx: haversine(point, gps_points[idx]))

    # ── One-time initialisation from existing JSON ───────────────────────────
    if not initialized:
        existing_data: list = []
        if os.path.exists(_output_path):
            try:
                with open(_output_path, "r", encoding="utf-8") as f:
                    stripped = f.read().strip()
                    if stripped:
                        existing_data = json.loads(stripped)
                        if not isinstance(existing_data, list):
                            raise ValueError("The existing JSON file must contain a list.")
            except json.JSONDecodeError:
                raise ValueError(f"The existing JSON file '{_output_path}' is invalid.")
        current_id = 0
        for item in existing_data:
            if "lat" not in item or "lon" not in item:
                continue
            point = [item["lat"], item["lon"]]
            _add_point_to_index(point, current_id)
            existing_database.append({
                    "place_id":     item.get("place_id"),
                    "display_name": item.get("display_name"),
                    "address":      item.get("address"),
                    "data":         item.get("data"),
                })
            current_id += 1
        id_start_of_new_points = current_id
        initialized = True

    _existing_count = len(existing_database)

    # Ensure the output file exists before opening it r+
    if not os.path.exists(_output_path):
        with open(_output_path, "w", encoding="utf-8") as f:
            print("Initializing empty JSON database")
            f.write("[]")

    all_results = []

    def _maybe_publish(ch_ref, element, db_entry):
        """Publish to RabbitMQ using the worker's own producer channel.
        ch_ref is a single-element list [channel] — updated in place on reconnection.
        Must only be called from the worker thread.
        """
        if ch_ref is None:
            return
        place_id = db_entry.get("place_id")
        with _published_place_ids_lock:
            is_known = place_id in _published_place_ids
            if not is_known:
                _published_place_ids.add(place_id)
        routing_key, payload = _build_rmq_payload(
            element["lat"], element["lon"], media_id, db_entry, is_known
        )
        _publish_with_reconnect(
            ch_ref, routing_key, json.dumps(payload, ensure_ascii=False)
        )
        print(f"[INFO] Published {routing_key} for place_id={place_id}, media_id={media_id}")

        # Publish the address hierarchy for new places only
        if not is_known:
            hierarchy_json = _build_address_hierarchy(db_entry)
            if hierarchy_json:
                _publish_with_reconnect(ch_ref, "hierarchy", hierarchy_json)
                print(f"[INFO] Published address hierarchy for place_id={place_id}")

    with open(_output_path, "r+", encoding="utf-8") as db:
        for element in gps_metadatas:
            point = [element["lat"], element["lon"]]

            nearest_id = _nearest(point)

            if nearest_id is None:
                assigned_id  = current_id
                current_id  += 1

                place_info, data = get_place(point, assigned_id)

                db_entry = {
                    "lat":          element["lat"],
                    "lon":          element["lon"],
                    "place_id":     data.get("place_id"),
                    "display_name": place_info[0],
                    "address":      place_info[1],
                    "data":         data,
                }

                with _db_lock:
                    _append_to_db_file(db, db_entry)

                _add_point_to_index(point, assigned_id)
                reference_entries.append({
                    "place_id":     db_entry["place_id"],
                    "display_name": db_entry["display_name"],
                    "address":      db_entry["address"],
                    "data":         db_entry["data"],
                })

                result_entry = {
                    "lat":          element["lat"],
                    "lon":          element["lon"],
                    "place_id":     db_entry["place_id"],
                    "display_name": db_entry["display_name"],
                    "address":      db_entry["address"],
                    "data":         db_entry["data"],
                }
                all_results.append(result_entry)
                _maybe_publish(ch, element, db_entry)
                continue

            nearest_point = gps_points[nearest_id]
            if nearest_id < id_start_of_new_points:
                ref_entry = existing_database[nearest_id]
            else:
                rel       = nearest_id - id_start_of_new_points
                ref_entry = reference_entries[rel]

            if haversine(point, nearest_point) > max_distance_m:
                assigned_id  = current_id
                current_id  += 1

                place_info, data = get_place(point, assigned_id)

                if "error" in data:
                    print(
                        f"Error for point ({point[0]:.5f}, {point[1]:.5f}): "
                        f"{data['error']}"
                    )
                    all_results.append({
                        "lat":          element["lat"],
                        "lon":          element["lon"],
                        "display_name": None,
                        "address":      None,
                        "data":         data,
                    })
                    current_id -= 1  # roll back pre-allocated ID
                    continue

                db_entry = {
                    "lat":          element["lat"],
                    "lon":          element["lon"],
                    "place_id":     data.get("place_id"),
                    "display_name": place_info[0],
                    "address":      place_info[1],
                    "data":         data,
                }

                with _db_lock:
                    _append_to_db_file(db, db_entry)

                _add_point_to_index(point, assigned_id)
                reference_entries.append({
                    "place_id":     db_entry["place_id"],
                    "display_name": db_entry["display_name"],
                    "address":      db_entry["address"],
                    "data":         db_entry["data"],
                })

                result_entry = {
                    "lat":          element["lat"],
                    "lon":          element["lon"],
                    "place_id":     db_entry["place_id"],
                    "display_name": db_entry["display_name"],
                    "address":      db_entry["address"],
                    "data":         db_entry["data"],
                }
                all_results.append(result_entry)
                _maybe_publish(ch, element, db_entry)

            else:
                # Close enough — reuse cached entry, no I/O needed
                all_results.append({
                    "lat":          element["lat"],
                    "lon":          element["lon"],
                    "place_id":     ref_entry.get("place_id"),
                    "display_name": ref_entry.get("display_name"),
                    "address":      ref_entry.get("address"),
                    "data":         ref_entry.get("data"),
                })
                _maybe_publish(ch, element, ref_entry)

    print(f"{_existing_count} entries already present in the database")
    print(f"{current_id} unique locations in the database after processing")
    print(f"{len(all_results)} enriched coordinates returned")
    print(f"JSON database updated: {_output_path}")

    return all_results


def _queue_worker(output_path=DEFAULT_OUTPUT_PATH, max_distance_m=5):
    """Worker thread that processes queued GPS coordinates.

    Opens its own dedicated RabbitMQ producer connection at startup.
    This is the only correct way to publish from a background thread with
    pika BlockingConnection, which is not thread-safe. The consumer callbacks
    (preprocess_data, preprocess_update) run on their own listener threads
    and must never share their channel with this worker.
    """
    print(f"[INFO] Metadata worker started (output: {output_path})")

    # Dedicated producer connection — owned exclusively by this thread.
    # Stored in a mutable list so that _maybe_publish can update it in-place
    # after a reconnection without needing a nonlocal declaration in every caller.
    producer_ch_ref = [rabbitMQ_helpers.producer_connection_init()[0]]
    print("[INFO] Worker producer connection established")

    while True:
        item = metadata_queue.get()
        try:
            if item is STOP_SENTINEL:
                print("[INFO] Metadata worker received stop signal")
                return

            lat      = item.get("lat")
            lon      = item.get("lon")
            media_id = item.get("media_id")

            if lat is None or lon is None:
                print(f"[WARN] Ignored invalid queued item: {item}")
                continue

            try:
                generate_metadatas(
                    {"lat": float(lat), "lon": float(lon)},
                    max_distance_m=max_distance_m,
                    output_path=output_path,
                    ch=producer_ch_ref,
                    media_id=media_id,
                )
            except Exception as e:
                print(f"[ERROR] Failed to process queued item {item}: {e}")
        finally:
            metadata_queue.task_done()


def start_metadata_worker(output_path=DEFAULT_OUTPUT_PATH, max_distance_m=5):
    global worker_thread

    if worker_thread is not None and worker_thread.is_alive():
        return worker_thread

    worker_thread = threading.Thread(
        target=_queue_worker,
        kwargs={
            "output_path": output_path,
            "max_distance_m": max_distance_m,
        },
        name="metadata-queue-worker",
        daemon=True,
    )
    worker_thread.start()
    return worker_thread


def stop_metadata_worker(wait=False):
    global worker_thread

    if worker_thread is None or not worker_thread.is_alive():
        return

    metadata_queue.put(STOP_SENTINEL)
    if wait:
        worker_thread.join()


def preprocess_data(ch, method, properties, body):
    """Consumer callback — runs on the listener thread.

    Only enqueues the work item. Never publishes to RabbitMQ directly.
    The channel `ch` received here belongs to the consumer connection and
    must NOT be used for publishing (pika is not thread-safe).
    """
    try:
        data     = json.loads(body)
        media_id = data.get("mediaID")
        parts    = data.get("taggingValue", "").split(" ")

        if len(parts) != 2:
            print(f"[WARN] Message ignored, expected 'lat lon' in taggingValue: {data}")
            return

        lat = parts[0]
        lon = parts[1]

        if lat is None or lon is None:
            print(f"[WARN] Message ignored, missing lat/lon: {data}")
            return

        # Note: ch is intentionally NOT stored in the queue item.
        # The worker thread uses its own dedicated producer connection instead.
        item = {
            "lat":      float(lat),
            "lon":      float(lon),
            "media_id": media_id,
        }
        metadata_queue.put(item)
        print(
            f"[INFO] Queued coordinates ({item['lat']:.5f}, {item['lon']:.5f}) "
            f"media_id={media_id} - pending items: {metadata_queue.qsize()}"
        )
    except Exception as e:
        print(f"[ERROR] Failed to enqueue message: {e}")


def _resolve_place_id_from_media(media_id):
    """
    Look up the place_id associated with a media_id by scanning the in-memory
    database (existing_database + reference_entries).

    Returns the place_id (int) or None if no entry maps to that media_id.
    """
    media_id_str = str(media_id)

    with _db_lock:
        for entry in existing_database:
            if str(entry.get("media_id")) == media_id_str:
                pid = entry.get("place_id")
                if pid is not None:
                    return pid
        for entry in reference_entries:
            if str(entry.get("media_id")) == media_id_str:
                pid = entry.get("place_id")
                if pid is not None:
                    return pid
    return None


def preprocess_update(ch, method, properties, body):
    try:
        print(f"[INFO] Received update message: {body}")
        data     = json.loads(body)
        place_id = data.get('place_id')
        fields   = data.get('fields')
        media_id = data.get('media_id')

        if not isinstance(fields, dict) or not fields:
            print(f"[WARN] Update message ignored, missing or invalid fields: {data}")
            return

        # If place_id is missing (null / None), try to resolve it from media_id.
        # This happens when the message is forwarded from a tagging_update via
        # listenForTaggingUpdateMessage on the server side.
        if place_id is None and media_id is not None:
            place_id = _resolve_place_id_from_media(media_id)
            if place_id is None:
                print(f"[WARN] Update message ignored, could not resolve place_id from media_id={media_id}")
                return
            print(f"[INFO] Resolved place_id={place_id} from media_id={media_id}")

        if place_id is None:
            print(f"[WARN] Update message ignored, missing place_id and media_id: {data}")
            return

        print(f"[INFO] Received update request for place_id={place_id}, fields={list(fields.keys())}")
        update_entry(place_id=place_id, fields=fields)

    except Exception as e:
        print(f"[ERROR] Failed to process update message: {e}")


def main():
    if not production:
        print("Running in development mode...")
        path = "../../../../lsc2020-metadata.csv"
        return generate_metadatas(path)

    # Start the worker thread first — it opens its own producer connection internally
    start_metadata_worker(output_path=DEFAULT_OUTPUT_PATH)

    media_thread = threading.Thread(
        target=rabbitMQ_helpers.listen,
        args=('tagging.*.1.Location', preprocess_data),
        name="media-listener",
        daemon=True,
    )
    update_thread = threading.Thread(
        target=rabbitMQ_helpers.listen,
        args=('update_geodatas.*', preprocess_update),
        name="update-listener",
        daemon=True,
    )

    media_thread.start()
    update_thread.start()

    media_thread.join()
    update_thread.join()


if __name__ == "__main__":
    main()
