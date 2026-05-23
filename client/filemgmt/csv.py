import csv
import grpc_client

from grpc import RpcError
import logging
from filemgmt.filehandler import FileHandler

# Types: 1=alphanumerical, 2=timestamp, 3=time, 4=date, 5=numerical
# Note: float-valued columns (elevation, speed, heart, calories, steps) use
# alphanumerical (type 1) because the numerical type only accepts integers.

# Default column map — used when no external file is provided

# Columns to ignore — used as media identifier, not a tag
LSC_IGNORED_COLUMNS = {"minute_id"}


def load_lsc_column_map(path: str) -> dict:
    """Load LSC column map from a JSON file.

    Expected format:
    {
        "csv_column_name": {"tagset_name": "...", "tagtype_id": N},
        ...
    }

    Returns a dict matching the internal structure: {col: (tagset_name, tagtype_id), ...}
    """
    import json as _json
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = _json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("Column map JSON must be an object (dict).")
        column_map = {}
        for col, entry in raw.items():
            if not isinstance(entry, dict) or "tagset_name" not in entry or "tagtype_id" not in entry:
                raise ValueError(
                    f"Invalid entry for column '{col}': expected keys 'tagset_name' and 'tagtype_id'."
                )
            column_map[col] = (entry["tagset_name"], int(entry["tagtype_id"]))
        logging.info("Loaded %d column mappings from %s", len(column_map), path)
        return column_map
    except FileNotFoundError:
        logging.error("Column map file not found: %s", path)
        raise
    except _json.JSONDecodeError as e:
        logging.error("Invalid JSON in %s: %s", path, e)
        raise


class CSVHandler(FileHandler):

    def importFile(self, path, lsc=False, media_host="http://localhost:5005", column_map=None):
        """Import a CSV file into the database.

        Standard format (lsc=False):
            Line 1 : tagset_name_1;tagset_type_1;tagset_name_2;tagset_type_2;...
            Line N : media_path;tagset_index_1;value_1;tagset_index_2;value_2;...

        LSC format (lsc=True):
            Line 1 : header with column names (minute_id, utc_time, local_time, ...)
            Line N : data rows with values per column
            media_host : base URL for building media URIs (default: http://localhost:5005)
            column_map : optional path to a JSON file overriding the default LSC_COLUMN_MAP
        """
        if lsc:
            # Resolve the column map: external file > default
            if column_map is not None:
                lsc_column_map = load_lsc_column_map(column_map)
            else:
                lsc_column_map = {}
            self._importLSC(path, media_host, lsc_column_map)
        else:
            self._importStandard(path)

    # ------------------------------------------------------------------ #
    #  Standard import                                                     #
    # ------------------------------------------------------------------ #

    def _importStandard(self, path):
        try:
            with open(path, 'r') as file:
                reader = csv.reader(file, delimiter=';')
                first_line = next(reader)
                id_map = {}
                iterator = 0

                # Process the first line, i.e. the tagsets
                # Syntax: [tagset_name_1];[tagset_type_1];[tagset_name_2];[tagset_type_2]...
                current_name = None
                for item in first_line:
                    if item and current_name is None:
                        iterator += 1
                        current_name = item
                    elif item.isdigit() and current_name is not None:
                        try:
                            response = self.client.add_tagset(current_name, int(item))
                        except RpcError as e:
                            logging.warning(f"Failed to add tagset {current_name} with type {item}: {e}")
                            continue
                        id_map[iterator] = (response.id, response.tagTypeId)
                        current_name = None
                    else:
                        print(f"Invalid item in tagsets: {item}")

                # Process the following lines, containing medias and their tags
                # Syntax: [path];[tagset_index_1];[value_1];[tagset_index_2];[value_2]...
                iterator = 1
                for row in reader:
                    try:
                        iterator += 1
                        if not row:
                            continue        # Skip empty rows
                        path = row[0]
                        try:
                            media_response = self.client.add_file(path)
                        except RpcError as e:
                            logging.warning(f"Failed to add media {path}: {e}")
                            continue
                        for i in range(1, len(row), 2):
                            tagset_id = id_map[int(row[i])][0]     # use the correct tagset_id from DB
                            tagtype_id = id_map[int(row[i])][1]
                            if (not tagset_id) or (not tagtype_id):
                                raise Exception(f"Invalid tag definition: {tagset_id}:{tagtype_id}")
                            value = row[i + 1]
                            try:
                                tag_response = self.client.add_tag(tagset_id, tagtype_id, value)
                            except RpcError as e:
                                logging.warning(f"Failed to add tag {value} for tagset {tagset_id}: {e}")
                                continue
                            if not tag_response.id:
                                raise Exception("Could not add tag")
                            try:
                                tagging_response = self.client.add_tagging(
                                    media_id=media_response.id,
                                    tag_id=tag_response.id
                                )
                            except RpcError as e:
                                logging.warning(
                                    f"Failed to tag media {media_response.id} with tag {tag_response.id}: {e}"
                                )
                                continue
                            if not tagging_response.mediaId:
                                raise Exception("Could not add tagging")
                    except Exception as e:
                        print(f"Error at line {iterator}: {e}")

        except FileNotFoundError:
            print(f"File not found: {path}")

    # ------------------------------------------------------------------ #
    #  LSC import                                                          #
    # ------------------------------------------------------------------ #

    def _importLSC(self, path, media_host, column_map):
        """Import a LSC-format CSV file.

        Builds the media URI from minute_id:
            http://<media_host>/lsc/<YYYYMM>/<DD>/<minute_id>_000.jpg

        For each row, collects all tag values and sends them in a single
        createMediaWithTags gRPC call. The server inserts everything and
        publishes one enriched RabbitMQ message containing the media info
        and all its tags.

        Silently skips NULL values.
        """
        ok, skipped, errors = 0, 0, 0
        try:
            with open(path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=',')

                for line_num, row in enumerate(reader, start=2):
                    try:
                        ImageID = row.get("ImageID", "").strip()
                        minute_id = row.get("minute_id", "").strip()
                        if ImageID:
                            media = ImageID.strip("[]").replace("'", "").split(", ")[0]
                            date_part = media.split("_")[0]   # 20150223
                            year_month = date_part[:6]             # 201502
                            day = date_part[6:8]  
                            media_uri = f"{media_host}/lsc/{year_month}/{day}/{media}"
                            # print(f"Processing media URI: {media_uri}")
                        elif minute_id :
                            date_part = minute_id.split("_")[0]   # 20150223
                            year_month = date_part[:6]             # 201502
                            day = date_part[6:8]                   # 23
                            media_uri = f"{media_host}/lsc/{year_month}/{day}/{minute_id}_000.jpg"
                        else:
                            skipped += 1
                            continue

                        # ── Collect all non-NULL tag entries ────────────
                        tags = []
                        for col, value in row.items():
                            col = col.strip()

                            # Skip the media identifier and unmapped columns
                            if col in LSC_IGNORED_COLUMNS:
                                continue
                            if col not in column_map:
                                continue

                            # Skip NULL values
                            if not value or value.strip().upper() == "NULL":
                                continue

                            tagset_name, tagtype_id = column_map[col]
                            tags.append({
                                'tagset_name': tagset_name,
                                'tagtype_id':  tagtype_id,
                                'value':       value.strip(),
                            })

                        # ── Single gRPC call: media + all tags at once ──
                        try:
                            self.client.add_media_with_tags(media_uri, tags)
                        except RpcError as e:
                            logging.warning(f"[line {line_num}] Failed to create media with tags {media_uri}: {e}")
                            errors += 1
                            continue

                        ok += 1
                    except Exception as e:
                        print(f"Error at line {line_num}: {e}")
                        errors += 1

        except FileNotFoundError:
            print(f"File not found: {path}")
            return

        print(f"\n── Summary ─────────────────────────")
        print(f"  Media processed : {ok}")
        print(f"  Skipped         : {skipped}")
        print(f"  Errors          : {errors}")
        print(f"────────────────────────────────────")

    # ------------------------------------------------------------------ #
    #  Export                                                              #
    # ------------------------------------------------------------------ #

    def exportFile(self, path):
        self.client = grpc_client.LoaderClient()

        # Build the header line from all tagsets in the database
        header = []
        try:
            response_tagsets = self.client.get_tagsets(-1)
        except RpcError as e:
            logging.error(f"Failed to retrieve tagsets: {e}")
            return
        for tagset_response in response_tagsets:
            if tagset_response.HasField("error"):
                logging.warning(f"Error retrieving tagset: {tagset_response.error}")
                continue
            header.extend([f"\"{tagset_response.tagset.name}\"", f"{tagset_response.tagset.tagTypeId}"])

        # Write tagsets header and media rows to the CSV file
        with open(path, "w", newline="", encoding="UTF-8") as file:
            csv_writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_NONE, escapechar='', quotechar='')
            csv_writer.writerow(header)

            try:
                response_medias = self.client.get_medias(-1)
            except RpcError as e:
                logging.error(f"Failed to retrieve medias: {e}")
                return
            for media_response in response_medias:
                if media_response.HasField("error"):
                    logging.warning(f"Error retrieving media: {media_response.error}")
                    continue
                path = media_response.media.file_uri
                row = [f'\"{path}\"']
                try:
                    tag_ids = self.client.get_media_tags(media_response.media.id)
                except RpcError as e:
                    logging.error(f"Failed to retrieve tags for media {media_response.media.id}: {e}")
                    continue
                for id_tag in tag_ids:
                    try:
                        tag_response = self.client.get_tag(int(id_tag))
                    except RpcError as e:
                        logging.warning(f"Failed to retrieve tag {id_tag}: {e}")
                        continue
                    tagset_id = tag_response.tagSetId
                    # Resolve the tag value regardless of its type
                    possible_values = [
                        tag_response.alphanumerical.value,
                        tag_response.timestamp.value,
                        tag_response.time.value,
                        tag_response.date.value,
                        tag_response.numerical.value
                    ]
                    value = next((v for v in possible_values if v != ""), "")
                    row.extend([f'{tagset_id}', f'\"{value}\"'])

                csv_writer.writerow(row)
