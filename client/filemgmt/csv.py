import csv
import grpc_client
from filemgmt.filehandler import FileHandler

class CSVHandler(FileHandler):
# OUTDATED
# Class used to parse CSV files but the syntax hasn't been updated for the optimized functions

    def importFile(self, path):
        try:
            with open(path, 'r') as file:
                reader = csv.reader(file, delimiter=';')
                first_line = next(reader)
                id_map = {}
                iterator = 0

                # Process the first line, i.e the tagsets
                # The syntax is [tagset_name_1];[tagset_type_1];[tagset_name_2];[tagset_type_2]...
                current_name = None
                for item in first_line:
                    if item and current_name is None:
                        iterator += 1
                        current_name = item
                    elif item.isdigit() and current_name is not None:
                        response = self.client.add_tagset(current_name, int(item))
                        if type(response) is dict: # If the tagset already exists
                            response = self.client.get_tagset_by_name(current_name)
                        id_map[iterator] = (response.id, response.tagTypeId)                            # type: ignore
                        current_name = None
                    else:
                        print(f"Invalid item in tagsets: {tagset_item}")                  # type: ignore

                # We the process the following lines, containing medias and their tags: 
                # Syntax: [path];[id_tag1];[value1];[id_tag2];[value2]...[id_tagN];[valueN]
                iterator = 1
                for row in reader:
                    try:
                        iterator += 1
                        if not row:
                            continue        # Skip empty rows
                        path = row[0]
                        media_response = self.client.add_file(path)
                        if not media_response.id:                       # type: ignore
                            raise Exception(f"Invalid media, could not add to DB")
                        for i in range(1, len(row), 2):
                            tagset_id = id_map[int(row[i])][0]          # we have to use the correct tagset_id in db
                            tagtype_id = id_map[int(row[i])][1]
                            if (not tagset_id) or (not tagtype_id):
                                raise Exception(f"Invalid tag definition: {tagset_id}:{tagtype_id}")
                            value = row[i+1]
                            tag_response = self.client.add_tag(tagset_id, tagtype_id, value)
                            if not tag_response.id:                     # type: ignore
                                raise Exception("Could not add tag")
                            tagging_response = self.client.add_tagging(media_id=media_response.id, tag_id=tag_response.id) # type: ignore
                            if not tagging_response.mediaId:            # type: ignore
                                raise Exception("Could not add tag")
                    except Exception as e:
                        print(f"Error at line {iterator}: {e}")

        except FileNotFoundError:
            print(f"File not found: {path}")


    def exportFile(self, path):
        self.client = grpc_client.LoaderClient()
        tagsets = []
        medias = []

        # Prepare the header line with tagset names and types
        header = []
        response_tagsets = self.client.get_tagsets(-1)
        for tagset_response in response_tagsets:
            header.extend([f"\"{tagset_response.name}\"", f"{tagset_response.tagTypeId}"]) # type: ignore
            
        # Write the data to the CSV file
        with open(path, "w", newline="", encoding="UTF-8") as file:
            csv_writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_NONE, escapechar='', quotechar='')
            csv_writer.writerow(header)


            response_medias = self.client.get_medias(-1)
            for media_response in response_medias:
                path = media_response.file_uri                              # type: ignore
                row = [f'\"{path}\"']
                tag_ids = self.client.get_media_tags(media_response.id)          # type: ignore
                for id_tag in tag_ids:
                    tag_response = self.client.get_tag(int(id_tag))                      # type: ignore
                    tagset_id = tag_response.tagSetId                               # type: ignore
                    possible_values = [tag_response.alphanumerical.value,           # type: ignore
                                    tag_response.timestamp.value,                # type: ignore
                                    tag_response.time.value,                     # type: ignore
                                    tag_response.date.value,                     # type: ignore
                                    tag_response.numerical.value]                # type: ignore
                    value = next(value for value in possible_values if value != "")
                    row.extend([f'{tagset_id}', f'\"{value}\"'])
                    
                csv_writer.writerow(row)
