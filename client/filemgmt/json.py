import json
from filemgmt.filehandler import FileHandler

class JSONHandler(FileHandler):
# Works with the human-friendly JSON files, less efficient but that can be used for manual tagging of a small collection
# It may be irrelevant because the 'fast' format is in fact readable enough and way more efficient.
# NOTE: the 'human-readable' format is used in the JSON files in the 'tests' folder. The main difference is the use of tag names instead of IDs.

    def importFile(self, path):
    # Import medias, tags and/or hierarchies from a JSON file using the 'human-readable' format
        def addNode(node, tagset_id, tagtype_id, hierarchy_id, parentnode_id):
            tag_value = node.get('tag_value')
            if tag_value:
                tag = self.client.add_tag(tagset_id, tagtype_id, tag_value)
                new_node = self.client.add_node(tag.id, hierarchy_id, parentnode_id)    # type: ignore
                child_nodes = node.get('child_nodes')
                for child_node_item in child_nodes:
                    addNode(child_node_item, tagset_id, tagtype_id, hierarchy_id, new_node.id)

        try:
            with open(path, 'r') as file:
                data = json.load(file)
                tagsets = data.get('tagsets', [])
                id_map = {}
                iterator = 0
                for tagset_item in tagsets:
                    iterator += 1
                    tagset_name = tagset_item.get('name')
                    tagset_type = tagset_item.get('type')
                    if tagset_name and tagset_type:
                        response = self.client.add_tagset(tagset_name, tagset_type)
                        id_map[iterator] = (response.id, response.tagTypeId)                        
                    else:
                        print(f"Invalid item in tagsets: {tagset_item}")

                medias = data.get('medias', [])
                for media_item in medias:
                    media_path = media_item.get('path')
                    if media_path:
                        media_response = self.client.add_file(media_path)
                        tags = media_item.get('tags', [])
                        for tag_item in tags:
                            tagset_id = id_map[tag_item.get('tagset_id')][0]
                            tagtype_id = id_map[tag_item.get('tagset_id')][1]
                            value = tag_item.get('value')
                            tag_response = self.client.add_tag(tagset_id, tagtype_id, value)
                            if type(tag_response) is not str:
                                self.client.add_tagging(media_id=media_response.id, tag_id=tag_response.id) # type: ignore
                    else:
                        print(f"Invalid item in medias: {media_item}")

                hierarchies = data.get('hierarchies', [])
                for hierarchy in hierarchies:
                    name = hierarchy.get('name')
                    tagset_id = id_map[hierarchy.get('tagset_id')][0]
                    tagtype_id = id_map[hierarchy.get('tagset_id')][1]
                    if name and tagset_id:
                        hierarchy_response = self.client.add_hierarchy(name, tagset_id)
                        rootnode_item = hierarchy.get('rootnode')
                        rootnode_tag_value = rootnode_item.get('tag_value')
                        if rootnode_tag_value:
                            rootnode_tag = self.client.add_tag(tagset_id, tagtype_id, rootnode_tag_value)
                            rootnode_id = self.client.add_rootnode(rootnode_tag.id, hierarchy_response.id).id   # type: ignore
                            child_nodes = rootnode_item.get('child_nodes')
                            for child_node_item in child_nodes:                    
                                addNode(child_node_item, tagset_id, tagtype_id, hierarchy_response.id, rootnode_id)

                    else:
                        print(f"Invalid item in medias: {hierarchy}")
            
            print(f"Successfully imported data from JSON file {path}")

        except FileNotFoundError:
            print(f"File not found: {path}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")





    def exportFile(self, path):
    # Export the current DB state to a JSON file using the 'human-friendly' format
        tagsets = []
        medias = []
        hierarchies = []

        def fillTree(node):
            tag_response = self.client.get_tag(node.tagId)
            possible_values = [tag_response.alphanumerical.value,           # type: ignore
                                        tag_response.timestamp.value,                # type: ignore
                                        tag_response.time.value,                     # type: ignore
                                        tag_response.date.value,                     # type: ignore
                                        tag_response.numerical.value]                # type: ignore
            value = next(value for value in possible_values if value != "")
            child_nodes_response = self.client.get_nodes(parentnode_id=node.id)
            child_nodes = []
            for child_node in child_nodes_response:
                if type(child_node) is not str:                 # cheap way to check that we did not get an error
                    child_nodes.append(fillTree(child_node))
            return {"tag_value": value, "child_nodes":child_nodes}
        
        response_tagsets = self.client.get_tagsets(-1)
        for tagset_response in response_tagsets:
            if type(tagset_response) is not str:
                tagsets.append(
                    {"name": tagset_response.name,      # type: ignore
                    "type": tagset_response.tagTypeId   # type: ignore
                    })
        
        response_medias = self.client.get_medias(-1)
        for media_response in response_medias:
            if type(media_response) is not str:
                media_path = media_response.file_uri
                tags = []
                tag_ids = self.client.get_media_tags(media_response.id)          # type: ignore
                if type(tag_ids) is not str:   
                    for id_tag in tag_ids:
                        tag_response = self.client.get_tag(id_tag)                     
                        tagset_id = tag_response.tagSetId                               # type: ignore
                        possible_values = [tag_response.alphanumerical.value,           # type: ignore
                                        tag_response.timestamp.value,                # type: ignore
                                        tag_response.time.value,                     # type: ignore
                                        tag_response.date.value,                     # type: ignore
                                        tag_response.numerical.value]                # type: ignore
                        value = next(value for value in possible_values if value != "")     # Due to grpc standrads, the numerical value won't be Null if not initialised, 
                                                                                            # but zero. Thus we take advantage of the fact that it is the last possible value and
                                                                                            # that only one value can be set for a specified tag

                        tags.append({"tagset_id":tagset_id, "value":value})

                medias.append({"path": media_path, "tags": tags})

        response_hierarchies = self.client.get_hierarchies(-1)
        for hierarchy_response in response_hierarchies:
            if type(hierarchy_response) is not str:
                hierarchy_name = hierarchy_response.name
                hierarchy_tagset_id = hierarchy_response.tagSetId
                rootnode = self.client.get_node(hierarchy_response.rootNodeId)
                filled_tree = fillTree(rootnode)
                hierarchies.append({
                    "name":hierarchy_name,
                    "tagset_id":hierarchy_tagset_id,
                    "rootnode": filled_tree
                })

        data = {"tagsets": tagsets, "medias": medias, "hierarchies": hierarchies}
        print("All data loaded, dumping json file")
        with open(path, "w") as json_file:
            json.dump(data, json_file, indent=4)