import json
import logging
import sys

from grpc import RpcError
from filemgmt.filehandler import FileHandler
from grpc import StatusCode

class JSONHandler(FileHandler):
# Works with the human-friendly JSON files, less efficient but that can be used for manual tagging of a small collection
# It may be irrelevant because the 'fast' format is in fact readable enough and way more efficient.
# NOTE: the 'human-readable' format is used in the JSON files in the 'tests' folder. The main difference is the use of tag names instead of IDs.

    def importFile(self, path):
    # Import medias, tags and/or hierarchies from a JSON file using the 'human-readable' format
        def addNode(node, tagset_id, tagtype_id, hierarchy_id, parentnode_id):
            tag_value = node.get('tag')
            if tag_value:
                try:
                    tag = self.client.add_tag(tagset_id, tagtype_id, tag_value)
                except RpcError as e:
                    if e.code() == StatusCode.UNAVAILABLE:
                        logging.error(f"Service unavailable while adding tag {tag_value}: {e}")
                        print("Fatal error, check log file.", file=sys.stderr);exit(1)
                    logging.warning(f"Failed to add tag {tag_value}: {e}\nSkipping adding node {node} and its children")
                    return
                try:
                    new_node = self.client.add_node(tag.id, hierarchy_id, parentnode_id)    # type: ignore
                except RpcError as e:
                    if e.code() == StatusCode.UNAVAILABLE:
                        logging.error(f"Service unavailable while adding node for tag {tag_value}: {e}")
                        print("Fatal error, check log file.", file=sys.stderr);exit(1)
                    logging.warning(f"Failed to add node for tag {tag_value}: {e}\nSkipping adding node {node} and its children")
                    return
                child_nodes = node.get('children', [])
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
                        try:
                            response = self.client.add_tagset(tagset_name, tagset_type)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding tagset {tagset_name}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add tagset {tagset_name}: {e}")
                            continue
                        id_map[tagset_name] = (response.id, response.tagTypeId)                        
                    else:
                        print(f"Invalid item in tagsets: {tagset_item}")

                medias = data.get('medias', [])
                for media_item in medias:
                    media_path = media_item.get('path')
                    thumbnail_path = media_item.get('thumbnail')
                    if media_path:
                        try:
                            media_response = self.client.add_file(media_path, thumbnail_path)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding media {media_path}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add media {media_path}: {e}")
                            continue
                        tags = media_item.get('tags', [])
                        for tag_item in tags:
                            (tagset_id, tagtype_id) = id_map[tag_item.get('tagset')]
                            value = tag_item.get('value')
                            try:
                                tag_response = self.client.add_tag(tagset_id, tagtype_id, value)
                            except RpcError as e:
                                if e.code() == StatusCode.UNAVAILABLE:
                                    logging.error(f"Service unavailable while adding tag {value}: {e}")
                                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                logging.warning(f"Failed to add tag {value} to media {media_path}: {e}\nSkipping adding tagging for media {media_path}")
                                continue
                            try:
                                self.client.add_tagging(media_id=media_response.id, tag_id=tag_response.id) # type: ignore
                            except RpcError as e:
                                if e.code() == StatusCode.UNAVAILABLE:
                                    logging.error(f"Service unavailable while adding tag {value} to media {media_path}: {e}")
                                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                logging.warning(f"Failed to tag media {media_path} with tag {value}: {e}")
                                continue
                    else:
                        print(f"Invalid item in medias: {media_item}")

                hierarchies = data.get('hierarchies', [])
                for hierarchy in hierarchies:
                    name = hierarchy.get('name')
                    try:
                        (tagset_id, tagtype_id) = id_map[hierarchy.get('tagset')]
                    except KeyError:
                        logging.warning(f"Could not find tagset for hierarchy {name}, skipping")
                        continue
                    if name and tagset_id:
                        try:
                            hierarchy_response = self.client.add_hierarchy(name, tagset_id)
                        except RpcError as e:
                            if e.code() == StatusCode.UNAVAILABLE:
                                logging.error(f"Service unavailable while adding hierarchy {name}: {e}")
                                print("Fatal error, check log file.", file=sys.stderr);exit(1)
                            logging.warning(f"Failed to add hierarchy {name}: {e}")
                            continue
                        rootnode_item = hierarchy.get('rootnode')
                        rootnode_tag_value = rootnode_item.get('tag')
                        if rootnode_tag_value:
                            try:
                                rootnode_tag = self.client.add_tag(tagset_id, tagtype_id, rootnode_tag_value)
                            except RpcError as e:
                                if e.code() == StatusCode.UNAVAILABLE:
                                    logging.error(f"Service unavailable while adding tag {rootnode_tag_value} for hierarchy {name}: {e}")
                                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                logging.warning(f"Failed to add root node tag {rootnode_tag_value} for hierarchy {name}: {e}\nSkipping adding hierarchy {name}")
                                continue
                            try:
                                rootnode_id = self.client.add_rootnode(rootnode_tag.id, hierarchy_response.id).id   # type: ignore
                            except RpcError as e:
                                if e.code() == StatusCode.UNAVAILABLE:
                                    logging.error(f"Service unavailable while adding root node for hierarchy {name}: {e}")
                                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                                logging.warning(f"Failed to add root node for hierarchy {name}: {e}\nSkipping adding hierarchy {name}")
                                continue
                            child_nodes = rootnode_item.get('children')
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
            try:
                tag_response = self.client.get_tag(node.tagId)
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    logging.error(f"Service unavailable while retrieving tag {node.tagId}: {e}")
                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                logging.warning(f"Failed to retrieve tag for node {node.id}: {e}")
                return {"tag_value": "", "child_nodes":[]}
            possible_values = [tag_response.alphanumerical.value,           # type: ignore
                                        tag_response.timestamp.value,                # type: ignore
                                        tag_response.time.value,                     # type: ignore
                                        tag_response.date.value,                     # type: ignore
                                        tag_response.numerical.value]                # type: ignore
            value = next(value for value in possible_values if value != "")
            try:
                child_nodes_response = self.client.get_nodes(parentnode_id=node.id)
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    logging.error(f"Service unavailable while retrieving child nodes for node {node.id}: {e}")
                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                logging.warning(f"Failed to retrieve child nodes for node {node.id}: {e}")
                return {"tag_value": value, "child_nodes":[]}
            child_nodes = []
            for child_node in child_nodes_response:
                if child_node.HasField("error"):
                    if child_node.error.code == StatusCode.NOT_FOUND:
                        return {"tag_value": value, "child_nodes":[]}
                    else:
                        logging.warning(f"Error retrieving child node {child_node.id}: {child_node.error.message}")
                        continue
                child_nodes.append(fillTree(child_node))
            return {"tag_value": value, "child_nodes":child_nodes}
        
        try:
            response_tagsets = self.client.get_tagsets(-1)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logging.error(f"Service unavailable while retrieving tagsets: {e}")
                print("Fatal error, check log file.", file=sys.stderr);exit(1)
            logging.error(f"Failed to retrieve tagsets: {e}")
            return
        for tagset_response in response_tagsets:
            if tagset_response.HasField("error"):
                if tagset_response.error.code == StatusCode.NOT_FOUND:
                    break
                else:
                    logging.warinig(f"Error retrieving tagset: {tagset_response.error.message}")
                    continue
            tagsets.append(
                {"name": tagset_response.name,      # type: ignore
                "type": tagset_response.tagTypeId   # type: ignore
                })
        
        try:
            response_medias = self.client.get_medias(-1)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logging.error(f"Service unavailable while retrieving medias: {e}")
                print("Fatal error, check log file.", file=sys.stderr);exit(1)
            logging.error(f"Failed to retrieve medias: {e}")
            return
        for media_response in response_medias:
            if media_response.HasField("error"):
                if media_response.error.code == StatusCode.NOT_FOUND:
                    continue
                else:
                    logging.warning(f"Error retrieving media: {media_response.error.message}")
                    continue
            media_path = media_response.file_uri
            tags = []
            try:
                tag_ids = self.client.get_media_tags(media_response.id)          # type: ignore
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    logging.error(f"Service unavailable while retrieving tags for media {media_response.id}: {e}")
                    print("Fatal error, check log file.", file=sys.stderr);exit(1)
                logging.error(f"Failed to retrieve tags for media {media_response.id}: {e}")
                continue
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


        try:
            response_hierarchies = self.client.get_hierarchies(-1)
        except RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                logging.error(f"Service unavailable while retrieving hierarchies: {e}")
                print("Fatal error, check log file.", file=sys.stderr);exit(1)
            logging.error(f"Failed to retrieve hierarchies: {e}")
            return
        for hierarchy_response in response_hierarchies:
            if hierarchy_response.HasField("error"):
                if hierarchy_response.error.code == StatusCode.NOT_FOUND:
                    continue
                else:
                    logging.warning(f"Error retrieving hierarchy: {hierarchy_response.error.message}")
                    continue
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