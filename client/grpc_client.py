import os

import grpc
import dataloader_pb2 as rpc_objects
import dataloader_pb2_grpc

class LoaderClient:
    def __init__(self, grpc_host='localhost', grpc_port='50051') -> None:
        self.grpc_channel = grpc.insecure_channel(f'{grpc_host}:{grpc_port}')
        self.grpc_stub = dataloader_pb2_grpc.DataLoaderStub(self.grpc_channel)

    #!================ Media/media functions ======================================================================

    def get_media_by_id(self, id: int):
    # Get a single media with the given ID
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.getMediaById(request)
        return response

    
    def get_media_by_uri(self, file_uri: str):
    # Get a an media ID using its URI
        request = rpc_objects.GetMediaByURIRequest(file_uri=file_uri)
        response = self.grpc_stub.getMediaByURI(request)
        return response


    def get_medias(self, file_type: int):
        # List all the medias stored with an optional filter on the file type
        if file_type > 0:
            request = rpc_objects.GetMediasRequest(file_type=file_type)
        else :
            request = rpc_objects.GetMediasRequest()
        response_iterator = self.grpc_stub.getMedias(request)
        for response in response_iterator:
            yield response.media
            

    def add_dir(self, directory: str, formats):
    # Add files from a specified directory to the database.
        file_count = 0
        def add_media_requests_generator():
            nonlocal file_count
            for file in os.listdir(directory):
                if any(file.endswith('.' + ext) for ext in formats):
                    file_path = os.path.abspath(os.path.join(directory, file))
                    if file.lower().endswith(('jpg', 'png', 'bmp')):
                        file_type = 1	    # Image
                    elif file.lower().endswith(('mp3', 'wav', 'flac')):
                        file_type = 2		# Audio
                    elif file.lower().endswith(('mp4', 'avi')):
                        file_type = 3		# Video
                    else: 
                        file_type = 4		# Other
                    file_count += 1
                    request = rpc_objects.Media(
                        file_uri= file_path,
                        file_type= file_type,
                        thumbnail_uri= file_path
                    )
                    yield request
        
       
        response_iterator = self.grpc_stub.createMediaStream(add_media_requests_generator())
        for response in response_iterator:
            yield 'Info: added %d medias to database.' % (response.count)
        if file_count == 0 : yield 'Info: no files of the specified format were found in the directory.'
        else : yield 'Info: %d files were found in the directory.' % file_count


    def add_file(self, path: str, thumbnail_path: str = None):
    # Add a specific file to the database.
        file_path = path #os.path.abspath(path)
        if path.lower().endswith(('jpg', 'png', 'bmp')):
            file_type = 1		# Image
        elif path.lower().endswith(('mp3', 'wav', 'flac')):
            file_type = 2		# Audio
        elif path.lower().endswith(('mp4', 'avi')):
            file_type = 3		# Video
        else: 
            file_type = 4		# Other
        request = rpc_objects.Media(
            file_uri= file_path,
            file_type= file_type,
            thumbnail_uri= thumbnail_path if thumbnail_path else file_path
            )        
        response = self.grpc_stub.createMedia(request)
        return response


    # Delete a single media with the given ID
    def delete_media(self, id: int):
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.deleteMedia(request)
        return 'Success, media removed from database.'


    #!================ Tagset functions ======================================================================

    def get_tagsets(self, tagtype_id: int):
        if tagtype_id > 0: request = rpc_objects.GetTagSetsRequest(tagTypeId=tagtype_id)
        else : request = rpc_objects.GetTagSetsRequest()
        response_iterator = self.grpc_stub.getTagSets(request)
        for response in response_iterator:
            yield response.tagset

    def add_tagset(self, name: str, tagtype_id: int):
        request = rpc_objects.CreateTagSetRequest(name=name, tagTypeId=tagtype_id)
        response = self.grpc_stub.createTagSet(request)
        return response
    
    def get_tagset_by_id(self, id: int):
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.getTagSetById(request)
        return response
    
    def get_tagset_by_name(self, name: str):
        request = rpc_objects.GetTagSetRequestByName(name=name)
        response = self.grpc_stub.getTagSetByName(request)
        return response
        
    #!================ Tag functions ======================================================================
    
    def get_tags(self, tagtype_id: int, tagset_id: int):
        if tagtype_id > 0 and tagset_id > 0:
            request = rpc_objects.GetTagsRequest(
                tagTypeId=tagtype_id,
                tagSetId=tagset_id
            )
        elif tagtype_id > 0:
            request = rpc_objects.GetTagsRequest(
                tagTypeId=tagtype_id
            )
        elif tagset_id > 0:
            request = rpc_objects.GetTagsRequest(
                tagSetId=tagset_id
            )
        else:
            request = rpc_objects.GetTagsRequest()
            
        response_iterator = self.grpc_stub.getTags(request)
        for response in response_iterator:
            yield response.tag

    def add_tag(self, tagset_id: int, tagtype_id: int, value):
        match tagtype_id:
            case 1: 
                request = rpc_objects.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    alphanumerical = rpc_objects.AlphanumericalValue(value=str(value))
                )
            case 2: 
                value = value.replace('/', ' ')
                request = rpc_objects.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    timestamp = rpc_objects.TimeStampValue(value=str(value))
                )
            case 3: 
                request = rpc_objects.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    time = rpc_objects.TimeValue(value=str(value))
                )
            case 4: 
                request = rpc_objects.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    date = rpc_objects.DateValue(value=str(value))
                )
            case 5: 
                request = rpc_objects.CreateTagRequest(
                    tagSetId=tagset_id, 
                    tagTypeId=tagtype_id,
                    numerical = rpc_objects.NumericalValue(value=int(value))
                )
            case _:
                return 'Error : Not a valid tag type. Range is [1:5]'

        response = self.grpc_stub.createTag(request)
        return response
   
        
    def add_tags(self, tagset_id: int, tagtype_id: int, tags:list[dict]):
        def tags_iterator():
            for tag_item in tags:
                request = None
                try:
                    match tagtype_id:
                        case 1:
                                request = rpc_objects.CreateTagStreamRequest(
                                    tagId=tag_item.get('id'),
                                    tagSetId=tagset_id,
                                    tagTypeId=tagtype_id,
                                    alphanumerical = rpc_objects.AlphanumericalValue(value=str(tag_item.get('value')))
                                )
                        case 2:
                            request = rpc_objects.CreateTagStreamRequest(
                                tagId=tag_item.get('id'),
                                tagSetId=tagset_id,
                                tagTypeId=tagtype_id,
                                timestamp = rpc_objects.TimeStampValue(value=str(tag_item.get('value')))
                            )
                        case 3:
                            request = rpc_objects.CreateTagStreamRequest(
                                tagId=tag_item.get('id'),
                                tagSetId=tagset_id,
                                tagTypeId=tagtype_id,
                                time = rpc_objects.TimeValue(value=str(tag_item.get('value')))
                            )
                        case 4:
                            request = rpc_objects.CreateTagStreamRequest(
                                tagId=tag_item.get('id'),
                                tagSetId=tagset_id,
                                tagTypeId=tagtype_id,
                                date = rpc_objects.DateValue(value=str(tag_item.get('value')))
                            )
                        case 5:
                            request = rpc_objects.CreateTagStreamRequest(
                                tagId=tag_item.get('id'),
                                tagSetId=tagset_id,
                                tagTypeId=tagtype_id,
                                numerical = rpc_objects.NumericalValue(value=tag_item.get('value'))
                            )
                        case _:
                            raise "Error: wrong type %d" % tagtype_id
                except Exception as e:
                    print(f"Error: {e}")
                yield request
        response_iterator = self.grpc_stub.createTagStream(tags_iterator())
        try:
            for response in response_iterator:
                yield response.id_map
        except Exception as e:
            print(f"Tagset: {tagset_id}, TagType: {tagtype_id}")
            print(f"Error: {e}")
    
    def get_tag(self, tag_id: int):
        request = rpc_objects.IdRequest(id=tag_id)
        response = self.grpc_stub.getTag(request)
        return response

    #!================ Tagging functions ======================================================================
    def get_taggings(self):
        request = rpc_objects.Empty()
        response_iterator = self.grpc_stub.getTaggings(request)
        for response in response_iterator:
            yield response.tagging

    def add_tagging(self, tag_id: int, media_id: int):
        request = rpc_objects.CreateTaggingRequest(
            tagId=tag_id,
            mediaId=media_id
        )
        response = self.grpc_stub.createTagging(request)
        return response
    
    def add_taggings(self, media_id, tag_ids):
        def taggings_iterator():
            for tag_id in tag_ids:
                yield rpc_objects.CreateTaggingRequest(mediaId=media_id, tagId=tag_id)

        for response in self.grpc_stub.createTaggingStream(taggings_iterator()):
            yield response.count

    def get_medias_with_tag(self, id: int):
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.getMediasWithTag(request)
        return response.ids

    def get_media_tags(self, id: int):
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.getMediaTags(request)
        return response.ids

#!================ Hierarchy functions ====================================================================

    def get_hierarchies(self, tagset_id: int):
        if tagset_id > 0:
            request = rpc_objects.GetHierarchiesRequest(tagSetId=tagset_id)
        else: 
            request = rpc_objects.GetHierarchiesRequest()
        response_iterator = self.grpc_stub.getHierarchies(request)
        for response in response_iterator:
            yield response.hierarchy

    def add_hierarchy(self, name: str, tagset_id: int):
        request = rpc_objects.CreateHierarchyRequest(
            name=name,
            tagSetId=tagset_id
        )
        response = self.grpc_stub.createHierarchy(request)
        return response
    

    def get_hierarchy(self, id: int):
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.getHierarchy(request)
        return response



    #!================ Node functions ======================================================================

    def add_node(self, tag_id: int, hierarchy_id: int, parentnode_id: int):
        request = rpc_objects.CreateNodeRequest(
            tagId=tag_id,
            hierarchyId=hierarchy_id,
            parentNodeId=parentnode_id
        )
        response = self.grpc_stub.createNode(request)
        return response

        

    def add_rootnode(self, tag_id: int, hierarchy_id: int):
        request = rpc_objects.CreateNodeRequest(
            tagId=tag_id,
            hierarchyId=hierarchy_id
        )
        response = self.grpc_stub.createNode(request)
        return response
        
        
    def get_node(self, id: int):
        request = rpc_objects.IdRequest(id=id)
        response = self.grpc_stub.getNode(request)
        return response
    
    def get_nodes(self, hierarchy_id: int = 0, tag_id: int = 0, parentnode_id: int = 0):
        request = rpc_objects.GetNodesRequest(hierarchyId=hierarchy_id, tagId=tag_id, parentNodeId=parentnode_id)
        response_iterator = self.grpc_stub.getNodes(request)
        for response in response_iterator:
            yield response.node
    
    def delete_node(self, node_id: int):
        request = rpc_objects.IdRequest(id=node_id)
        response = self.grpc_stub.deleteNode(request)
        return f"Node {node_id} deleted."
    

    #!================ DB management ======================================================================

    # Reset the database
    def reset(self):
        request = rpc_objects.Empty()
        response = self.grpc_stub.resetDatabase(request)
        return 'Success, database was successfully reset.'