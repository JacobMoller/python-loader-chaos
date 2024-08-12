from concurrent import futures
import logging
import psycopg
from psycopg.rows import dict_row
import random

from words import WORDS
import grpc
import dataloader_pb2 as rpc_objects
from dataloader_pb2_grpc import DataLoaderServicer, add_DataLoaderServicer_to_server

BATCH_SIZE = 5000


class DataLoader(DataLoaderServicer):
# The DataLoader class is the implementation of the GRPC dataloader server
# It implements all the functions defined in the Protobuf file dataloader.proto

    def __init__(self) -> None:
        super().__init__()
        self.conn = psycopg.connect(
            conninfo="dbname=SpotifyDataBase user=postgres password=root host=localhost port=5432",
            row_factory=dict_row, # Retreive the columns by their names
            autocommit=True
        )
        cursor = self.conn.cursor() 
        cursor.execute("select version()")
        data = cursor.fetchone()
        print("Connection established to: ", data)
        cursor.close()
        
        # ! Uncomment to update an old schema with the new namings, triggers and tag_type in the tagsets table
        # try:
        #     cursor.execute(open("update_db_tables.sql", "r").read())
        #     print("DB has been updated")
        # except Exception as e:
        #     print("Error updating DB:" % repr(e))
        # cursor.close()

    def __del__(self):
        self.conn.close()


    #!================ Medias =============================================================================
    def getMedias(self, request: rpc_objects.GetMediasRequest, context):
    # Get all the medias stored in DB, with optional type filter
      
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMedias request" % thread_id)
        cursor = self.conn.cursor()
        try:
            sql = """SELECT * FROM public.medias;"""
            if request.file_type > 0 :
                sql += " WHERE file_type = %d" % request.file_type
            cursor.execute(sql)
            res = cursor.fetchall()
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            for row in res:
                yield rpc_objects.MediaResponse(
                    media=rpc_objects.Media(
                        id= row["id"],
                        file_uri= row["file_uri"],
                        file_type= row["file_type"],
                        thumbnail_uri= row["thumbnail_uri"]
                    )
                )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            yield rpc_objects.MediaResponse(error_message=repr(e))
        finally:
            cursor.close()


    def getMediaById(self, request: rpc_objects.IdRequest, context) -> rpc_objects.MediaResponse:
    # Get a single media with the given ID
     
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaById request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.medias WHERE id=%d" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                        id= result["id"],
                        file_uri= result["file_uri"],
                        file_type= result["file_type"],
                        thumbnail_uri= result["thumbnail_uri"]
                    )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.MediaResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  

    def getMediaByURI(self, request: rpc_objects.GetMediaByURIRequest, context) -> rpc_objects.MediaResponse:
    # Get a single media with the given URI
      
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediaIdFromURI request with URI=%s" % (thread_id, request.file_uri))
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.medias WHERE file_uri=%s"
            data = (request.file_uri,)  # The comma is to make it a tuple with one element
            cursor.execute(sql, data)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                        id= result["id"],
                        file_uri= result["file_uri"],
                        file_type= result["file_type"],
                        thumbnail_uri= result["thumbnail_uri"]
                    )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.MediaResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  



    def createMedia(self, request: rpc_objects.CreateMediaRequest, context) -> rpc_objects.MediaResponse:
    # Create a single media with given URI, type and thumbnail URI
     
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createMedia request" % thread_id)
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.medias WHERE file_uri = %s" 
            data = (request.media.file_uri,)                                # The comma is to make it a tuple with one element
            cursor.execute(sql, data)
            if cursor.rowcount > 0 :
                # print("[%s] -> File URI '%s' already exists in database" % (thread_id, request.media.file_uri))
                existing_media = cursor.fetchall()[0]
                if existing_media['file_type'] == request.media.file_type and existing_media['thumbnail_uri'] == request.media.thumbnail_uri:
                    # print("[%s] -> No conflicts, returning existing media" % thread_id)
                    return rpc_objects.MediaResponse(
                        media=rpc_objects.Media(
                            id= existing_media['id'],
                            file_uri= existing_media['file_uri'],
                            file_type= existing_media['file_type'],
                            thumbnail_uri= existing_media['thumbnail_uri']
                        ))
                else :
                    # print("[%s] -> Other fields conflict, returning error message" % thread_id)
                    return rpc_objects.MediaResponse(
                        error_message="Error: Media URI '%s' already exists with a different type or thumbnail_uri" % request.media.file_uri
                        )
                
            sql = "INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES (%s, %s, %s) RETURNING *;" 
            data = (request.media.file_uri, request.media.file_type, request.media.thumbnail_uri)
            cursor.execute(sql, data)
            response = cursor.fetchall()[0]
            return rpc_objects.MediaResponse(
                media=rpc_objects.Media(
                    id=response['id'],
                    file_uri=response['file_uri'],
                    file_type=response['file_type'],
                    thumbnail_uri=response['thumbnail_uri']
                )
            )
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.MediaResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  


    def createMediaStream(self, request_iterator, context):
    # Create multiple medias at the same time in batches, 
    # returns confirmation/error messages when a batch is added

        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createMediaStream request" % thread_id)
        cursor = self.conn.cursor()
        request_counter = 0
        sql = "INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES "
        data = ()

        for request in request_iterator:
            sql += "(%s, %s, %s)," 
            data += (
                request.media.file_uri,
                request.media.file_type,
                request.media.thumbnail_uri,
            )
            request_counter += 1

            if request_counter % BATCH_SIZE == 0:
                sql = sql[:-1] + ";"
                try:
                    cursor.execute(sql, data)
                    yield rpc_objects.CreateMediaStreamResponse(count=request_counter)
                except Exception as e:
                    yield rpc_objects.CreateMediaStreamResponse(error_message=repr(e))
                    # print("[%s] -> Error: packet addition failed" % thread_id)
                finally:
                    sql = "INSERT INTO public.medias (file_uri, file_type, thumbnail_uri) VALUES "
                    data = ()

        if request_counter % BATCH_SIZE > 0:
            sql = sql[:-1] + ";"
            try:
                cursor.execute(sql, data)
                response = rpc_objects.CreateMediaStreamResponse(count=request_counter)
            except Exception as e:
                response = rpc_objects.CreateMediaStreamResponse(error_message=repr(e))
                # print("[%s] -> %s" % (thread_id, repr(e)))
            yield response

        cursor.close()


    def deleteMedia(self, request: rpc_objects.IdRequest, context) -> rpc_objects.StatusResponse:
    # Delete a single media with the given ID

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received deleteMedia request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        response = rpc_objects.StatusResponse()
        try:
            sql = "DELETE FROM public.medias WHERE id=%d;" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0:
                raise Exception("Element not found")
            
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            response = rpc_objects.StatusResponse(error_message=repr(e))

        finally:
            cursor.close()
            return response


    #!================ TagSets ============================================================================
    def getTagSets(self, request: rpc_objects.GetTagSetsRequest, context):
    # Get all the tagsets stored in DB, with optional tagtype filter

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagSets request" % thread_id)
        cursor = self.conn.cursor()
        count = 0
        try:
            sql = """SELECT * FROM public.tagsets"""
            if request.tagTypeId > 0 :
                sql += " WHERE tagtype_id = %d" % request.tagTypeId

            cursor.execute(sql)
            res = cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched")
            for row in res:
                count += 1
                yield rpc_objects.TagSetResponse(
                    tagset=rpc_objects.TagSet(
                        id= row['id'],
                        name= row['name'],
                        tagTypeId= row['tagtype_id']
                    ))

        except Exception as e:
            yield rpc_objects.TagSetResponse(error_message=repr(e))
            print("[%s] -> %s" % (thread_id, repr(e)))

        finally:
            cursor.close()


    def getTagSetById(self, request: rpc_objects.IdRequest, context) -> rpc_objects.TagSetResponse:
    # Get a single tagset with the given ID

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagsetById request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.tagsets WHERE id=%d;" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                ))
        
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.TagSetResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  
        

    def getTagSetByName(self, request: rpc_objects.GetTagSetRequestByName, context) -> rpc_objects.TagSetResponse:
    # Get a single tagset with the given name

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTagSetByName request with name=%s" % (thread_id, request.name))
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.tagsets WHERE name=%s"
            data = (request.name,)                              # The comma is to make it a tuple with one element
            cursor.execute(sql, data)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")          
            result = cursor.fetchall()[0]
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= result['id'],
                    name= result['name'],
                    tagTypeId= result['tagtype_id']
                ))
        
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.TagSetResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  

    
    def createTagSet(self, request: rpc_objects.CreateTagSetRequest, context) -> rpc_objects.TagSetResponse:
    # Create of get Tagset: if a tagset with the same name and type exists, return the existent tagset.
    # If the name exists but with a different type, raise an error
    # Otherwise, create the new Tagset
    
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createTagSet request with name=%s and tag_type=%d" % (thread_id, request.name, request.tagTypeId))
        cursor = self.conn.cursor()
        try:
            # Check if the name already exists
            sql = "SELECT * FROM public.tagsets WHERE name = %s;"
            data = (request.name,)
            cursor.execute(sql, data)
            if cursor.rowcount > 0 :
                print("[%s] -> Tagset name '%s' already exists in database" % (thread_id, request.name))
                existing_tagset = cursor.fetchall()[0]

                # Check if the type matches
                if existing_tagset['tagtype_id'] == request.tagTypeId:
                    print("[%s] -> No type conflict, returning existing tagset" % thread_id)
                    return rpc_objects.TagSetResponse(
                        tagset=rpc_objects.TagSet(
                            id= existing_tagset['id'],
                            name= existing_tagset['name'],
                            tagTypeId= existing_tagset['tagtype_id']
                        ))
                else :
                    print("[%s] -> Type conflict, returning error message" % thread_id)
                    return rpc_objects.TagSetResponse(
                        error_message="Error: Tagset name '%s' already exists with a different type" % request.name
                        )
                
            # If name inexistent, create the new tagset
            sql = "INSERT INTO public.tagsets (name, tagtype_id) VALUES (%s, %s) RETURNING *;"
            data = (request.name, request.tagTypeId)
            cursor.execute(sql, data)
            inserted_tagset = cursor.fetchall()[0] 
            return rpc_objects.TagSetResponse(
                tagset=rpc_objects.TagSet(
                    id= inserted_tagset['id'],
                    name= inserted_tagset['name'],
                    tagTypeId= inserted_tagset['tagtype_id']
                )
            )
        
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.TagSetResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  
        

    #!================ Tags ===============================================================================
    def getTags(self, request: rpc_objects.GetTagsRequest, context):
    # Get all the tags stored in DB, with optional tagtype and tagset filters
      
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTags request" % thread_id)
        cursor = self.conn.cursor()
        count = 0
        try:
            sql = """SELECT
    t.id,
    t.tagtype_id,
    t.tagset_id,
    ant.name as text_value,
	tst.name as timestamp_value,
	tt.name as time_value,
	dt.name as date_value,
	nt.name as num_value
FROM
    public.tags t
LEFT JOIN
    public.alphanumerical_tags ant ON t.id = ant.id
LEFT JOIN
    public.timestamp_tags tst ON t.id = tst.id
LEFT JOIN
    public.time_tags tt ON t.id = tt.id
LEFT JOIN
    public.date_tags dt ON t.id = dt.id
LEFT JOIN
    public.numerical_tags nt ON t.id = nt.id
            """
            # Optional filters
            if request.tagSetId > 0 or request.tagTypeId > 0:
                sql += " WHERE "
                if request.tagSetId > 0 and request.tagTypeId > 0:
                    sql += "t.tagset_id = %d AND t.tagtype_id = %d" % (request.tagSetId, request.tagTypeId)
                elif request.tagSetId > 0:
                    sql += "t.tagset_id = %d" % request.tagSetId
                else:
                    sql += "t.tagtype_id = %d" % request.tagTypeId
                    
            cursor.execute(sql)
            res = cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched")
            for row in res:
                count += 1
                match row['tagtype_id']:
                    case 1:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            alphanumerical = rpc_objects.AlphanumericalValue(value=row['text_value'])
                        )
                    case 2:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            timestamp =  rpc_objects.TimeStampValue(value=str(row['timestamp_value']))
                        )
                    case 3:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            time = rpc_objects.TimeValue(value=str(row['time_value']))
                        )
                    case 4:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            date = rpc_objects.DateValue(value=str(row['date_value']))
                        )
                    case 5:
                        tag = rpc_objects.Tag(
                            id=row['id'],
                            tagSetId=row['tagset_id'],
                            tagTypeId=row['tagtype_id'],
                            numerical = rpc_objects.NumericalValue(value=row['num_value'])
                        )
                    case _:
                        tag = {}
                yield rpc_objects.TagResponse(
                    tag=tag
                    )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            yield rpc_objects.TagResponse(error_message=repr(e))
        # print("[%s] -> Fetched %d items from database" % (thread_id, count))
        finally:
            cursor.close()


    def getTag(self, request: rpc_objects.IdRequest, context) -> rpc_objects.TagResponse:
    # Get a single tag with the given ID
        
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getTag request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = """SELECT
    t.id,
    t.tagtype_id,
    t.tagset_id,
    ant.name as text_value,
	tst.name as timestamp_value,
	tt.name as time_value,
	dt.name as date_value,
	nt.name as num_value
FROM
    (SELECT * FROM public.tags WHERE id = %d) t
LEFT JOIN
    public.alphanumerical_tags ant ON t.id = ant.id
LEFT JOIN
    public.timestamp_tags tst ON t.id = tst.id
LEFT JOIN
    public.time_tags tt ON t.id = tt.id
LEFT JOIN
    public.date_tags dt ON t.id = dt.id
LEFT JOIN
    public.numerical_tags nt ON t.id = nt.id""" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            match result['tagtype_id']:
                case 1:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        alphanumerical = rpc_objects.AlphanumericalValue(value=result['text_value'])
                    )
                case 2:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        timestamp =  rpc_objects.TimeStampValue(value=str(result['timestamp_value']))
                    )
                case 3:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        time = rpc_objects.TimeValue(value=str(result['time_value']))
                    )
                case 4:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        date = rpc_objects.DateValue(value=str(result['date_value']))
                    )
                case 5:
                    tag = rpc_objects.Tag(
                        id=result['id'],
                        tagSetId=result['tagset_id'],
                        tagTypeId=result['tagtype_id'],
                        numerical = rpc_objects.NumericalValue(value=result['num_value'])
                    )
                case _:
                    tag = {}
            return rpc_objects.TagResponse(tag=tag)
            # print("[%s] -> Fetched 1 tag from database" % thread_id)
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.TagResponse(error_message=repr(e))

        finally:    # Runs before the return of each section
            cursor.close()  


    def createTag(self, request: rpc_objects.CreateTagRequest, context) -> rpc_objects.TagResponse:
    # Create or get tag if already existent. This function is lengthy as it needs to send and receive different RPC objects
    # depending on the type of the Tag.

        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createTag request with tagset_id=%d and tagtype_id=%d" % (thread_id, request.tagSetId, request.tagTypeId))
        tagset_id = request.tagSetId
        tagtype_id = request.tagTypeId
        cursor = self.conn.cursor()
        try:
            # Check existence of the tag
            sql = """SELECT t.id, t.tagtype_id, t.tagset_id, a.name as value FROM 
(SELECT * FROM public.tags WHERE tagset_id = %d AND tagtype_id = %d) t
LEFT JOIN """ % (tagset_id, tagtype_id)
            data = ()
            match request.tagTypeId:
                case 1: 
                    sql += "public.alphanumerical_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.alphanumerical.value,)
                case 2:                                                              
                    sql += "public.timestamp_tags a ON t.id = a.id WHERE a.name = %s"
                    data = (request.timestamp.value,)
                case 3:                                                              
                    sql += "public.time_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.time.value,)
                case 4:                                                              
                    sql += "public.date_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.date.value,)
                case 5: 
                    sql += "public.numerical_tags a ON t.id = a.id WHERE a.name = %s" 
                    data = (request.numerical.value,)
                case _:
                    raise Exception("invalid tag type: range is 1-5")
            cursor.execute(sql, data)

            # If cursor has result, i.e. the tag already exists, return it
            if cursor.rowcount > 0:
                # print("[%s] -> Tag already present in DB, returning tag info" % thread_id)
                result = cursor.fetchall()[0]
                match request.tagTypeId:
                    case 1:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                alphanumerical= rpc_objects.AlphanumericalValue(value=result['value'])
                            ))
                    case 2:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                timestamp= rpc_objects.TimeStampValue(value=str(result['value']))
                            ))
                    case 3:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                time = rpc_objects.TimeValue(value=str(result['value']))
                            ))
                    case 4:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                date = rpc_objects.DateValue(value=str(result['value']))
                            ))
                    case 5:
                        return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=result['tagtype_id'],
                                numerical= rpc_objects.NumericalValue(value=result['value'])
                            ))


            # print("[%s] -> Tag valid and non-existent, creating tag.." % thread_id)
            sql = "INSERT INTO public.tags (tagtype_id, tagset_id) VALUES (%d, %d) RETURNING id" % (tagtype_id, tagset_id)
            cursor.execute(sql)
            tag_id = cursor.fetchall()[0]['id']
            sql = "INSERT INTO public."
            match tagtype_id:
                case 1:
                    sql += "alphanumerical_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.alphanumerical.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                alphanumerical= rpc_objects.AlphanumericalValue(value=result['name'])
                            ))
                case 2:
                    sql += "timestamp_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.timestamp.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                timestamp= rpc_objects.TimeStampValue(value=str(result['name']))
                            ))
                case 3:
                    sql += "time_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.time.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                time= rpc_objects.TimeValue(value=str(result['name']))
                            ))
                case 4:
                    sql += "date_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.date.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                date= rpc_objects.DateValue(value=str(result['name']))
                            ))
                case 5:
                    sql += "numerical_tags (id, name, tagset_id) VALUES (%s, %s, %s) RETURNING *"
                    data = (tag_id, request.numerical.value, tagset_id)
                    cursor.execute(sql, data)
                    result = cursor.fetchall()[0]
                    return rpc_objects.TagResponse(
                            tag = rpc_objects.Tag(
                                id=result['id'],
                                tagSetId=result['tagset_id'],
                                tagTypeId=tagtype_id,
                                numerical= rpc_objects.NumericalValue(value=result['name'])
                            ))
                case _:
                    raise Exception("This should never happen")

        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.TagResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  


    def createTagStream(self, request_iterator, context):
    # Create multiple tags using batches of INSERT commands
    # Returns a map of the given IDs to the created IDs (used for JSON imports)

        print("createTagStream")
        tag_counter = 0
        tag_sql = "INSERT INTO public.tags (id, tagtype_id, tagset_id) VALUES (%s, %s, %s) RETURNING *;"
        tag_data = []
        tag_values = {}
        rownum_to_tagid_map = {}

        cursor = self.conn.cursor()
        for req in request_iterator:
            match req.tagTypeId:
                case 1:
                    tag_values[tag_counter] = req.alphanumerical.value
                case 2:
                    tag_values[tag_counter] = req.timestamp.value
                case 3:
                    tag_values[tag_counter] = req.time.value
                case 4:
                    tag_values[tag_counter] = req.date.value
                case 5:
                    tag_values[tag_counter] = req.numerical.value
                    
            rownum_to_tagid_map[tag_counter] = req.tagId
            # tag_sql += "(%s, %s, %s)," 
            tag_data.append([
                req.tagId,
                req.tagTypeId,
                req.tagSetId
            ])
            tag_counter += 1

            # When we reach the max batch size, we execute the query and proceed to add to the 
            # different sub-tables of tags.
            if tag_counter == BATCH_SIZE :
                # tag_sql = tag_sql[:-1] + " RETURNING *;"
                id_to_realid_map = {}
                try:
                    cursor.executemany(tag_sql, tag_data, returning=True)
                    i = 0
                    sql = ""
                    data = []
                    while True:
                        inserted_tag = cursor.fetchone()
                        tag_id = inserted_tag['id']
                        id_to_realid_map[rownum_to_tagid_map[i]] = tag_id
                        match inserted_tag['tagtype_id']:
                            case 1:
                                sql += "INSERT INTO public.alphanumerical_tags (id, name, tagset_id) VALUES (%s,%s,%s);\n"
                            case 2:
                                sql += "INSERT INTO public.timestamp_tags (id, name, tagset_id) VALUES (%s,%s,%s);\n"
                            case 3:
                                sql += "INSERT INTO public.time_tags (id, name, tagset_id) VALUES (%s,%s,%s);\n"
                            case 4:
                                sql += "INSERT INTO public.date_tags (id, name, tagset_id) VALUES (%s,%s,%s);\n"
                            case 5:
                                sql += "INSERT INTO public.numerical_tags (id, name, tagset_id) VALUES (%s,%s,%s);\n"
                        data.append([tag_id, tag_values[i], inserted_tag['tagset_id']])
                        i += 1
                        if not cursor.nextset():
                            break

                    cursor.executemany(sql, data)
                    yield rpc_objects.CreateTagStreamResponse(
                        id_map=id_to_realid_map
                    )

                except Exception as e:
                    cursor.close()
                    yield rpc_objects.CreateTagStreamResponse(
                    error_message="Error adding batch of tags: %s" % repr(e)
                    )     
                finally:
                    tag_counter = 0
                    tag_sql = "INSERT INTO public.tags (tagtype_id, tagset_id) VALUES "
                    tag_data = ()
                    tag_values = {}
                    rownum_to_tagid_map = {}

        # Add the remaining tags
        if tag_counter > 0:
            # tag_sql = tag_sql[:-1] + " RETURNING *;"
            id_to_realid_map = {}
            try:
                cursor.executemany(tag_sql, tag_data, returning=True)
                i = 0
                sql = ""
                data = []
                while True:
                    inserted_tag = cursor.fetchone()
                    tag_id = inserted_tag['id']
                    id_to_realid_map[rownum_to_tagid_map[i]] = tag_id
                    match inserted_tag['tagtype_id']:
                        case 1:
                            sql = "INSERT INTO public.alphanumerical_tags (id, name, tagset_id) VALUES (%s,%s,%s);"
                        case 2:
                            sql = "INSERT INTO public.timestamp_tags (id, name, tagset_id) VALUES (%s,%s,%s);"
                        case 3:
                            sql = "INSERT INTO public.time_tags (id, name, tagset_id) VALUES (%s,%s,%s);"
                        case 4:
                            sql = "INSERT INTO public.date_tags (id, name, tagset_id) VALUES (%s,%s,%s);"
                        case 5:
                            sql += "INSERT INTO public.numerical_tags (id, name, tagset_id) VALUES (%s,%s,%s);"
                    data.append([tag_id, tag_values[i], inserted_tag['tagset_id']])
                    i += 1
                    if not cursor.nextset():
                        break

                cursor.executemany(sql, data)
                yield rpc_objects.CreateTagStreamResponse(
                        id_map=id_to_realid_map
                    )
            except Exception as e:
                yield rpc_objects.CreateTagStreamResponse(
                    error_message="Error adding batch of tags: %s" % repr(e)
                )
            
            finally:    # Runs before the return of each section
                cursor.close()      


    #!================ Taggings (ObjectTagRelations) ======================================================
    def getTaggings(self, request: rpc_objects.EmptyRequest, context):
    # Get all the taggings stored in DB.

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getTaggings request" % thread_id)
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.taggings"
            cursor.execute(sql)
            res = cursor.fetchall()
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            for row in res:
                yield rpc_objects.TaggingResponse(
                    tagging=rpc_objects.Tagging(
                        mediaId=row['object_id'],
                        tagId=row['tag_id']
                    ))
                
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            yield rpc_objects.TaggingResponse(error_message=repr(e))

        finally:
            cursor.close()


    def getMediasWithTag(self, request: rpc_objects.IdRequest, context) -> rpc_objects.RepeatedIdResponse :
    # Get IDs of all medias with a given tag (only providing the ID)
        
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getMediasWithTag request with tag_id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = ("SELECT object_id FROM public.taggings WHERE tag_id = %d"
                   % request.id)
            cursor.execute(sql)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            else: result = [item for item, in cursor]
            return rpc_objects.RepeatedIdResponse(ids=result)

        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.RepeatedIdResponse(error_message=repr(e))

        finally:    # Runs before the return of each section
            cursor.close()  


    def getMediaTags(self, request: rpc_objects.IdRequest, context) -> rpc_objects.RepeatedIdResponse :
    # Get IDs of all tags of a given media (only providing the ID)
    
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getMediaTags request with media_id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = ("SELECT tag_id FROM public.taggings WHERE object_id = %d"
                   % request.id)
            cursor.execute(sql)
            if cursor.rowcount == 0: raise Exception("No results were fetched")
            result = [item for item, in cursor]
            return rpc_objects.RepeatedIdResponse(ids=result)

        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.RepeatedIdResponse(error_message=repr(e))

        finally:    # Runs before the return of each section
            cursor.close()  


    def createTagging(self, request: rpc_objects.CreateTaggingRequest, context) -> rpc_objects.TaggingResponse:
    # Create a tagging, i.e. associate a given tag to a given media. Return the existing tagging if already present in DB
    
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received createTagging request with media_id=%d and tag_id=%d" % (thread_id, request.mediaId, request.tagId))
        cursor = self.conn.cursor()
        try:
            # Check for existence
            sql = ("SELECT * FROM public.taggings WHERE object_id = %d AND tag_id = %d"
                   % (request.mediaId, request.tagId))
            cursor.execute(sql)
            if cursor.rowcount == 0: 
                sql = ("INSERT INTO public.taggings (object_id, tag_id) VALUES (%d, %d) RETURNING *;" 
                % (request.mediaId, request.tagId))
                cursor.execute(sql)
            else:
                pass
                # print("[%s] -> Tagging already present in database, returning value to client" % thread_id)

            tagging = cursor.fetchall()[0]
            return rpc_objects.TaggingResponse(
                tagging = rpc_objects.Tagging(
                    mediaId=tagging['object_id'],
                    tagId=tagging['tag_id']
                ))
            
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.TaggingResponse(error_message=repr(e))
        
            cursor.close()  

    
    def createTaggingStream(self, request_iterator, context):
    # Create multiple taggings in a row, using batches of INSERT queries
    # Returns the amount added at each batch addition (similiar behaviour as in createMediaStream)

        cursor = self.conn.cursor()
        request_counter = 0
        sql = "INSERT INTO public.taggings (object_id, tag_id) VALUES "
        data = ()

        for request in request_iterator:

            request_counter += 1
            sql += "(%s, %s)," 
            data += (request.mediaId, request.tagId,)

            if request_counter % BATCH_SIZE == 0:
                sql = sql[:-1] + ";"
                try:
                    cursor.execute(sql, data)
                    response = rpc_objects.CreateTaggingStreamResponse(count=request_counter)
                except Exception as e:
                    response = rpc_objects.CreateTaggingStreamResponse(error_message=repr(e))
                yield response

                sql = "INSERT INTO public.taggings (object_id, tag_id) VALUES "
                data = ()

        if request_counter % BATCH_SIZE > 0:
            sql = sql[:-1] + ";"
            try:
                cursor.execute(sql, data)
                response = rpc_objects.CreateTaggingStreamResponse(count=request_counter)
            except Exception as e:
                response = rpc_objects.CreateTaggingStreamResponse(error_message=repr(e))
            yield response
            
        cursor.close()

    #!================ Hierarchies  =======================================================================

    def getHierarchies(self, request: rpc_objects.GetHierarchiesRequest, context) : 
    # Get all the hierarchies stored in DB, with optional tagset filter
        
        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getHierarchies request" % thread_id)
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.hierarchies"
            if request.tagSetId > 0:
                sql += " WHERE tagset_id = %d" % request.tagSetId
            cursor.execute(sql)
            res = cursor.fetchall()
            if len(res) == 0 : raise Exception("No results were fetched") 
            for row in res:
                yield rpc_objects.HierarchyResponse(
                    hierarchy=rpc_objects.Hierarchy(
                        id=row['id'],
                        name=row['name'],
                        tagSetId=row['tagset_id'],
                        rootNodeId=row['rootnode_id']
                    ))
                
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            yield rpc_objects.HierarchyResponse(error_message=repr(e))

        finally:
            cursor.close()


    def getHierarchy(self, request: rpc_objects.IdRequest, context) -> rpc_objects.HierarchyResponse:
    # Get a single hierarchy with the given ID

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received getHierarchy request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = """SELECT * FROM public.hierarchies WHERE id=%d""" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            return rpc_objects.HierarchyResponse(
                hierarchy=rpc_objects.Hierarchy(
                    id=result['id'],
                    name=result['name'],
                    tagSetId=result['tagset_id'],
                    rootNodeId=result['rootnode_id']
                ))
        
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.HierarchyResponse(error_message=repr(e)) 
        
        finally:    # Runs before the return of each section
            cursor.close()   


    def createHierarchy(self, request: rpc_objects.CreateHierarchyRequest, context) -> rpc_objects.HierarchyResponse:
    # Create hierarchy with the given name and tagset_id, or returns it if already existent

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received createHierarchy request with name = %s" % (thread_id, request.name))
        cursor = self.conn.cursor()
        try:
            # Note: the pair (name, tagset_id) is unique
            sql = "SELECT * FROM public.hierarchies WHERE name = %s AND tagset_id = %s"
            data = (request.name, request.tagSetId)
            cursor.execute(sql, data)
            if cursor.rowcount == 0:
                sql = """INSERT INTO public.hierarchies (name, tagset_id) VALUES (%s, %s) RETURNING *;"""
                cursor.execute(sql, data)
            else:
                print("[%s] -> Hierarchy already present in database, returning value to client" % thread_id)

            response = cursor.fetchall()[0]
            return rpc_objects.HierarchyResponse(
                hierarchy=rpc_objects.Hierarchy(
                    id=response['id'],
                    name=response['name'],
                    tagSetId=response['tagset_id'],
                    rootNodeId=response['rootnode_id']
                )
            )
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.HierarchyResponse(error_message=repr(e))
        
        finally:    # Runs before the return of each section
            cursor.close()  


    #!================ Nodes ==============================================================================
    def getNodes(self, request: rpc_objects.GetNodesRequest, context) :
    # Get all the nodes stored in DB, with optional hierarchy, tag or parent node filters

        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getNodes request" % (thread_id))
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM public.nodes"
            # Reflexion: a node is a tag reference in a hierarchy, 
            # so if more than one filter is applied we'll get only one result
            if request.hierarchyId > 0 or request.tagId > 0 or request.parentNodeId > 0:
                sql += " WHERE"
                if request.hierarchyId > 0:
                    sql += " hierarchy_id = %d AND" % request.hierarchyId
                if request.tagId > 0:
                    sql += " tag_id = %d AND" % request.tagId
                if request.parentNodeId > 0:
                    sql += " parentnode_id = %d AND" % request.parentNodeId
                sql = sql[:len(sql)-3]       
            cursor.execute(sql)
            results = cursor.fetchall()
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            for result in results:
                yield rpc_objects.NodeResponse(
                    node=rpc_objects.Node(
                        id=result['id'],
                        tagId=result['tag_id'],
                        hierarchyId=result['hierarchy_id'],
                        parentNodeId=result['parentnode_id']
                    )
                )
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            yield rpc_objects.NodeResponse(error_message=repr(e))
        
        finally:
            cursor.close()


    def getNode(self, request: rpc_objects.IdRequest, context) -> rpc_objects.NodeResponse:
    # Get a single node with the given ID

        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received getNode request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = """SELECT * FROM public.nodes WHERE id=%d""" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0 : raise Exception("No results were fetched")
            result = cursor.fetchall()[0]
            return rpc_objects.NodeResponse(
                node=rpc_objects.Node(
                    id=result['id'],
                    tagId=result['tag_id'],
                    hierarchyId=result['hierarchy_id'],
                    parentNodeId=result['parentnode_id']
                )
            )
        except Exception as e:
            # print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.NodeResponse(error_message=repr(e))   
        
        finally:    # Runs before the return of each section
            cursor.close()  


    def createNode(self, request: rpc_objects.CreateNodeRequest, context) -> rpc_objects.NodeResponse :
        # thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        # print("[%s] Received creatNode request" % thread_id)
        cursor = self.conn.cursor()
        
        if request.parentNodeId:
        # We're not trying to add a root node
            try:
                sql = ("SELECT * FROM public.nodes WHERE tag_id = %d AND hierarchy_id = %d AND parentnode_id = %d"
                    % (
                            request.tagId,
                            request.hierarchyId,
                            request.parentNodeId
                    ))
                cursor.execute(sql)
                if cursor.rowcount == 0:
                    sql = """INSERT INTO public.nodes (tag_id, hierarchy_id, parentnode_id) VALUES (%d, %d, %d) RETURNING *;""" % (
                            request.tagId,
                            request.hierarchyId,
                            request.parentNodeId
                    )      
                    cursor.execute(sql)
                else: 
                    # print("[%s] -> Node already present in database, returning value to client" % thread_id)
                    pass
                response = cursor.fetchall()[0]
                return rpc_objects.NodeResponse(
                    node=rpc_objects.Node(
                        id=response['id'],
                        tagId=response['tag_id'],
                        hierarchyId=response['hierarchy_id'],
                        parentNodeId=response['parentnode_id']
                    )
                )
            except Exception as e:
                # print("[%s] -> %s" % (thread_id, repr(e)))
                return rpc_objects.NodeResponse(error_message=repr(e))
            
            finally:
                cursor.close()
        
        else: 
        # We are trying to add a root node to hierarchy. Can be done only if it doesn't have any
            try:
                sql = ("SELECT * FROM public.nodes WHERE tag_id = %d AND hierarchy_id = %d AND parentnode_id IS NULL"
                    % (
                        request.tagId,
                        request.hierarchyId
                    ))
                cursor.execute(sql)
                if cursor.rowcount == 0:
                    sql = ("INSERT INTO public.nodes (tag_id, hierarchy_id) VALUES (%d, %d) RETURNING *;"
                           % (
                                request.tagId,
                                request.hierarchyId
                            ))
                    cursor.execute(sql)
                    node = cursor.fetchall()[0]
                    sql = ("UPDATE public.hierarchies SET rootnode_id = %d WHERE id = %d" 
                           % (node['id'], request.hierarchyId))
                    cursor.execute(sql)
                    self.conn.commit()
                else:
                    # print("[%s] -> Node already present in database, returning value to client" % thread_id)
                    node = cursor.fetchall()[0]
                return rpc_objects.NodeResponse(
                        node=rpc_objects.Node(
                            id=node['id'],
                            tagId=node['tag_id'],
                            hierarchyId=node['hierarchy_id']
                        )
                    )

            except Exception as e:
                # print("[%s] -> %s" % (thread_id, repr(e)))
                return rpc_objects.NodeResponse(error_message=repr(e))
            
            finally:
                cursor.close()

    def deleteNode(self, request: rpc_objects.IdRequest, context) -> rpc_objects.StatusResponse:
    # Delete a single node with a given ID. The process is as follows:
    # 
    # RootNode ----- ParentNode ---- *NodeToRemove* ---- ChildNodes
    # PROCESS: 
    # NodeToRemove has a parent node ?
    #     YES: Get the ChildNodes and set their parent node to the parent node of NodeToRemove if they exist
    #     NO: NodeToRemove is RootNode. count(childNodes) = 1 ? YES: set new rootnode to ChildNode | NO: throw an error
    # Delete NodeToRemove

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received deleteNode request with id=%d" % (thread_id, request.id))
        cursor = self.conn.cursor()
        try:
            sql = "SELECT parentnode_id FROM public.nodes WHERE id = %d" % request.id
            cursor.execute(sql)
            if cursor.rowcount == 0:
                raise Exception("Element not found")
            node_to_remove_parentnode_id = cursor.fetchall()[0]['parentnode_id']
            if node_to_remove_parentnode_id is not None:
                sql = "UPDATE public.nodes SET parentnode_id = %d WHERE parentnode_id = %d" % (node_to_remove_parentnode_id, request.id)
                cursor.execute(sql)
            else:
                sql = "SELECT id FROM public.nodes WHERE parentnode_id = %d" % request.id
                cursor.execute(sql)
                if cursor.rowcount > 1:
                    raise Exception("Cannot delete rootnode with mutiple children. Please delete children first until there is a single child left.")
                if cursor.rowcount == 0:
                    sql = "UPDATE public.hierarchies SET rootnode_id = NULL WHERE rootnode_id = %d" % request.id
                    cursor.execute(sql)
                if cursor.rowcount == 1:
                    singlechild_id = cursor.fetchall()[0]['id']
                    sql = """UPDATE public.hierarchies SET rootnode_id = %d WHERE rootnode_id = %d;
UPDATE public.nodes SET parentnode_id = NULL WHERE id = %d;""" % (singlechild_id, request.id, singlechild_id)
                    cursor.execute(sql)

            sql = "DELETE FROM public.nodes WHERE id=%d;" % request.id
            cursor.execute(sql)
            return rpc_objects.StatusResponse()
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.StatusResponse(error_message=repr(e))
        
        finally:
            cursor.close()
        

    #!================ DB Management ======================================================================
    def resetDatabase(self, request: rpc_objects.EmptyRequest, context) -> rpc_objects.StatusResponse:
    # Reads and executes the DDL, which does 2 things: drop the schemas, and recreate all the tables and rules

        thread_id = "%s-%d" % (random.choice(WORDS), random.randint(1000,9999))
        print("[%s] Received ResetDatabase request" % thread_id)
        cursor = self.conn.cursor()
        try:
            cursor.execute(open("../ddl.sql", "r").read())
            print("[%s] -> SUCCESS: DB has been reset" % thread_id)
            return rpc_objects.StatusResponse()
        except Exception as e:
            print("[%s] -> %s" % (thread_id, repr(e)))
            return rpc_objects.StatusResponse(error_message=repr(e))
        finally:
            cursor.close()


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_DataLoaderServicer_to_server(DataLoader(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    server.start()
    print("Server listening at %s" % listen_addr)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()