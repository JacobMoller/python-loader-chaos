from google.rpc import status_pb2 as _status_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IdRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class IdResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class RepeatedIdResponse(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, ids: _Optional[_Iterable[int]] = ...) -> None: ...

class Media(_message.Message):
    __slots__ = ("id", "file_uri", "file_type", "thumbnail_uri")
    ID_FIELD_NUMBER: _ClassVar[int]
    FILE_URI_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    THUMBNAIL_URI_FIELD_NUMBER: _ClassVar[int]
    id: int
    file_uri: str
    file_type: int
    thumbnail_uri: str
    def __init__(self, id: _Optional[int] = ..., file_uri: _Optional[str] = ..., file_type: _Optional[int] = ..., thumbnail_uri: _Optional[str] = ...) -> None: ...

class GetMediasRequest(_message.Message):
    __slots__ = ("file_type",)
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    file_type: int
    def __init__(self, file_type: _Optional[int] = ...) -> None: ...

class GetMediaByURIRequest(_message.Message):
    __slots__ = ("file_uri",)
    FILE_URI_FIELD_NUMBER: _ClassVar[int]
    file_uri: str
    def __init__(self, file_uri: _Optional[str] = ...) -> None: ...

class StreamingMediaResponse(_message.Message):
    __slots__ = ("media", "error")
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    media: Media
    error: _status_pb2.Status
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class CreateMediaStreamResponse(_message.Message):
    __slots__ = ("count", "error")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    count: int
    error: _status_pb2.Status
    def __init__(self, count: _Optional[int] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class TagSet(_message.Message):
    __slots__ = ("id", "name", "tagTypeId")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    tagTypeId: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class GetTagSetsRequest(_message.Message):
    __slots__ = ("tagTypeId",)
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    tagTypeId: int
    def __init__(self, tagTypeId: _Optional[int] = ...) -> None: ...

class GetTagSetRequestByName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class CreateTagSetRequest(_message.Message):
    __slots__ = ("name", "tagTypeId")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    name: str
    tagTypeId: int
    def __init__(self, name: _Optional[str] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class StreamingTagSetResponse(_message.Message):
    __slots__ = ("tagset", "error")
    TAGSET_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tagset: TagSet
    error: _status_pb2.Status
    def __init__(self, tagset: _Optional[_Union[TagSet, _Mapping]] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class Tag(_message.Message):
    __slots__ = ("id", "tagSetId", "tagTypeId", "alphanumerical", "timestamp", "time", "date", "numerical")
    ID_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    id: int
    tagSetId: int
    tagTypeId: int
    alphanumerical: AlphanumericalValue
    timestamp: TimeStampValue
    time: TimeValue
    date: DateValue
    numerical: NumericalValue
    def __init__(self, id: _Optional[int] = ..., tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class AlphanumericalValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class NumericalValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class DateValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class TimeValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class TimeStampValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class GetTagsRequest(_message.Message):
    __slots__ = ("tagSetId", "tagTypeId")
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    tagSetId: int
    tagTypeId: int
    def __init__(self, tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class CreateTagRequest(_message.Message):
    __slots__ = ("tagSetId", "tagTypeId", "alphanumerical", "timestamp", "time", "date", "numerical")
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    tagSetId: int
    tagTypeId: int
    alphanumerical: AlphanumericalValue
    timestamp: TimeStampValue
    time: TimeValue
    date: DateValue
    numerical: NumericalValue
    def __init__(self, tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class StreamingTagResponse(_message.Message):
    __slots__ = ("tag", "error")
    TAG_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tag: Tag
    error: _status_pb2.Status
    def __init__(self, tag: _Optional[_Union[Tag, _Mapping]] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class CreateTagStreamRequest(_message.Message):
    __slots__ = ("tagId", "tagSetId", "tagTypeId", "alphanumerical", "timestamp", "time", "date", "numerical")
    TAGID_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    tagId: int
    tagSetId: int
    tagTypeId: int
    alphanumerical: AlphanumericalValue
    timestamp: TimeStampValue
    time: TimeValue
    date: DateValue
    numerical: NumericalValue
    def __init__(self, tagId: _Optional[int] = ..., tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class CreateTagStreamResponse(_message.Message):
    __slots__ = ("id_map", "error_message")
    class IdMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    ID_MAP_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    id_map: _containers.ScalarMap[int, int]
    error_message: str
    def __init__(self, id_map: _Optional[_Mapping[int, int]] = ..., error_message: _Optional[str] = ...) -> None: ...

class Tagging(_message.Message):
    __slots__ = ("mediaId", "tagId")
    MEDIAID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    mediaId: int
    tagId: int
    def __init__(self, mediaId: _Optional[int] = ..., tagId: _Optional[int] = ...) -> None: ...

class CreateTaggingRequest(_message.Message):
    __slots__ = ("mediaId", "tagId")
    MEDIAID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    mediaId: int
    tagId: int
    def __init__(self, mediaId: _Optional[int] = ..., tagId: _Optional[int] = ...) -> None: ...

class StreamingTaggingResponse(_message.Message):
    __slots__ = ("tagging", "error")
    TAGGING_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tagging: Tagging
    error: _status_pb2.Status
    def __init__(self, tagging: _Optional[_Union[Tagging, _Mapping]] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class CreateTaggingStreamResponse(_message.Message):
    __slots__ = ("count", "error")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    count: int
    error: _status_pb2.Status
    def __init__(self, count: _Optional[int] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class Hierarchy(_message.Message):
    __slots__ = ("id", "name", "tagSetId", "rootNodeId")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    ROOTNODEID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    tagSetId: int
    rootNodeId: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tagSetId: _Optional[int] = ..., rootNodeId: _Optional[int] = ...) -> None: ...

class GetHierarchiesRequest(_message.Message):
    __slots__ = ("tagSetId",)
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    tagSetId: int
    def __init__(self, tagSetId: _Optional[int] = ...) -> None: ...

class CreateHierarchyRequest(_message.Message):
    __slots__ = ("name", "tagSetId")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    name: str
    tagSetId: int
    def __init__(self, name: _Optional[str] = ..., tagSetId: _Optional[int] = ...) -> None: ...

class StreamingHierarchyResponse(_message.Message):
    __slots__ = ("hierarchy", "error")
    HIERARCHY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    hierarchy: Hierarchy
    error: _status_pb2.Status
    def __init__(self, hierarchy: _Optional[_Union[Hierarchy, _Mapping]] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...

class Node(_message.Message):
    __slots__ = ("id", "tagId", "hierarchyId", "parentNodeId")
    ID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    id: int
    tagId: int
    hierarchyId: int
    parentNodeId: int
    def __init__(self, id: _Optional[int] = ..., tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class CreateNodeRequest(_message.Message):
    __slots__ = ("tagId", "hierarchyId", "parentNodeId")
    TAGID_FIELD_NUMBER: _ClassVar[int]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    tagId: int
    hierarchyId: int
    parentNodeId: int
    def __init__(self, tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class GetNodesRequest(_message.Message):
    __slots__ = ("tagId", "hierarchyId", "parentNodeId")
    TAGID_FIELD_NUMBER: _ClassVar[int]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    tagId: int
    hierarchyId: int
    parentNodeId: int
    def __init__(self, tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class StreamingNodeResponse(_message.Message):
    __slots__ = ("node", "error")
    NODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    node: Node
    error: _status_pb2.Status
    def __init__(self, node: _Optional[_Union[Node, _Mapping]] = ..., error: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...) -> None: ...
