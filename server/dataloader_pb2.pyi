from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AlphanumericalValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class CreateHierarchyRequest(_message.Message):
    __slots__ = ["name", "tagSetId"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    name: str
    tagSetId: int
    def __init__(self, name: _Optional[str] = ..., tagSetId: _Optional[int] = ...) -> None: ...

class CreateMediaRequest(_message.Message):
    __slots__ = ["media"]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    media: Media
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ...) -> None: ...

class CreateMediaStreamResponse(_message.Message):
    __slots__ = ["count", "error_message"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    count: int
    error_message: str
    def __init__(self, count: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class CreateNodeRequest(_message.Message):
    __slots__ = ["hierarchyId", "parentNodeId", "tagId"]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    hierarchyId: int
    parentNodeId: int
    tagId: int
    def __init__(self, tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class CreateTagRequest(_message.Message):
    __slots__ = ["alphanumerical", "date", "numerical", "tagSetId", "tagTypeId", "time", "timestamp"]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    alphanumerical: AlphanumericalValue
    date: DateValue
    numerical: NumericalValue
    tagSetId: int
    tagTypeId: int
    time: TimeValue
    timestamp: TimeStampValue
    def __init__(self, tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class CreateTagSetRequest(_message.Message):
    __slots__ = ["name", "tagTypeId"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    name: str
    tagTypeId: int
    def __init__(self, name: _Optional[str] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class CreateTagStreamRequest(_message.Message):
    __slots__ = ["alphanumerical", "date", "numerical", "tagId", "tagSetId", "tagTypeId", "time", "timestamp"]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    alphanumerical: AlphanumericalValue
    date: DateValue
    numerical: NumericalValue
    tagId: int
    tagSetId: int
    tagTypeId: int
    time: TimeValue
    timestamp: TimeStampValue
    def __init__(self, tagId: _Optional[int] = ..., tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class CreateTagStreamResponse(_message.Message):
    __slots__ = ["error_message", "id_map"]
    class IdMapEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ID_MAP_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    id_map: _containers.ScalarMap[int, int]
    def __init__(self, id_map: _Optional[_Mapping[int, int]] = ..., error_message: _Optional[str] = ...) -> None: ...

class CreateTaggingRequest(_message.Message):
    __slots__ = ["mediaId", "tagId"]
    MEDIAID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    mediaId: int
    tagId: int
    def __init__(self, mediaId: _Optional[int] = ..., tagId: _Optional[int] = ...) -> None: ...

class CreateTaggingStreamResponse(_message.Message):
    __slots__ = ["count", "error_message"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    count: int
    error_message: str
    def __init__(self, count: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class DateValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class EmptyRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetHierarchiesRequest(_message.Message):
    __slots__ = ["tagSetId"]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    tagSetId: int
    def __init__(self, tagSetId: _Optional[int] = ...) -> None: ...

class GetMediaByURIRequest(_message.Message):
    __slots__ = ["file_uri"]
    FILE_URI_FIELD_NUMBER: _ClassVar[int]
    file_uri: str
    def __init__(self, file_uri: _Optional[str] = ...) -> None: ...

class GetMediasRequest(_message.Message):
    __slots__ = ["file_type"]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    file_type: int
    def __init__(self, file_type: _Optional[int] = ...) -> None: ...

class GetNodesRequest(_message.Message):
    __slots__ = ["hierarchyId", "parentNodeId", "tagId"]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    hierarchyId: int
    parentNodeId: int
    tagId: int
    def __init__(self, tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class GetTagSetRequestByName(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class GetTagSetsRequest(_message.Message):
    __slots__ = ["tagTypeId"]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    tagTypeId: int
    def __init__(self, tagTypeId: _Optional[int] = ...) -> None: ...

class GetTagsRequest(_message.Message):
    __slots__ = ["tagSetId", "tagTypeId"]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    tagSetId: int
    tagTypeId: int
    def __init__(self, tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class Hierarchy(_message.Message):
    __slots__ = ["id", "name", "rootNodeId", "tagSetId"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ROOTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    rootNodeId: int
    tagSetId: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tagSetId: _Optional[int] = ..., rootNodeId: _Optional[int] = ...) -> None: ...

class HierarchyResponse(_message.Message):
    __slots__ = ["error_message", "hierarchy"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    HIERARCHY_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    hierarchy: Hierarchy
    def __init__(self, hierarchy: _Optional[_Union[Hierarchy, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class IdRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class IdResponse(_message.Message):
    __slots__ = ["error_message", "id"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    id: int
    def __init__(self, id: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class Media(_message.Message):
    __slots__ = ["file_type", "file_uri", "id", "thumbnail_uri"]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_URI_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    THUMBNAIL_URI_FIELD_NUMBER: _ClassVar[int]
    file_type: int
    file_uri: str
    id: int
    thumbnail_uri: str
    def __init__(self, id: _Optional[int] = ..., file_uri: _Optional[str] = ..., file_type: _Optional[int] = ..., thumbnail_uri: _Optional[str] = ...) -> None: ...

class MediaResponse(_message.Message):
    __slots__ = ["error_message", "media"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    MEDIA_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    media: Media
    def __init__(self, media: _Optional[_Union[Media, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class Node(_message.Message):
    __slots__ = ["hierarchyId", "id", "parentNodeId", "tagId"]
    HIERARCHYID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PARENTNODEID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    hierarchyId: int
    id: int
    parentNodeId: int
    tagId: int
    def __init__(self, id: _Optional[int] = ..., tagId: _Optional[int] = ..., hierarchyId: _Optional[int] = ..., parentNodeId: _Optional[int] = ...) -> None: ...

class NodeResponse(_message.Message):
    __slots__ = ["error_message", "node"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    node: Node
    def __init__(self, node: _Optional[_Union[Node, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class NumericalValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class RepeatedIdResponse(_message.Message):
    __slots__ = ["error_message", "ids"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, ids: _Optional[_Iterable[int]] = ..., error_message: _Optional[str] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ["error_message"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    def __init__(self, error_message: _Optional[str] = ...) -> None: ...

class Tag(_message.Message):
    __slots__ = ["alphanumerical", "date", "id", "numerical", "tagSetId", "tagTypeId", "time", "timestamp"]
    ALPHANUMERICAL_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NUMERICAL_FIELD_NUMBER: _ClassVar[int]
    TAGSETID_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    alphanumerical: AlphanumericalValue
    date: DateValue
    id: int
    numerical: NumericalValue
    tagSetId: int
    tagTypeId: int
    time: TimeValue
    timestamp: TimeStampValue
    def __init__(self, id: _Optional[int] = ..., tagSetId: _Optional[int] = ..., tagTypeId: _Optional[int] = ..., alphanumerical: _Optional[_Union[AlphanumericalValue, _Mapping]] = ..., timestamp: _Optional[_Union[TimeStampValue, _Mapping]] = ..., time: _Optional[_Union[TimeValue, _Mapping]] = ..., date: _Optional[_Union[DateValue, _Mapping]] = ..., numerical: _Optional[_Union[NumericalValue, _Mapping]] = ...) -> None: ...

class TagResponse(_message.Message):
    __slots__ = ["error_message", "tag"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    tag: Tag
    def __init__(self, tag: _Optional[_Union[Tag, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class TagSet(_message.Message):
    __slots__ = ["id", "name", "tagTypeId"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGTYPEID_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    tagTypeId: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., tagTypeId: _Optional[int] = ...) -> None: ...

class TagSetResponse(_message.Message):
    __slots__ = ["error_message", "tagset"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TAGSET_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    tagset: TagSet
    def __init__(self, tagset: _Optional[_Union[TagSet, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class Tagging(_message.Message):
    __slots__ = ["mediaId", "tagId"]
    MEDIAID_FIELD_NUMBER: _ClassVar[int]
    TAGID_FIELD_NUMBER: _ClassVar[int]
    mediaId: int
    tagId: int
    def __init__(self, mediaId: _Optional[int] = ..., tagId: _Optional[int] = ...) -> None: ...

class TaggingResponse(_message.Message):
    __slots__ = ["error_message", "tagging"]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TAGGING_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    tagging: Tagging
    def __init__(self, tagging: _Optional[_Union[Tagging, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class TimeStampValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class TimeValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...