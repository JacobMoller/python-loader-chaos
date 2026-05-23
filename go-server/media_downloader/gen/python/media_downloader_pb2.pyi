from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RequestMediaRequest(_message.Message):
    __slots__ = ("media_uri",)
    MEDIA_URI_FIELD_NUMBER: _ClassVar[int]
    media_uri: str
    def __init__(self, media_uri: _Optional[str] = ...) -> None: ...

class RequestMediaResponse(_message.Message):
    __slots__ = ("media_path",)
    MEDIA_PATH_FIELD_NUMBER: _ClassVar[int]
    media_path: str
    def __init__(self, media_path: _Optional[str] = ...) -> None: ...

class ReleaseMediaRequest(_message.Message):
    __slots__ = ("media_uri",)
    MEDIA_URI_FIELD_NUMBER: _ClassVar[int]
    media_uri: str
    def __init__(self, media_uri: _Optional[str] = ...) -> None: ...

class ReleaseMediaResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
