import datetime

from ledger._proto import events_pb2 as _events_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_OK: _ClassVar[RunStatus]
    RUN_STATUS_ERROR: _ClassVar[RunStatus]
    RUN_STATUS_UNFINISHED: _ClassVar[RunStatus]
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_OK: RunStatus
RUN_STATUS_ERROR: RunStatus
RUN_STATUS_UNFINISHED: RunStatus

class RunSummary(_message.Message):
    __slots__ = ("run_id", "mode", "source_run_id", "graph", "status", "error", "calls", "live", "diverged", "resumed", "events", "started_at", "finished_at", "diagnoses")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CALLS_FIELD_NUMBER: _ClassVar[int]
    LIVE_FIELD_NUMBER: _ClassVar[int]
    DIVERGED_FIELD_NUMBER: _ClassVar[int]
    RESUMED_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSES_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    mode: _events_pb2.Mode
    source_run_id: str
    graph: str
    status: RunStatus
    error: _events_pb2.Error
    calls: int
    live: int
    diverged: int
    resumed: int
    events: int
    started_at: _timestamp_pb2.Timestamp
    finished_at: _timestamp_pb2.Timestamp
    diagnoses: int
    def __init__(self, run_id: _Optional[str] = ..., mode: _Optional[_Union[_events_pb2.Mode, str]] = ..., source_run_id: _Optional[str] = ..., graph: _Optional[str] = ..., status: _Optional[_Union[RunStatus, str]] = ..., error: _Optional[_Union[_events_pb2.Error, _Mapping]] = ..., calls: _Optional[int] = ..., live: _Optional[int] = ..., diverged: _Optional[int] = ..., resumed: _Optional[int] = ..., events: _Optional[int] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finished_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., diagnoses: _Optional[int] = ...) -> None: ...

class Diagnosis(_message.Message):
    __slots__ = ("id", "run_id", "created_at", "json")
    ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    id: int
    run_id: str
    created_at: _timestamp_pb2.Timestamp
    json: str
    def __init__(self, id: _Optional[int] = ..., run_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., json: _Optional[str] = ...) -> None: ...

class ListRunsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListRunsResponse(_message.Message):
    __slots__ = ("runs",)
    RUNS_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[RunSummary]
    def __init__(self, runs: _Optional[_Iterable[_Union[RunSummary, _Mapping]]] = ...) -> None: ...

class GetRunRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class GetRunResponse(_message.Message):
    __slots__ = ("summary", "events")
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    summary: RunSummary
    events: _containers.RepeatedCompositeFieldContainer[_events_pb2.Event]
    def __init__(self, summary: _Optional[_Union[RunSummary, _Mapping]] = ..., events: _Optional[_Iterable[_Union[_events_pb2.Event, _Mapping]]] = ...) -> None: ...

class WatchRunRequest(_message.Message):
    __slots__ = ("run_id", "from_index")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_INDEX_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    from_index: int
    def __init__(self, run_id: _Optional[str] = ..., from_index: _Optional[int] = ...) -> None: ...

class WatchRunResponse(_message.Message):
    __slots__ = ("event",)
    EVENT_FIELD_NUMBER: _ClassVar[int]
    event: _events_pb2.Event
    def __init__(self, event: _Optional[_Union[_events_pb2.Event, _Mapping]] = ...) -> None: ...

class GetPayloadRequest(_message.Message):
    __slots__ = ("sha256",)
    SHA256_FIELD_NUMBER: _ClassVar[int]
    sha256: str
    def __init__(self, sha256: _Optional[str] = ...) -> None: ...

class GetPayloadResponse(_message.Message):
    __slots__ = ("payload",)
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    payload: _events_pb2.Payload
    def __init__(self, payload: _Optional[_Union[_events_pb2.Payload, _Mapping]] = ...) -> None: ...

class ListDiagnosesRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class ListDiagnosesResponse(_message.Message):
    __slots__ = ("diagnoses",)
    DIAGNOSES_FIELD_NUMBER: _ClassVar[int]
    diagnoses: _containers.RepeatedCompositeFieldContainer[Diagnosis]
    def __init__(self, diagnoses: _Optional[_Iterable[_Union[Diagnosis, _Mapping]]] = ...) -> None: ...
