import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Mode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODE_UNSPECIFIED: _ClassVar[Mode]
    MODE_RECORD: _ClassVar[Mode]
    MODE_REPLAY: _ClassVar[Mode]
    MODE_FORK: _ClassVar[Mode]

class DivergenceReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DIVERGENCE_REASON_UNSPECIFIED: _ClassVar[DivergenceReason]
    DIVERGENCE_REASON_CHANGED_REQUEST: _ClassVar[DivergenceReason]
    DIVERGENCE_REASON_UNRECORDED_CALL: _ClassVar[DivergenceReason]
    DIVERGENCE_REASON_CHANGED_KIND: _ClassVar[DivergenceReason]
    DIVERGENCE_REASON_EDITED: _ClassVar[DivergenceReason]
    DIVERGENCE_REASON_RERUN: _ClassVar[DivergenceReason]
MODE_UNSPECIFIED: Mode
MODE_RECORD: Mode
MODE_REPLAY: Mode
MODE_FORK: Mode
DIVERGENCE_REASON_UNSPECIFIED: DivergenceReason
DIVERGENCE_REASON_CHANGED_REQUEST: DivergenceReason
DIVERGENCE_REASON_UNRECORDED_CALL: DivergenceReason
DIVERGENCE_REASON_CHANGED_KIND: DivergenceReason
DIVERGENCE_REASON_EDITED: DivergenceReason
DIVERGENCE_REASON_RERUN: DivergenceReason

class StepRef(_message.Message):
    __slots__ = ("namespace", "superstep", "node", "path", "seq")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    SUPERSTEP_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    superstep: int
    node: str
    path: str
    seq: int
    def __init__(self, namespace: _Optional[str] = ..., superstep: _Optional[int] = ..., node: _Optional[str] = ..., path: _Optional[str] = ..., seq: _Optional[int] = ...) -> None: ...

class Payload(_message.Message):
    __slots__ = ("sha256", "json")
    SHA256_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    sha256: str
    json: bytes
    def __init__(self, sha256: _Optional[str] = ..., json: _Optional[bytes] = ...) -> None: ...

class Error(_message.Message):
    __slots__ = ("type", "message", "args")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    type: str
    message: str
    args: Payload
    def __init__(self, type: _Optional[str] = ..., message: _Optional[str] = ..., args: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...

class RunStarted(_message.Message):
    __slots__ = ("mode", "input", "source_run_id", "graph")
    MODE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    mode: Mode
    input: Payload
    source_run_id: str
    graph: str
    def __init__(self, mode: _Optional[_Union[Mode, str]] = ..., input: _Optional[_Union[Payload, _Mapping]] = ..., source_run_id: _Optional[str] = ..., graph: _Optional[str] = ...) -> None: ...

class RunFinished(_message.Message):
    __slots__ = ("output", "error")
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    output: Payload
    error: Error
    def __init__(self, output: _Optional[_Union[Payload, _Mapping]] = ..., error: _Optional[_Union[Error, _Mapping]] = ...) -> None: ...

class LLMCall(_message.Message):
    __slots__ = ("base_url", "operation", "model", "request", "response", "error", "latency_us", "request_key")
    BASE_URL_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    LATENCY_US_FIELD_NUMBER: _ClassVar[int]
    REQUEST_KEY_FIELD_NUMBER: _ClassVar[int]
    base_url: str
    operation: str
    model: str
    request: Payload
    response: Payload
    error: Error
    latency_us: int
    request_key: str
    def __init__(self, base_url: _Optional[str] = ..., operation: _Optional[str] = ..., model: _Optional[str] = ..., request: _Optional[_Union[Payload, _Mapping]] = ..., response: _Optional[_Union[Payload, _Mapping]] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., latency_us: _Optional[int] = ..., request_key: _Optional[str] = ...) -> None: ...

class ToolCall(_message.Message):
    __slots__ = ("name", "args", "result", "error", "latency_us", "args_key")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    LATENCY_US_FIELD_NUMBER: _ClassVar[int]
    ARGS_KEY_FIELD_NUMBER: _ClassVar[int]
    name: str
    args: Payload
    result: Payload
    error: Error
    latency_us: int
    args_key: str
    def __init__(self, name: _Optional[str] = ..., args: _Optional[_Union[Payload, _Mapping]] = ..., result: _Optional[_Union[Payload, _Mapping]] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., latency_us: _Optional[int] = ..., args_key: _Optional[str] = ...) -> None: ...

class Nondet(_message.Message):
    __slots__ = ("source", "value")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    source: str
    value: Payload
    def __init__(self, source: _Optional[str] = ..., value: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...

class EffectCall(_message.Message):
    __slots__ = ("phase", "name", "args", "args_key", "idempotency_key", "result", "error", "latency_us", "attempts")
    class Phase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PHASE_UNSPECIFIED: _ClassVar[EffectCall.Phase]
        PHASE_PENDING: _ClassVar[EffectCall.Phase]
        PHASE_CONFIRMED: _ClassVar[EffectCall.Phase]
        PHASE_FAILED: _ClassVar[EffectCall.Phase]
    PHASE_UNSPECIFIED: EffectCall.Phase
    PHASE_PENDING: EffectCall.Phase
    PHASE_CONFIRMED: EffectCall.Phase
    PHASE_FAILED: EffectCall.Phase
    PHASE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    ARGS_KEY_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    LATENCY_US_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    phase: EffectCall.Phase
    name: str
    args: Payload
    args_key: str
    idempotency_key: str
    result: Payload
    error: Error
    latency_us: int
    attempts: int
    def __init__(self, phase: _Optional[_Union[EffectCall.Phase, str]] = ..., name: _Optional[str] = ..., args: _Optional[_Union[Payload, _Mapping]] = ..., args_key: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., result: _Optional[_Union[Payload, _Mapping]] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., latency_us: _Optional[int] = ..., attempts: _Optional[int] = ...) -> None: ...

class Diverged(_message.Message):
    __slots__ = ("reason", "kind", "recorded", "actual")
    REASON_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    RECORDED_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    reason: DivergenceReason
    kind: str
    recorded: str
    actual: str
    def __init__(self, reason: _Optional[_Union[DivergenceReason, str]] = ..., kind: _Optional[str] = ..., recorded: _Optional[str] = ..., actual: _Optional[str] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("run_id", "index", "recorded_at", "step", "from_log", "run_started", "run_finished", "llm_call", "tool_call", "nondet", "diverged", "effect")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    FROM_LOG_FIELD_NUMBER: _ClassVar[int]
    RUN_STARTED_FIELD_NUMBER: _ClassVar[int]
    RUN_FINISHED_FIELD_NUMBER: _ClassVar[int]
    LLM_CALL_FIELD_NUMBER: _ClassVar[int]
    TOOL_CALL_FIELD_NUMBER: _ClassVar[int]
    NONDET_FIELD_NUMBER: _ClassVar[int]
    DIVERGED_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    index: int
    recorded_at: _timestamp_pb2.Timestamp
    step: StepRef
    from_log: bool
    run_started: RunStarted
    run_finished: RunFinished
    llm_call: LLMCall
    tool_call: ToolCall
    nondet: Nondet
    diverged: Diverged
    effect: EffectCall
    def __init__(self, run_id: _Optional[str] = ..., index: _Optional[int] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., step: _Optional[_Union[StepRef, _Mapping]] = ..., from_log: _Optional[bool] = ..., run_started: _Optional[_Union[RunStarted, _Mapping]] = ..., run_finished: _Optional[_Union[RunFinished, _Mapping]] = ..., llm_call: _Optional[_Union[LLMCall, _Mapping]] = ..., tool_call: _Optional[_Union[ToolCall, _Mapping]] = ..., nondet: _Optional[_Union[Nondet, _Mapping]] = ..., diverged: _Optional[_Union[Diverged, _Mapping]] = ..., effect: _Optional[_Union[EffectCall, _Mapping]] = ...) -> None: ...
