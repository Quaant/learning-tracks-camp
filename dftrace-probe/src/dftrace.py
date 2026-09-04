from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class TaskEvent:
    id: str
    name: str
    pid: int
    start_ns: int
    end_ns: int
    input_bytes_estimate: int
    output_bytes_estimate: int


@dataclass(frozen=True)
class EdgeEvent:
    source: str
    target: str
    transfer: str
    estimated_serialized_bytes: int


@dataclass(frozen=True)
class TaskResult:
    task: TaskEvent
    value: Any
