from dataclasses import asdict, dataclass
from typing import Any
import os
import pickle
import time
import uuid
from collections.abc import Callable

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


def estimate_size(value: Any) -> int:
    #размер сериализованного объекта
    return len(pickle.dumps(value))

def execute_task(name: str, function: Callable[[Any], Any], value: Any) -> TaskResult:
    
    task_id = f"{name}-{uuid.uuid4().hex[:8]}"
    input_size = estimate_size(value)
    start = time.perf_counter_ns()
    output = function(value)
    end = time.perf_counter_ns()
    
    event = TaskEvent(
        id=task_id,
        name=name,
        pid=os.getpid(),
        start_ns=start,
        end_ns=end,
        input_bytes_estimate=input_size,
        output_bytes_estimate=estimate_size(output),
    )
    
    return TaskResult(event, output)
