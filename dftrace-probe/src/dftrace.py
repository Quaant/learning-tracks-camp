from dataclasses import asdict, dataclass
from typing import Any, Callable
import os
import pickle
import time
import uuid
from collections.abc import Callable
import multiprocessing as mp 
import json 
from concurrent.futures import ProcessPoolExecutor 
import tempfile 
from pathlib import Path 

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

class TraceSession: 
    def __init__(self, workers: int = 2) -> None: 
        context = mp.get_context("spawn")
        self._executor = ProcessPoolExecutor( max_workers = workers, mp_context=context,)
        self.nodes: list[TaskEvent] = [] 
        self.edges: list[EdgeEvent] = [] 
        self._closed = False 

    def __enter__(self) -> "TraceSession":  
        return self
     
    def __exit__(self, exc_type, exc_value, traceback) -> None: 
        self.close()

    def close(self) -> None: 
        if not self._closed: 
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._closed = True

    def _ensure_open(self) -> None: 
        if self._closed:
            raise RuntimeError("trace session is closed")

    def _record(self, result: TaskResult, inputs: list[tuple[str, Any]]) -> tuple[str, Any]: 
        self.nodes.append(result.task)

        for source_id, value in inputs: 
            self.edges.append(EdgeEvent(source=source_id, target=result.task.id, transfer="pickle", estimated_serialized_bytes=estimate_size(value)))

        return result.task.id, result.value 

    def run(self, name: str, function: Callable[[Any], Any], value: Any, source_id: str | None = None) -> tuple[str, Any]: 
        result = self._executor.submit(execute_task, name, function, value).result() 
        inputs = [] if source_id is None else [(source_id, value)]
        return self._record(result, inputs)

    def run_paralell(self, specs:list[tuple[str, Callable[[Any], Any]]], value: Any, source_id: str) -> list[tuple[str, Any]]: 
        self._ensure_open() 

        futures = [self._executor.submit(execute_task, name, function, value) for name, function in specs]

        results = []

        for i in futures: 
            result = i.result() 
            task_id, value_result = self._record(result, [(source_id, value)])
            results.append((task_id, value_result))

        return results 

    def run_join(self, name:str, function: Callable[[Any], Any], inputs: list[tuple[str, Any]]) -> tuple[str, Any]: 
        self._ensure_open() 

        joined_value = tuple(value for _, value in inputs)
        
        result = self._executor.submit(execute_task, name, function, joined_value).result()
        
        return self._record(result, inputs)

    def as_json(self) -> dict[str, Any]: 
         return {
            "schema_version": 1,
            "clock": "perf_counter_ns",
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
            "limitations": [
                "serialized sizes are estimates, not measured OS traffic"
            ],
        }
    def save_json(self, path: str | Path) -> None: 
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            delete=False,
        ) as temporary:
            json.dump(self.as_json(), temporary, indent=2)
            temporary.write("\n")
            temporary_name = temporary.name
        
        os.replace(temporary_name, destination)

    def to_dot(self) -> str: 
        def escape(value: str) -> str:
            """Экранирует спецсимволы для DOT"""
            return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        
        lines = ["digraph dataflow {"]
        
        # Добавляем все узлы
        for node in self.nodes:
            duration_ms = (node.end_ns - node.start_ns) / 1_000_000
            label = escape(f"{node.name}\n{duration_ms:.3f} ms\npid={node.pid}")
            lines.append(f'  "{escape(node.id)}" [label="{label}"];')
        
        # Добавляем все рёбра
        for edge in self.edges:
            label = escape(
                f"{edge.estimated_serialized_bytes} B\n{edge.transfer}"
            )
            lines.append(
                f'  "{escape(edge.source)}" -> "{escape(edge.target)}" '
                f'[label="{label}"];'
            )
        
        lines.append("}")
        return "\n".join(lines) + "\n"