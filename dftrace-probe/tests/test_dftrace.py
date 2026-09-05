import os
import pickle
import pytest

from src.dftrace import estimate_size, execute_task, TaskEvent, TaskResult


def test_estimate_size():
    data = [1, 2, 3, 4, 5]
    size = estimate_size(data)
    
    assert size > 0
    
    assert estimate_size(data) == size
    
    assert estimate_size([1]) != estimate_size([1, 2, 3])


def test_execute_task_in_current_process():
    def double(x):
        return x * 2
    
    result = execute_task("double", double, 5)
    
    assert isinstance(result, TaskResult)
    assert isinstance(result.task, TaskEvent)
    assert result.value == 10
    
    assert result.task.name == "double"
    assert result.task.pid == os.getpid()  # выполняется в текущем процессе
    assert result.task.start_ns > 0
    assert result.task.end_ns >= result.task.start_ns
    assert result.task.input_bytes_estimate > 0
    assert result.task.output_bytes_estimate > 0


def test_execute_task_with_complex_data():
    def process(data):
        return {"sum": sum(data), "len": len(data)}
    
    data = list(range(100))
    result = execute_task("process", process, data)
    
    assert result.value["sum"] == sum(range(100))
    assert result.value["len"] == 100


def test_uuid_uniqueness():
    def identity(x):
        return x
    
    r1 = execute_task("test", identity, 1)
    r2 = execute_task("test", identity, 2)
    
    assert r1.task.id != r2.task.id
    assert r1.task.id.startswith("test-")
    assert r2.task.id.startswith("test-")