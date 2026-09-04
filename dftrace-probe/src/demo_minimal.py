import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor

from src.dftrace import execute_task


def square(values: list[int]) -> list[int]:
    return [value * value for value in values]


def main() -> None:
    context = mp.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
        result = executor.submit(
            execute_task, "square", square, [1, 2, 3]
        ).result()
    print(result.task)
    print(result.value)
    assert result.task.pid != os.getpid()
    assert result.value == [1, 4, 9]


if __name__ == "__main__":
    main()
