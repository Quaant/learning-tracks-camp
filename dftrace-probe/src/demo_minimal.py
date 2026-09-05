import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor

from src.dftrace import execute_task


def square(values: list[int]) -> list[int]:
    return [value * value for value in values]


def main() -> None:
    print(f"Родительский PID: {os.getpid()}")
    print()
    
    context = mp.get_context("spawn")
    
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
        # отправляем задачу в дочерний процесс
        future = executor.submit(
            execute_task,   # наша обёртка
            "square",       # имя задачи
            square,         # функция
            [1, 2, 3]       # входные данные
        )
        
        result = future.result()
    
    print("Результат выполнения:")
    print(f"  Task: {result.task}")
    print(f"  Значение: {result.value}")
    print()
    
    assert result.task.pid != os.getpid(), "Ошибка: задача выполнилась в родительском процессе!"
    assert result.value == [1, 4, 9], "Ошибка: неверный результат!"
    
    print("Все проверки пройдены!")
    print(f"   PID дочернего процесса: {result.task.pid}")
    print(f"   Время выполнения: {(result.task.end_ns - result.task.start_ns) / 1_000_000:.3f} мс")
    print(f"   Входные данные: {result.task.input_bytes_estimate} байт (сериализованные)")
    print(f"   Выходные данные: {result.task.output_bytes_estimate} байт (сериализованные)")


if __name__ == "__main__":
    main()