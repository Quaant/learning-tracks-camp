(.venv) quaant@quaant-Aspire-A315-23:~/source/repos/learning-tracks-camp/dftrace-probe$ python -m src.demo_minimal
Родительский PID: 106493

Результат выполнения:
  Task: TaskEvent(id='square-aefdad1d', name='square', pid=106495, start_ns=35590020510699, end_ns=35590020512399, input_bytes_estimate=22, output_bytes_estimate=22)
  Значение: [1, 4, 9]

Все проверки пройдены!
   PID дочернего процесса: 106495
   Время выполнения: 0.002 мс
   Входные данные: 22 байт (сериализованные)
   Выходные данные: 22 байт (сериализованные) 

   (.venv) quaant@quaant-Aspire-A315-23:~/source/repos/learning-tracks-camp/dftrace-probe$ python -m pytest tests/test_dftrace.py -v
========================================================================================= test session starts =========================================================================================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0 -- /home/quaant/source/repos/learning-tracks-camp/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/quaant/source/repos/learning-tracks-camp/dftrace-probe
collected 4 items                                                                                                                                                                                     

tests/test_dftrace.py::test_estimate_size PASSED                                                                                                                                                [ 25%]
tests/test_dftrace.py::test_execute_task_in_current_process PASSED                                                                                                                              [ 50%]
tests/test_dftrace.py::test_execute_task_with_complex_data PASSED                                                                                                                               [ 75%]
tests/test_dftrace.py::test_uuid_uniqueness PASSED                                                                                                                                              [100%]
