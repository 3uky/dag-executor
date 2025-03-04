import pytest

import time

from executor import Executor

def mock_task(a, b, delay=0):
    time.sleep(delay)  # Simulate some work with a delay
    return a + b

def test_submit_for_execution():
    executor = Executor()
    executor.submit_for_execution(mock_task, (2, 3))
    assert executor.has_tasks_to_do()

def test_has_tasks_to_do():
    executor = Executor()
    assert not executor.has_tasks_to_do()
    executor.submit_for_execution(mock_task, (2, 3))
    assert executor.has_tasks_to_do()
    executor.execute_submitted_tasks()
    assert not executor.has_tasks_to_do()

def test_parallel_execution():
    executor = Executor()

    # Submit two tasks with a small delay (1s) to simulate parallel execution
    delay = 1
    executor.submit_for_execution(mock_task, (2, 3, delay))  # Task 1 with 1 seconds delay
    executor.submit_for_execution(mock_task, (4, 5, delay))  # Task 2 with 1 seconds delay

    # Execute tasks in parallel
    start_time = time.time()
    while executor.has_tasks_to_do():
        executor.execute_submitted_tasks()  # This will block until some task(s) is completed
    end_time = time.time()

    # Assert that total time taken is around 1 seconds, indicating tasks ran in parallel
    total_time = end_time - start_time
    assert total_time >= 1
    assert total_time < 2