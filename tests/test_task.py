import pytest
import threading
import time

from task import Task, TaskState


def sample_task(x):
    return x * 2

def sample_task_with_delay(x):
    time.sleep(1)
    return x * 2

class TestTask:
    def test_task_state_transitions(self):
        # initially, the task is in NOT_STARTED state
        task = Task(sample_task)
        assert task.is_not_started()
        assert not task.is_running()
        assert not task.is_finished()

        # after calling execute, the state should change to STARTED and then FINISHED
        task.execute(5)

        assert not task.is_not_started()
        assert not task.is_running()
        assert task.is_finished()

    def test_task_execution_result(self):
        task = Task(sample_task)
        task.execute(5)
        assert task.result == 10  # 5 * 2 = 10

    def test_task_is_not_started_after_execution(self):
        task = Task(sample_task)
        task.execute(5)
        assert not task.is_not_started()

    def test_task_is_finished_after_execution(self):
        task = Task(sample_task)
        task.execute(5)
        assert task.is_finished()

    def test_task_is_running_during_execution(self):
        # run the task in a separate thread
        task = Task(sample_task_with_delay)
        task_thread = threading.Thread(target=task.execute, args=(5,))
        task_thread.start()

        # while the task is running, we check if task is in running state
        assert task.is_running()

        # wait for the task to finish
        task_thread.join()

        # after task execution is finished, the state should be `FINISHED`
        assert task.is_finished()
        assert not task.is_running()