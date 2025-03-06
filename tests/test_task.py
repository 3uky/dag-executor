import pytest
import time

from task import Task

def example_callable(x):
    return x * 2

class TestTask:
    def test_initial_state(self):
        # Test if the task starts in the PENDING state
        task = Task(example_callable)
        assert task.is_pending() is True
        assert task.is_started() is False
        assert task.is_finished() is False

    def test_set_started(self):
        # Test if the task can be set to the STARTED state
        task = Task(example_callable)
        task.set_state_to_started()
        assert task.is_started() is True

    def test_set_finished(self):
        # Test if the task can be set to the FINISHED state
        task = Task(example_callable)
        task.set_state_to_finished()
        assert task.is_finished() is True

    def test_execute_with_no_arguments(self):
        # Test that executing without arguments (if no arguments are expected)
        def no_arg_callable():
            return "Hello, World!"

        task = Task(no_arg_callable)
        result = task.callable()
        assert result == "Hello, World!"

    def test_execute_with_arguments(self):
        def arg_callable(a, b):
            return a + b

        a = 5
        b = 10
        expected_result = a + b

        task = Task(arg_callable)
        result = task.callable(a, b)

        assert result == expected_result

    def skip_test_execute(self):
        def example_callable_with_delay():
            time.sleep(0.1)

        task = Task(example_callable_with_delay)

        start_time = time.time()
        task.callable()
        end_time = time.time()

        total_time = end_time - start_time
        assert total_time >= 0.1
