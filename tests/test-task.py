import pytest

from task import *

def callable_example(*args):
    pass

class TestTask:
    def test_task_state(self):
        # GIVEN
        t = Task(callable_example)

        assert(t.is_running(), False)
        assert(t.is_finished(), False)

    def test_task_in_finished_state_after_execution_finish(self):
        # GIVEN
        t = Task(callable_example)
        t.execute(None)

        assert(t.is_running(), False)
        assert(t.is_finished(), True)

