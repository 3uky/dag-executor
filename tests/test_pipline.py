import pytest
from unittest.mock import MagicMock

from pipeline import Pipeline

def example_callable(x):
    return x * 2

class TestPipeline:
    @pytest.fixture
    def pipeline(self):
        return Pipeline()

    @pytest.fixture
    def task(self, pipeline):
        return pipeline.create_task(example_callable)

    @pytest.fixture
    def tasks_with_dependencies(self, pipeline):
        task_a = pipeline.create_task(example_callable)
        task_b = pipeline.create_task(example_callable)
        pipeline.set_dependency(task_a, task_b)
        return task_a, task_b

    @pytest.fixture
    def tasks_with_cycle(self, pipeline):
        task_a = pipeline.create_task(example_callable)
        task_b = pipeline.create_task(example_callable)
        pipeline.set_dependency(task_a, task_b)
        pipeline.set_dependency(task_b, task_a)  # Create a cycle
        return task_a, task_b

    def test_create_task(self, pipeline, task):
        assert task.is_pending() is True

    def test_get_tasks(self, pipeline, task):
        tasks = pipeline.get_tasks()
        assert len(tasks) == 1
        assert task in tasks

    def test_get_ready_to_run_tasks_no_dependencies(self, pipeline, task):
        ready_tasks = pipeline.get_ready_to_run_tasks()
        assert len(ready_tasks) == 1
        assert task in ready_tasks

    def test_get_ready_to_run_tasks_with_dependencies(self, pipeline, tasks_with_dependencies):
        task_a, task_b = tasks_with_dependencies
        task_a.set_state_to_finished()  # Simulate task_a being finished
        ready_tasks = pipeline.get_ready_to_run_tasks()
        assert len(ready_tasks) == 1
        assert task_b in ready_tasks

    def test_initial_check_raises_error_on_cyclic_graph(self, pipeline, tasks_with_cycle):
        with pytest.raises(ValueError, match="The graph has cycles. Cannot execute pipeline."):
            pipeline.initial_check()

    def test_get_task_inputs_from_multiple_dependencies(self, pipeline):
        task_a = pipeline.create_task(example_callable)
        task_b = pipeline.create_task(example_callable)
        task_c = pipeline.create_task(example_callable)

        # Set up dependencies: task_b and task_c depend on task_a
        pipeline.set_dependency(task_a, task_b)
        pipeline.set_dependency(task_a, task_c)

        # Simulate task_a having a result
        task_a.set_state_to_finished()
        task_a.result = 10

        # Task_b and task_c depend on task_a, so get_task_inputs should return task_a's result for both
        task_inputs_b = pipeline.get_task_inputs(task_b)
        task_inputs_c = pipeline.get_task_inputs(task_c)

        assert task_inputs_b == [task_a.result]
        assert task_inputs_c == [task_a.result]
