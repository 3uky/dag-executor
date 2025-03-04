import pytest

from pipeline import Pipeline

def sample_task(x):
    return x * 2

class TestPipeline:
    def test_get_read_to_run_tasks(self):
        pipeline = Pipeline()
        a = pipeline.create_task(sample_task)
        b = pipeline.create_task(sample_task)
        pipeline.set_dependency(a, b)

        assert a in pipeline.get_ready_to_run_tasks()
        assert b not in pipeline.get_ready_to_run_tasks()