import pytest
import time

from pipeline import Pipeline

class TestSystem:
    def test_data_propagation(self):
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(lambda: "Hello")
        b = pipeline.create_task(lambda a: f"{a}, World!")

        # Set task dependencies
        pipeline.set_dependency(a, b)

        # Run pipeline
        pipeline.run()

        assert b.get_result() == "Hello, World!"

    def test_synchronization(self):
        pipeline = Pipeline()

        def a():
            return 1
        def b():
            return 10
        def c():
            return 100

        def d(a, b, c):
            return a + b + c

        # Create tasks
        a = pipeline.create_task(a)
        b = pipeline.create_task(b)
        c = pipeline.create_task(c)
        d = pipeline.create_task(d)

        # Set task dependencies
        pipeline.set_dependency(a, d)
        pipeline.set_dependency(b, d)
        pipeline.set_dependency(c, d)

        # Run pipeline
        pipeline.run()

        assert d.get_result() == 111

    def test_parallel_execution_order(self):
        # GIVEN
        def task_a():
            time.sleep(0.1)

        def task_b():
            time.sleep(0.4)

        def task_c():
            time.sleep(0.1)

        def task_d():
            time.sleep(0.1)

        def task_e():
            time.sleep(0.1)

        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(task_a)
        b = pipeline.create_task(task_b)
        c = pipeline.create_task(task_c)
        d = pipeline.create_task(task_d)
        e = pipeline.create_task(task_e)

        # Set task dependencies
        pipeline.set_dependency(a, b)
        pipeline.set_dependency(a, c)
        pipeline.set_dependency(c, d)
        pipeline.set_dependency(d, e)
        pipeline.set_dependency(b, e)

        # Run pipeline
        pipeline.run()

        # manually check order from logs, expected order:
        # Task task_a STARTED
        # Task task_a FINISHED
        # Task task_b STARTED
        # Task task_c STARTED
        # Task task_c FINISHED
        # Task task_d STARTED
        # Task task_d FINISHED
        # Task task_b FINISHED
        # Task task_e STARTED
        # Task task_e FINISHED