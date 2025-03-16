import pytest
import time

from pipeline import Pipeline

def task_hello():
    return "Hello"

def task_world(a_result):
    return(f"{a_result}, World!")

def task_1():
    return 1

def task_10():
    return 10

def task_100():
    return 100

def task_sum(a, b, c):
    return a + b + c

def task_sleep_100_ms():
    time.sleep(0.1)

def task_sleep_400_ms():
    time.sleep(0.4)

class TestSystem:
    def test_data_propagation(self):
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(task_hello)
        b = pipeline.create_task(task_world)

        # Set task dependencies
        pipeline.set_dependency(a, b)

        # Run pipeline
        pipeline.run()

        assert b.get_result() == "Hello, World!"

    def test_synchronization(self):
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(task_1)
        b = pipeline.create_task(task_10)
        c = pipeline.create_task(task_100)
        d = pipeline.create_task(task_sum)

        # Set task dependencies
        pipeline.set_dependency(a, d)
        pipeline.set_dependency(b, d)
        pipeline.set_dependency(c, d)

        # Run pipeline
        pipeline.run()

        assert d.get_result() == 111

    def test_parallel_execution_order(self):
        # GIVEN
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(task_sleep_100_ms, "a (100 ms)")
        b = pipeline.create_task(task_sleep_400_ms, "b (400 ms)")
        c = pipeline.create_task(task_sleep_100_ms, "c (100 ms)")
        d = pipeline.create_task(task_sleep_100_ms, "d (100 ms)")
        e = pipeline.create_task(task_sleep_100_ms, "e (100 ms)")

        # Set task dependencies
        pipeline.set_dependency(a, b)
        pipeline.set_dependency(a, c)
        pipeline.set_dependency(c, d)
        pipeline.set_dependency(d, e)
        pipeline.set_dependency(b, e)

        # Run pipeline
        pipeline.run()

        # manually check order from logs, expected order:
        # Task a (100 ms) STARTED
        # Task a (100 ms) FINISHED
        # Task task_b (400 ms) STARTED
        # Task task_c (100 ms) STARTED
        # Task task_c (100 ms) FINISHED
        # Task task_d (100 ms) STARTED
        # Task task_d (100 ms) FINISHED
        # Task task_b (400 ms) FINISHED
        # Task task_e (100 ms) STARTED
        # Task task_e (100 ms) FINISHED