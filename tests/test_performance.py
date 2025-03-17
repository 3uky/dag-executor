import pytest
import time
import numpy as np
from pipeline import Pipeline
import sys

def heavy_cpu_work():
    np.sum(range(20000000))

class TestPerformance:
    def test_pipelined_parallel_cpu_heavy_tasks_should_be_faster_than_sequential_execution(self):
        pipeline = Pipeline()

        # Create tasks - parallel run of cpu heavy work
        a = pipeline.create_task(heavy_cpu_work)
        b = pipeline.create_task(heavy_cpu_work)

        # Run the pipeline in parallel (using ProcessPoolExecutor or similar parallel executor)
        start_time = time.time()
        pipeline.run()
        end_time = time.time()
        total_time_pipeline = end_time - start_time

        # Run the tasks sequentially (without pipeline parallelism)
        start_time = time.time()
        heavy_cpu_work()
        heavy_cpu_work()
        end_time = time.time()
        total_time_sequential = end_time - start_time

        if hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled():
            print(f"\nGil: DISABLED")
        else:
            print(f"\nGil: ENABLED")
        print(f"Total time for parallel execution (pipeline): {total_time_pipeline} s")
        print(f"Total time for sequential execution: {total_time_sequential} s")

        # Expecting that pipelined execution of two longer tasks would be at least 1.6 faster than sequential run
        assert total_time_pipeline < total_time_sequential / 1.6