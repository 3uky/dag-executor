import pytest
import time
import numpy as np

from executor import *

class TestManual:
    def skip_test_parallel_execution_order_of_entry_task(self):
        # GIVEN
        def task_a(*args, **kwargs):
            time.sleep(1)
            return "Data from Task A"

        def task_b(*args, **kwargs):
            time.sleep(4)
            return "Data from Task B"

        def task_c(*args, **kwargs):
            time.sleep(1)
            return "Data from Task C"

        def task_d(*args, **kwargs):
            time.sleep(1)
            return "Data from Task D"

        def task_e(*args, **kwargs):
            time.sleep(1)
            return "Data from Task E"

        executor = Executor()
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(task_a)
        b = pipeline.create_task(task_b)
        c = pipeline.create_task(task_c)
        d = pipeline.create_task(task_d)
        e = pipeline.create_task(task_e)

        # Set task dependencies
        pipeline.set_dependency(a, b)  # task_b depends on task_a
        pipeline.set_dependency(a, c)  # task_c depends on task_a
        pipeline.set_dependency(c, d)  # task_d depends on task_c
        pipeline.set_dependency(d, e)  # task_e depends on task_d
        pipeline.set_dependency(b, e)  # task_e depends on task_b

        # WHEN
        executor.run_pipeline(pipeline)

        # THEN - check log output

    def test_result_propagation_of_entry_task(self):
        # GIVEN
        def generate_random_data(*args, **kwargs):
            random_data = np.random.randint(0, 10, size=(3, 4))

            return random_data

        def compute_raw_stats(*args, **kwargs):
            random_data = args[0][0]
            raw_stats = (np.min(random_data), np.max(random_data), np.mean(random_data), np.std(random_data))
            return raw_stats

        def normalize_array(*args, **kwargs):
            random_data = args[0][0]
            min_value = np.min(random_data)
            max_value = np.max(random_data)
            normalized_array = (random_data - min_value) / (max_value - min_value)
            return normalized_array

        def compute_normalized_stats(*args, **kwargs):
            normalized_array = args[0][0]
            normalized_stats = (np.min(normalized_array), np.max(normalized_array), np.mean(normalized_array), np.std(normalized_array))
            return normalized_stats

        def merge_and_print_stats(*args, **kwargs):
            raw_min, raw_max, raw_mean, raw_std = args[0][0]
            norm_min, norm_max, norm_mean, norm_std = args[0][1]

            stats = {
                'raw min': raw_min,
                'raw max': raw_max,
                'raw mean': raw_mean,
                'raw std': raw_std,
                'normalized min': norm_min,
                'normalized max': norm_max,
                'normalized mean': norm_mean,
                'normalized std': norm_std
            }
            print(f"\nStats:\n{stats}")

        executor = Executor()
        pipeline = Pipeline()

        # Create tasks
        A = pipeline.create_task(generate_random_data)
        B = pipeline.create_task(compute_raw_stats)
        C = pipeline.create_task(normalize_array)
        D = pipeline.create_task(compute_normalized_stats)
        E = pipeline.create_task(merge_and_print_stats)

        # Set task dependencies
        pipeline.set_dependency(A, B)
        pipeline.set_dependency(A, C)
        pipeline.set_dependency(C, D)
        pipeline.set_dependency(D, E)
        pipeline.set_dependency(B, E)

        # WHEN
        executor.run_pipeline(pipeline)

        # THEN - check log output