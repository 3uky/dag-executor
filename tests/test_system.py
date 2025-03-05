import pytest
import time
import numpy as np

from pipeline import Pipeline

class TestSystem:
    def test_simple_task_dependency_without_inputs(self):
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(lambda: None)
        b = pipeline.create_task(lambda: None)

        # Set task dependencies
        pipeline.set_dependency(a, b)

        # Run pipeline
        pipeline.run()


    def test_simple_task_with_data_propagation(self):
        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(lambda: "Hello")
        b = pipeline.create_task(lambda a: print(f"{a}, World!"))

        # Set task dependencies
        pipeline.set_dependency(a, b)

        # Run pipeline
        pipeline.run()

    def test_simple_task_synchronization(self):
        pipeline = Pipeline()

        def a():
            return "Hello"
        def b():
            return ", "
        def c():
            return "World!"

        def d(a, b, c):
            print(f"{a}{b}{c}")

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

    def test_parallel_execution_order(self):
        # GIVEN
        def task_a():
            time.sleep(1)

        def task_b():
            time.sleep(4)

        def task_c():
            time.sleep(1)

        def task_d():
            time.sleep(1)

        def task_e():
            time.sleep(1)

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

        # Check log output

    def test_result_propagation_of_entry_task(self):
        # GIVEN
        def generate_random_data():
            random_data = np.random.randint(0, 10, size=(3, 4))
            return random_data

        def compute_raw_stats(random_data):
            raw_stats = (np.min(random_data), np.max(random_data), np.mean(random_data), np.std(random_data))
            return raw_stats

        def normalize_array(random_data):
            min_value = np.min(random_data)
            max_value = np.max(random_data)
            normalized_array = (random_data - min_value) / (max_value - min_value)
            return normalized_array

        def compute_normalized_stats(normalized_array):
            normalized_stats = (np.min(normalized_array), np.max(normalized_array), np.mean(normalized_array), np.std(normalized_array))
            return normalized_stats

        def merge_and_print_stats(raw_array, normalized_array):
            raw_min, raw_max, raw_mean, raw_std = raw_array
            norm_min, norm_max, norm_mean, norm_std = normalized_array

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

            for key, value in stats.items():
                print(f"{key}: {value}")

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

        # Run pipeline
        pipeline.run()

        # Check log output