import pytest
import numpy as np

from pipeline import Pipeline

class TestEntryTask:
    @pytest.fixture
    def data(self): # generate random data as input for first task
        return np.random.randint(0, 10, size=(3, 4))

    def test_entry_task(self, data):
        # GIVEN
        def generate_random_data():
            random_data = data
            return random_data

        def compute_raw_stats(random_data): # this method could be reused also for computing normalized stats
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
                'raw min': raw_min, 'raw max': raw_max, 'raw mean': raw_mean, 'raw std': raw_std,'normalized min': norm_min,
                'normalized max': norm_max, 'normalized mean': norm_mean, 'normalized std': norm_std
            }

            print(f"\nStats:")
            for key, value in stats.items():
                print(f"{key}: {value}")

            return stats

        def calculate_expected_result(input_array):
            raw_min, raw_max, raw_mean, raw_std = compute_raw_stats(input_array)
            normalized_array = normalize_array(input_array)
            norm_min, norm_max, norm_mean, norm_std = compute_normalized_stats(normalized_array)
            stats = {
                'raw min': raw_min, 'raw max': raw_max, 'raw mean': raw_mean,'raw std': raw_std,
                'normalized min': norm_min, 'normalized max': norm_max, 'normalized mean': norm_mean, 'normalized std': norm_std
            }
            return stats

        pipeline = Pipeline()

        # Create tasks
        a = pipeline.create_task(generate_random_data)
        b = pipeline.create_task(compute_raw_stats)
        c = pipeline.create_task(normalize_array)
        d = pipeline.create_task(compute_normalized_stats)
        e = pipeline.create_task(merge_and_print_stats)

        # Set task dependencies
        pipeline.set_dependency(a, b)
        pipeline.set_dependency(a, c)
        pipeline.set_dependency(c, d)
        pipeline.set_dependency(d, e)
        pipeline.set_dependency(b, e)

        # Run pipeline
        pipeline.run()

        pipeline_result = e.get_result()
        expected_result = calculate_expected_result(data)
        assert pipeline_result == expected_result