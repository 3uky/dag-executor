"""
Test Example

This example processes a random 2D NumPy array and computes statistics before and
after normalization.
Processing Steps:
1. Generate Random Data → Creates a 2D array with random values.
2. Compute Raw Stats → Computes min, max, mean, and std of the original array.
3. Normalize Array → Scales the array to the range [0,1].
4. Compute Normalized Stats → Computes min, max, mean, and std of the normalized array.
5. Merge and Print Stats → Collects and prints the results.

Graph TD;
    A[Generate Random Data] --> B[Compute Raw Stats];
    A --> C[Normalize Array];
    C --> D[Compute Normalized Stats];
    B --> E[Merge and Print Stats];
    D --> E;
"""

import numpy as np

from executor import *

# Tasks definitions
def generate_random_data():
    """Creates a 2D array with random values."""
    random_data = np.random.randint(0, 10, size=(3, 4))
    return random_data

def compute_raw_stats(random_data):
    """Computes min, max, mean, std of the raw array. """
    raw_stats = (np.min(random_data), np.max(random_data), np.mean(random_data), np.std(random_data))
    return raw_stats

def normalize_array(random_data):
    """Scales data to [0,1] using min-max normalization."""
    min_value = np.min(random_data)
    max_value = np.max(random_data)
    normalized_array = (random_data - min_value) / (max_value - min_value)
    return normalized_array

def compute_normalized_stats(normalized_array):
    """Computes min, max, mean, std of the normalized array."""
    normalized_stats = (np.min(normalized_array), np.max(normalized_array), np.mean(normalized_array), np.std(normalized_array))
    return normalized_stats

def merge_and_print_stats(raw_array, normalized_array):
    """Collects both sets of stats, combines them into single dictionary and prints them. """
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


def main():
    # Initialization
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

    # Execute pipeline
    executor.run_pipeline(pipeline)

if __name__ == "__main__":
    main()
