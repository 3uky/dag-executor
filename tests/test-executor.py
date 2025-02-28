import pytest
import time

from src.executor import *

class TestExecutor:
    def test_execute_tasks_in_expected_order(self):
        # GIVEN
        def task_a():
            time.sleep(1)
            return "Data from Task A"

        def task_b():
            time.sleep(4)
            return "Data from Task B"

        def task_c():
            time.sleep(1)
            return "Data from Task C"

        def task_d():
            time.sleep(1)
            return "Data from Task D"

        def task_e():
            time.sleep(1)
            return "Data from Task E"

        executor = Executor()

        # create tasks
        task_a_instance = executor.create_task('task_a', task_a)
        task_b_instance = executor.create_task('task_b', task_b)
        task_c_instance = executor.create_task('task_c', task_c)
        task_d_instance = executor.create_task('task_d', task_d)
        task_e_instance = executor.create_task('task_e', task_e)

        # set dependencies
        executor.set_dependency('task_b', 'task_a')  # task_b depends on task_a
        executor.set_dependency('task_c', 'task_a')  # task_c depends on task_a
        executor.set_dependency('task_d', 'task_c')  # task_d depends on task_b
        executor.set_dependency('task_e', 'task_d')  # task_e depends on task_b
        executor.set_dependency('task_e', 'task_b')  # task_e depends on task_d

        # WHEN
        # run the pipeline (task_a -> task_b -> task_d ->, task_b -> task_e)
        executor.run_pipeline()

        # THEN
