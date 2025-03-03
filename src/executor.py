import concurrent.futures

from pipeline import *

class Executor:
    """Executor that runs the tasks in the DAG respecting the dependencies."""
    def __init__(self):
        self.futures = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def submit_for_execution(self, tasks, pipeline):
        for task in tasks:
            future = self.executor.submit(task.execute, pipeline.get_required_inputs(task))
            self.futures.add(future)

    def remove_from_execution(self, finished_task):
        if finished_task in self.futures:
            self.futures.remove(finished_task)

    def execute_submitted_tasks(self):
        done, _ = concurrent.futures.wait(self.futures, return_when=concurrent.futures.FIRST_COMPLETED)
        return next(iter(done)) # wait return set containing exactly one element with FIRST_COMPLETED option

    def is_there_tasks_to_run(self):
        return bool(self.futures)

    def run_pipeline(self, pipeline):
        """Run the tasks in the pipeline respecting their dependencies, executing independent tasks in parallel."""
        self.submit_for_execution(pipeline.get_initial_tasks(), pipeline)

        while self.is_there_tasks_to_run():
            finished_task = self.execute_submitted_tasks()
            self.remove_from_execution(finished_task)
            self.submit_for_execution(pipeline.get_ready_to_run_tasks(), pipeline)
