import concurrent.futures

from pipeline import Pipeline

class Executor:
    """Executor that runs the tasks in the DAG respecting the dependencies."""
    def __init__(self):
        self.futures = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def submit_for_execution(self, task, inputs=()):
            future = self.executor.submit(task.execute, *inputs)
            self.futures.add(future)

    def remove_from_execution(self, finished_tasks):
        for finished_task in finished_tasks:
            self.futures.remove(finished_task)

    def execute_submitted_tasks(self):
        done, _ = concurrent.futures.wait(self.futures, return_when=concurrent.futures.FIRST_COMPLETED)
        return done

    def is_there_tasks_to_run(self):
        return bool(self.futures)

    def run_pipeline(self, pipeline):
        """Run the tasks in the pipeline respecting their dependencies, executing independent tasks in parallel."""
        for task in pipeline.get_initial_tasks():
            self.submit_for_execution(task)

        while self.is_there_tasks_to_run():
            finished_tasks = self.execute_submitted_tasks()
            self.remove_from_execution(finished_tasks)
            for task in pipeline.get_ready_to_run_tasks():
                self.submit_for_execution(task, pipeline.get_required_inputs(task))
