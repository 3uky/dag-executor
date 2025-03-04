import concurrent.futures

class Executor:
    """Responsible for executing the assigned tasks. It maintains a pool of workers/futures."""
    def __init__(self):
        self.futures = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def submit_for_execution(self, executable, inputs=()):
            future = self.executor.submit(executable, *inputs)
            self.futures.add(future)

    def remove_from_execution(self, finished_tasks):
        for finished_task in finished_tasks:
            self.futures.remove(finished_task)

    def execute_submitted_tasks(self):
        done, _ = concurrent.futures.wait(self.futures, return_when=concurrent.futures.FIRST_COMPLETED)
        self.remove_from_execution(done)

    def has_tasks_to_do(self):
        return bool(self.futures)
