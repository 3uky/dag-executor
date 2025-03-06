import concurrent.futures

class Executor:
    """Responsible for executing the assigned tasks. It maintains a pool of workers/futures."""
    def __init__(self):
        self.futures = {}
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def submit_for_execution(self, task, inputs=()):
            task.set_state_to_started()
            future = self.executor.submit(task.callable, *inputs)
            self.futures[future] = task

    def wait_for_task_finish(self):
        done, _ = concurrent.futures.wait(self.futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED)

        for future in done:
            task = self.futures[future]
            task.result = future.result()
            task.set_state_to_finished()
            del self.futures[future]