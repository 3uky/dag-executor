import concurrent.futures

class Executor:
    """Responsible for executing the assigned tasks. It maintains a pool of workers/futures."""
    def __init__(self):
        self.futures = {}
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def submit_for_execution(self, task, inputs=()):
            task.set_state_to_started()
            future = self.executor.submit(task.execute, *inputs)
            self.futures[future] = task

    def remove_finished_futures(self, finished_futures):
        for finished_future in finished_futures:
            del self.futures[finished_future]

    def wait_for_task_finish(self):
        done, _ = concurrent.futures.wait(self.futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED)
        self.set_state_to_finished_for_all_finished_tasks(done)
        self.remove_finished_futures(done)

    def set_state_to_finished_for_all_finished_tasks(self, futures_done):
        for task in [self.futures[future_done] for future_done in futures_done]:
            task.set_state_to_finished()

    def has_tasks_to_do(self):
        return bool(self.futures)