import concurrent.futures

class Executor:
    """Responsible for executing the assigned tasks. It maintains a pool of workers/futures."""
    def __init__(self):
        self.futures = {}
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def submit_for_execution(self, task, inputs=()):
            task.set_submitted()
            future = self.executor.submit(task.execute, *inputs)
            self.futures[future] = task

    def remove_from_execution(self, finished_futures):
        for finished_future in finished_futures:
            del self.futures[finished_future]

    def execute_submitted_tasks(self):
        self.set_state_to_started_for_all_submitted_tasks()
        done = self.execute_and_wait_until_something_finish()
        self.set_state_to_finished_for_all_finished_tasks(done)
        self.remove_from_execution(done)

    def set_state_to_started_for_all_submitted_tasks(self):
        for task in self.futures.values():
            task.set_started()

    def set_state_to_finished_for_all_finished_tasks(self, futures_done):
        for task in [self.futures[future_done] for future_done in futures_done]:
            task.set_finished()

    def execute_and_wait_until_something_finish(self):
        done, _ = concurrent.futures.wait(self.futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED)
        return done

    def has_tasks_to_do(self):
        return bool(self.futures)