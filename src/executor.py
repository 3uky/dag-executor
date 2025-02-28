import concurrent.futures
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class Task:
    def __init__(self, task_id, callable):
        self.task_id = task_id
        self.callable = callable
        self.dependencies = []
        self.started = False  # flag to check if the task is started
        self.executed = False  # flag to check if the task is executed
        self.result = None

    def set_downstream(self, task):
        self.dependencies.append(task)

    def execute(self):
        logger.info(f"{self.task_id} STARTED")
        self.started = True
        self.result = self.callable()  # Run the task's function
        self.executed = True
        logger.info(f"{self.task_id} FINISHED\tresult: {self.result}")

class Executor:
    def __init__(self):
        self.tasks = {}  # Dictionary of task_id -> Task object

    def create_task(self, task_id, callable):
        if task_id in self.tasks:
            raise ValueError(f"Task with id {task_id} already exists.")
        task = Task(task_id, callable)
        self.tasks[task_id] = task
        return task

    def set_dependency(self, task_id, dependency_id):
        if task_id not in self.tasks or dependency_id not in self.tasks:
            raise ValueError("Both task_id and dependency_id must be valid task IDs.")
        task = self.tasks[task_id]
        dependency = self.tasks[dependency_id]
        task.set_downstream(dependency)

    def run_pipeline(self):
        """Run the tasks in the pipeline respecting their dependencies, executing independent tasks in parallel."""

        # list of tasks that can be executed (i.e., tasks with no un-executed dependencies)
        ready_tasks = [task for task in self.tasks.values() if not task.dependencies]

        # execute independent tasks in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}

            # Queue tasks for execution
            for task in ready_tasks:
                if not task.executed:
                    futures[task] = executor.submit(task.execute)

            # Wait for the tasks to finish and check for dependent tasks
            while futures:
                # Wait for any task to finish
                done, _ = concurrent.futures.wait(futures.values(), return_when=concurrent.futures.FIRST_COMPLETED)

                # For each finished task, check if dependent tasks can now be executed
                for task in list(done):
                    # remove the task from the futures dictionary
                    for t in list(futures.keys()):
                        if futures[t] == task:
                            del futures[t]

                    # check which dependent tasks can now run
                    for t in self.tasks.values():
                        if not t.started and all(dep.executed for dep in t.dependencies):
                            futures[t] = executor.submit(t.execute)
