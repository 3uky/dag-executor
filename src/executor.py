import concurrent.futures
import logging
import time
from enum import Enum

from digraph import DiGraph

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class TaskState(Enum):
    NOT_STARTED = 0
    STARTED = 1
    FINISHED = 2

class Task:
    def __init__(self, callable):
        self.id = callable.__name__
        self.callable = callable
        self.state = TaskState.NOT_STARTED
        self.result = None

    def __str__(self):
        return f"{self.id}"

    def is_not_started(self):
        return self.state == TaskState.NOT_STARTED

    def is_running(self):
        return self.state == TaskState.STARTED

    def is_finished(self):
        return self.state == TaskState.FINISHED

    def execute(self, inputs=None):
        logger.info(f"Task {self.id} STARTED\tinputs: {inputs}")
        self.state = TaskState.STARTED
        self.result = self.callable(inputs)
        self.state = TaskState.FINISHED
        logger.info(f"Task {self.id} FINISHED")

class Pipeline:
    def __init__(self):
        self.graph = DiGraph()

    def create_task(self, callable):
        task = Task(callable)
        self.graph.add_node(task)
        return task

    def set_dependency(self, node_a, node_b):
        self.graph.add_edge(node_a, node_b)

    def get_dependencies(self, node):
        return self.graph.get_input_nodes(node)

    def get_initial_tasks(self):
        return [task for task in self.get_tasks_without_dependencies() if task.is_not_started()]

    def get_ready_to_run_tasks(self):
        return [task for task in self.get_tasks() if task.is_not_started() and self.are_all_required_inputs_available(task)]

    def get_tasks(self):
        return self.graph.nodes

    def get_tasks_without_dependencies(self):
        return self.graph.get_nodes_without_input_edge()

    def get_required_inputs(self, task):
        return [dependent_task.result for dependent_task in self.get_dependencies(task)]

    def are_all_required_inputs_available(self, task):
        return all(dependent_task.is_finished() for dependent_task in self.get_dependencies(task))

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
