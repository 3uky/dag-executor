import concurrent.futures
import logging
import time
from enum import Enum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DiGraph:
    def __init__(self):
        self.nodes = {}

    def __str__(self):
        for node, neighbors in self.nodes.items():
            neighbors_str = ", ".join(map(str, neighbors))
            print(f"{node} -> {neighbors_str}")

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes[node] = set()

    def add_edge(self, u, v):
        if u not in self.nodes:
            self.add_node(u)
        if v not in self.nodes:
            self.add_node(v)

        self.nodes[u].add(v)

    def remove_edge(self, u, v):
        if u in self.nodes and v in self.nodes[u]:
            self.nodes[u].remove(v)

    def remove_node(self, node):
        # Remove a node and all edges associated with it
        if node in self.nodes:
            del self.nodes[node]  # Remove the node from the adjacency list
        # Also, remove the node from all other nodes' adjacency lists
        for u in list(self.nodes):
            if node in self.nodes[u]:
                self.nodes[u].remove(node)

    def get_output_nodes(self, node):
        if node in self.nodes:
            return self.nodes[node]

    def get_input_nodes(self, node):
        return [u for u in self.nodes if node in self.nodes[u]]

    def get_nodes_without_input_edge(self):
        return [node for node in self.nodes if not any(node in self.nodes[u] for u in self.nodes)]

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

    def submit_for_execution(self, tasks):
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
        self.submit_for_execution(pipeline.get_initial_tasks())

        while self.is_there_tasks_to_run():
            finished_task = self.execute_submitted_tasks()
            self.remove_from_execution(finished_task)
            self.submit_for_execution(pipeline.get_ready_to_run_tasks())


# Example Tasks
def task_a(*args, **kwargs):
    time.sleep(1)
    return "Data from Task A"

def task_b(*args, **kwargs):
    time.sleep(4)
    return "Data from Task B"

def task_c(*args, **kwargs):
    time.sleep(1)
    return "Data from Task C"

def task_d(*args, **kwargs):
    time.sleep(1)
    return "Data from Task D"

def task_e(*args, **kwargs):
    time.sleep(1)
    return "Data from Task E"

if __name__ == '__main__':
    pipeline = Pipeline()
    executor = Executor()

    # Create tasks
    a = pipeline.create_task(task_a)
    b = pipeline.create_task(task_b)
    c = pipeline.create_task(task_c)
    d = pipeline.create_task(task_d)
    e = pipeline.create_task(task_e)

    # Set task dependencies
    pipeline.set_dependency(a, b)  # task_b depends on task_a
    pipeline.set_dependency(a, c)  # task_c depends on task_a
    pipeline.set_dependency(c, d)  # task_d depends on task_c
    pipeline.set_dependency(d, e)  # task_e depends on task_d
    pipeline.set_dependency(b, e)  # task_e depends on task_b

    # Run the pipeline
    executor.run_pipeline(pipeline)
