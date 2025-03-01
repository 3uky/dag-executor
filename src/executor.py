import concurrent.futures
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DiGraph:
    def __init__(self):
        self.nodes = {}

    def __str__(self):
        for node, neighbors in self.nodes.items():
            neighbors_str = ", ".join(map(str, neighbors))
            print(f"{node} >> {neighbors_str}")

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes[node] = set()

    def add_edge(self, u, v):
        # Add a directed edge from node u to node v
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

    def output_nodes(self, node):
        if node in self.nodes:
            return self.nodes[node]

    def input_nodes(self, node):
        return {u for u in self.nodes if node in self.nodes[u]}

    def nodes_without_input(self):
        return {node for node in self.nodes if not any(node in self.nodes[u] for u in self.nodes)}

class Task:
    def __init__(self, callable):
        self.id = callable.__name__
        self.callable = callable
        self.started = False
        self.executed = False
        self.result = None

    def __str__(self):
        return f"{self.id}"

    def execute(self):
        logger.info(f"Task {self.id} STARTED")
        self.started = True
        self.result = self.callable()
        self.executed = True
        logger.info(f"Task {self.id} FINISHED")

class Executor:
    """Executor that runs the tasks in the DAG respecting the dependencies."""

    def __init__(self):
        self.graph = DiGraph()
        self.futures = {}
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def create_task(self, callable):
        task = Task(callable)
        self.graph.add_node(task)
        return task

    def set_dependency(self, node_a, node_b):
        self.graph.add_edge(node_a, node_b)

    def __get_tasks(self):
        return self.graph.nodes

    def __are_all_required_inputs_available(self, task):
        return all(input.executed for input in self.graph.input_nodes(task))

    def __remove_tasks_from_execution_queue(self, finished_tasks):
        for task in list(finished_tasks):
            for t in list(self.futures.keys()):
                if self.futures[t] == task:
                    del self.futures[t]

    def __add_ready_to_run_tasks_to_execution_queue(self):
        for task in self.__get_tasks():
            if not task.started and self.__are_all_required_inputs_available(task):
                self.__add_task_to_execution_queue(task)


    def __add_task_to_execution_queue(self, task):
        self.futures[task] = self.executor.submit(task.execute)

    def run_pipeline(self):
        """Run the tasks in the pipeline respecting their dependencies, executing independent tasks in parallel."""

        #self.futures = {} # reinit

        ready_tasks = self.graph.nodes_without_input()
        for task in ready_tasks:
            if not task.executed:
                self.__add_task_to_execution_queue(task)

        # Wait for the tasks to finish and check for dependent tasks
        while self.futures:
            # Wait for any task to finish
            finished_tasks, _ = concurrent.futures.wait(self.futures.values(), return_when=concurrent.futures.FIRST_COMPLETED)

            self.__remove_tasks_from_execution_queue(finished_tasks)
            self.__add_ready_to_run_tasks_to_execution_queue()


# Example Tasks
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

if __name__ == '__main__':
    executor = Executor()

    # Create tasks
    a = executor.create_task(task_a)
    b = executor.create_task(task_b)
    c = executor.create_task(task_c)
    d = executor.create_task(task_d)
    e = executor.create_task(task_e)

    # Set task dependencies
    executor.set_dependency(a, b)  # task_b depends on task_a
    executor.set_dependency(a, c)  # task_c depends on task_a
    executor.set_dependency(c, d)  # task_d depends on task_c
    executor.set_dependency(d, e)  # task_e depends on task_d
    executor.set_dependency(b, e)  # task_e depends on task_b

    # Run the pipeline
    executor.run_pipeline()
