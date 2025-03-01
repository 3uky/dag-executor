import concurrent.futures
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class Task:
    def __init__(self, callable):
        self.callable = callable
        self.name = callable.__name__
        self.started = False
        self.executed = False
        self.result = None

    def execute(self):
        logger.info(f"Task STARTED {self.name}")
        self.started = True
        self.result = self.callable()
        self.executed = True
        logger.info(f"Task FINISHED\tresult: {self.name}")

class Node:
    def __init__(self, value):
        self.value = value
        self.dependencies = []  # Nodes this node depends on
        self.dependents = []    # Nodes that depend on this node

    def __repr__(self):
        return f"Node({self.value})"

class Graph:
    def __init__(self):
        self.nodes = []

    def create_node(self, value):
        """Creates a new node and adds it to the graph."""
        node = Node(value)
        self.nodes.append(node)
        return node

    def add_edge(self, from_node, to_node):
        """Adds a directed edge from `from_node` to `to_node`."""
        if to_node not in from_node.dependents:
            from_node.dependents.append(to_node)
        if from_node not in to_node.dependencies:
            to_node.dependencies.append(from_node)

    def __lshift__(self, from_node, to_node):
        """Overload << to add dependency from 'from_node' to 'to_node'."""
        self.add_edge(from_node, to_node)

    def __repr__(self):
        """Return a string representation of the graph."""
        return "\n".join(f"{node.value} -> {[n.value for n in node.dependents]}" for node in self.nodes)


class Executor:
    """Executor that runs the tasks in the DAG respecting the dependencies."""

    def __init__(self):
        self.graph = Graph()

    def create_task(self, callable):
        node = self.graph.create_node(Task(callable))
        return node


    # list of tasks that can be executed (i.e., tasks with no un-executed dependencies)
    def get_ready_tasks(self):
        return [node.value for node in self.graph.nodes if not node.dependencies]

    def set_dependency(self, node_a, node_b):
        self.graph.add_edge(node_a, node_b)

    def run_pipeline(self):
        """Run the tasks in the pipeline respecting their dependencies, executing independent tasks in parallel."""
        ready_tasks = self.get_ready_tasks()

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
                    for node in self.graph.nodes:
                        t = node.value
                        if not t.started and all(dep.value.executed for dep in node.dependencies):
                            futures[t] = executor.submit(t.execute)

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
