from graph import DirectAcyclicGraph
from task import Task

class Pipeline:
    def __init__(self):
        self.graph = DirectAcyclicGraph()

    def create_task(self, callable):
        task = Task(callable)
        self.graph.add_node(task)
        return task

    def set_dependency(self, node_a, node_b):
        self.graph.add_edge(node_a, node_b)

    def get_tasks(self):
        return self.graph.nodes

    def get_initial_tasks(self):
        return [task for task in self.get_tasks_without_dependencies() if task.is_not_started()]

    def get_ready_to_run_tasks(self):
        return [task for task in self.get_tasks() if task.is_not_started() and self.are_inputs_available(task)]

    def get_dependencies(self, node):
        return self.graph.get_input_nodes(node)

    def get_tasks_without_dependencies(self):
        return self.graph.get_nodes_without_input_edge()

    def get_required_inputs(self, task):
        return [dependent_task.result for dependent_task in self.get_dependencies(task) if dependent_task.result is not None]

    def are_inputs_available(self, task):
        return all(dependent_task.is_finished() for dependent_task in self.get_dependencies(task))