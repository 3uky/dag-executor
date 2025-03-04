from graph import DirectAcyclicGraph
from task import Task

class Pipeline:
    def __init__(self):
        self.graph = DirectAcyclicGraph()

    def create_task(self, callable):
        task = Task(callable)
        self.graph.add_node(task)
        return task

    def get_tasks(self):
        return self.graph.nodes

    def get_ready_to_run_tasks(self):
        return [task for task in self.get_tasks() if task.is_pending() and self.are_inputs_available(task)]

    def are_inputs_available(self, task):
        return all(dependent_task.is_finished() for dependent_task in self.get_dependencies(task))

    def set_dependency(self, node_a, node_b):
        self.graph.add_edge(node_a, node_b)

    def get_dependencies(self, node):
        return self.graph.get_input_nodes(node)

    def get_task_inputs(self, task):
        return [dependent_task.result for dependent_task in self.get_dependencies(task) if dependent_task.result is not None]
