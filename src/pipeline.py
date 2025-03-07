from graph import DirectAcyclicGraph
from task import Task
from executor import Executor

class Pipeline:
    """
    The Pipeline class manages the orchestration of tasks, checking for readiness and managing dependencies. This is a
    central place for coordinating task execution.
    """

    def __init__(self):
        self.graph = DirectAcyclicGraph()
        self.executor = Executor()

    def create_task(self, callable):
        task = Task(callable)
        self.graph.add_node(task)
        return task

    def set_dependency(self, node_a, node_b):
        self.graph.add_edge(node_a, node_b)

    def run(self):
        self.initial_check()

        while not self.are_all_tasks_finished():
            self.submit_ready_tasks()
            self.executor.wait_for_task_finish()

    def initial_check(self):
        if not self.graph.is_acyclic():
            raise ValueError("The graph has cycles. Cannot execute pipeline.")

    def are_all_tasks_finished(self):
        return all(task.is_finished() for task in self.get_tasks())

    def submit_ready_tasks(self):
        for task in self.get_ready_to_run_tasks():
            inputs = self.get_task_inputs(task)
            self.executor.submit_for_execution(task, inputs)

    def get_ready_to_run_tasks(self):
        return [task for task in self.get_tasks() if task.is_pending() and self.are_inputs_available(task)]

    def get_tasks(self):
        return self.graph.nodes

    def are_inputs_available(self, task):
        return all(dependent_task.is_finished() for dependent_task in self.get_dependencies(task))

    def get_dependencies(self, node):
        return self.graph.get_input_nodes(node)

    def get_task_inputs(self, task):
        return [dependent_task.result for dependent_task in self.get_dependencies(task) if dependent_task.result is not None]