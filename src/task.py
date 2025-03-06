from enum import Enum
import logging

logger = logging.getLogger(__name__)

class TaskState(Enum):
    PENDING = 0
    STARTED = 1
    FINISHED = 2

class Task:
    def __init__(self, callable):
        self.id = callable.__name__
        self.callable = callable
        self.state = TaskState.PENDING
        self.result = None

    def __str__(self):
        return f"{self.id}"

    def is_pending(self):
        return self.state == TaskState.PENDING

    def is_started(self):
        return self.state == TaskState.STARTED

    def is_finished(self):
        return self.state == TaskState.FINISHED

    def set_state_to_started(self):
        self.state = TaskState.STARTED
        logger.info(f"Task {self.id} STARTED")

    def set_state_to_finished(self):
        self.state = TaskState.FINISHED
        logger.info(f"Task {self.id} FINISHED")

    def get_result(self):
        return self.result

    def execute(self, *args):
        logger.info(f"Task {self.id} RUNNING")
        logger.info(f"inputs:\n{args}")
        result = self.callable(*args)
        logger.info(f"outputs:\n{result}")
        logger.info(f"Task {self.id} FINISHING")
        return result
