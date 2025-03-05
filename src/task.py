from enum import Enum

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class TaskState(Enum):
    PENDING = 0
    SUBMITTED = 1
    STARTED = 2
    FINISHED = 3

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

    def is_submitted(self):
        return self.state == TaskState.SUBMITTED

    def is_running(self):
        return self.state == TaskState.STARTED

    def is_finished(self):
        return self.state == TaskState.FINISHED

    def set_submitted(self):
        self.state = TaskState.SUBMITTED

    def set_started(self):
        self.state = TaskState.STARTED

    def set_finished(self):
        self.state = TaskState.FINISHED

    def execute(self, *args):
        logger.info(f"Task {self.id} STARTED")
        #logger.info(f"inputs: {inputs}")
        self.result = self.callable(*args)
        logger.info(f"Task {self.id} FINISHED")
        #logger.info(f"outputs: {self.result}")
