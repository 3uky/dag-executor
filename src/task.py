from enum import Enum

import logging

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
        logger.info(f"Task {self.id} STARTED")
        #logger.info(f"inputs: {inputs}")
        self.state = TaskState.STARTED
        self.result = self.callable(inputs)
        self.state = TaskState.FINISHED
        logger.info(f"Task {self.id} FINISHED")
        #logger.info(f"outputs: {self.result}")
