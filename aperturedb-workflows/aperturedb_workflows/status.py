from typing import List
from prometheus_client import Gauge, Enum, Info
import logging
from .status_tools import WorkFlowError

logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Status(metaclass=Singleton):
    accesssible = Enum(
        "accessible", "Is the workflow accessible?", states=["yes", "no"])
    status = Enum("status", "Status of the workflow", states=["started"])
    phases = Gauge("phases", "Phases of the workflow", ["phase"])
    workflow_info = Info("workflow_information",
                         "Information about the state of the workflow")

    def __init__(self, phases: List[str]):
        assert isinstance(phases, list), "phases must be a list"
        assert len(phases) > 0, "phases list cannot be empty"
        self.possible_phases = phases
        self.description = {
            "version": "1.0.0",
            "description": "This is an example workflow application.",
            "phases": "|".join(phases),
            "error_message": "",
            "error_code": "",
        }
        self.workflow_info.info(self.description)
        self.accesssible.state("no")
        logger.info("Workflow Status created.")

    def error(self, message: str, code: WorkFlowError):
        """
        Set an error message and code for the workflow status.
        """
        self.description["error_message"] = message
        self.description["error_code"] = code.value
        self.workflow_info.info(self.description)
        logger.error(f"Error in workflow: {message} (Code: {code})")
