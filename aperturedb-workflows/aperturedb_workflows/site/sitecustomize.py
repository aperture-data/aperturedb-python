import sys
from aperturedb_workflows.status_tools import StatusUpdater, WorkFlowError
import logging

old_handler = sys.excepthook

logging.info("Setting up exception handler")
updater = StatusUpdater()


def exception_handler(type, value, tb):
    updater.post_update(
        error_message=f"Exception: {type.__name__} {value}",
        error_code=WorkFlowError.WORKFLOW_ERROR
    )
    old_handler(type, value, tb)


sys.excepthook = exception_handler
