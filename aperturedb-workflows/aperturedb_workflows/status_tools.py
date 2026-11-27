from typing import List, Optional
import logging
from enum import Enum as PyEnum
import os
import requests

logger = logging.getLogger(__name__)


class WorkflowStatus(PyEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "completed"
    FAILED = "failed"


class WorkFlowError(PyEnum):
    DB_ERROR = "db_error"
    WORKFLOW_ERROR = "workflow_error"
    API_ERROR = "api_error"


class StatusUpdater:
    def post_update(
            self,
            completed: Optional[float] = None,
            phases: Optional[List[str]] = None,
            phase: Optional[str] = None,
            error_message: Optional[str] = None,
            error_code: Optional[WorkFlowError] = None,
            status: Optional[WorkflowStatus] = None,
            accessible: Optional[bool] = None):
        wf_status = {
        }
        if completed is not None:
            wf_status["completeness"] = float(completed) / 100.0
        if phases is not None:
            wf_status["phases"] = phases
        if phase is not None:
            wf_status["phase"] = phase
        if error_message is not None:
            wf_status["error_message"] = error_message
        if error_code is not None:
            wf_status["error_code"] = error_code.value
        if status is not None:
            wf_status["status"] = status.value
        else:
            wf_status["status"] = WorkflowStatus.RUNNING.value
        if accessible is not None:
            wf_status["accessible"] = accessible
        if len(wf_status) > 0:
            try:
                update_response = requests.post(
                    f"http://localhost:8080/response",
                    json=wf_status
                )
                print(
                    f"Updating status: {wf_status}. Got {update_response.status_code}")
            except Exception as e:
                print(f"Failed to update status: {e}")


def main():
    # protect this import because this module is used by sitecustomize.py
    from typer import Typer
    app = Typer()

    @app.command()
    def shell_updater(
        completed: Optional[float] = None,
        phases: Optional[List[str]] = None,
        phase: Optional[str] = None,
        status: Optional[WorkflowStatus] = None,
        error_message: Optional[str] = None,
        error_code: Optional[WorkFlowError] = None,
        accessible: Optional[bool] = None,
    ):
        updater = StatusUpdater()
        updater.post_update(
            completed=completed,
            phases=phases,
            phase=phase,
            status=status,
            error_message=error_message,
            error_code=error_code,
            accessible=accessible)

    app()


if __name__ == "__main__":
    main()
