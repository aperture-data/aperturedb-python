"""Check the installed wheel: commands, runtime files and app overrides."""
from importlib import metadata, resources
import subprocess
import sys

import pytest

import aperturedb
import aperturedb_workflows
from aperturedb_workflows import status_server

COMMANDS = {
    "wf-start": "aperturedb_workflows.cli:start",
    "wf-argparse": "aperturedb_workflows.wf_argparse:main",
    "wf-status": "aperturedb_workflows.status_tools:main",
    "wf-status-server": "aperturedb_workflows.status_server:main",
    "wf-log-processor": "aperturedb_workflows.log_processor:app",
    "wf-userlog": "aperturedb_workflows.userlog:run",
    "wf-slack-alert": "aperturedb_workflows.slack_alert:run",
}


def test_version_matches_the_sdk_released_with_it():
    assert aperturedb_workflows.__version__ == aperturedb.__version__
    assert metadata.version("aperturedb-workflows") == aperturedb.__version__


def test_commands_load():
    scripts = {entry.name: entry for entry in metadata.distribution(
        "aperturedb-workflows").entry_points if entry.group == "console_scripts"}
    assert {name: entry.value for name, entry in scripts.items()} == COMMANDS
    for entry in scripts.values():
        assert callable(entry.load())


@pytest.mark.parametrize("path", ["start.sh", "app.sh", "site/sitecustomize.py"])
def test_runtime_files_are_packaged(path):
    assert (resources.files("aperturedb_workflows") / path).is_file()


@pytest.mark.parametrize("script", ["start.sh", "app.sh"])
def test_shell_runtime_parses(script):
    path = resources.files("aperturedb_workflows") / script
    subprocess.run(["bash", "-n", str(path)], check=True)


def test_wf_argparse_command_validates():
    valid = subprocess.run(["wf-argparse", "--type", "bool", "--value", "yes"],
                           capture_output=True, text=True, check=True)
    assert valid.stdout.strip() == "true"
    invalid = subprocess.run(["wf-argparse", "--type", "port", "--value", "x"],
                             capture_output=True, text=True)
    assert invalid.returncode != 0


def test_reconfigure_defaults_to_the_package(monkeypatch):
    monkeypatch.delitem(sys.modules, "reconfigure", raising=False)
    monkeypatch.setattr(
        sys, "path", [p for p in sys.path if p not in ("", ".")])
    assert status_server.reconfigure_workflow() == {
        "message": "Reconfiguration not implemented"}


def test_app_reconfigure_replaces_the_default(tmp_path, monkeypatch):
    (tmp_path / "reconfigure.py").write_text(
        "def reconfigure():\n    return {'message': 'app'}\n")
    monkeypatch.delitem(sys.modules, "reconfigure", raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    assert status_server.reconfigure_workflow() == {"message": "app"}
