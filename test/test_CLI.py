from typer.testing import CliRunner
from aperturedb.cli.configure import app

import logging
logger = logging.getLogger(__name__)


class TestConfigure():
    def test_create_no_folder_exists_global(self):
        runner = CliRunner()
        result = runner.invoke(app, ["create", "test"])
        assert result.exit_code == 0

    def test_create_no_folder_exists_project(self):
        runner = CliRunner()
        result = runner.invoke(app, ["create", "test", "--no_as_global"])
        assert result.exit_code == 0
