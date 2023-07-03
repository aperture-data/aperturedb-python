import json
import logging
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import typer
from typer.testing import CliRunner

from aperturedb.cli.configure import app

logger = logging.getLogger(__name__)


class TestConfigure():
    def check_contents(self, filename: str, section: str, config_name: str):
        with open(filename) as instream:
            obj = json.loads(instream.read())
            if section is not None:
                assert section in obj
                assert config_name in obj[section]
            else:
                assert config_name in obj

    def test_create_no_folder_exists_global(self):
        tmp = "/tmp/global"
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
        fake_folder = tmp
        fake_file = os.path.join(tmp, "adb.json")
        with patch.multiple(typer,
                            get_app_dir = MagicMock(return_value = fake_folder)):
            runner = CliRunner()
            result = runner.invoke(app, ["create", "test"])
            assert result.exit_code == 0
            self.check_contents(fake_file, None, "test")

    def test_create_no_folder_exists_local(self):
        tmp = "/tmp/local"
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
        fake_folder = tmp
        fake_file = os.path.join(tmp, ".aperturedb", "adb.json")
        with patch.multiple(os,
                            getcwd = MagicMock(return_value = fake_folder)):
            runner = CliRunner()
            result = runner.invoke(app, ["create", "test", "--no-as-global"])
            assert result.exit_code == 0
            self.check_contents(fake_file, None, "test")

    def test_create_folder_exists_global(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with patch.multiple(typer,
                                get_app_dir = MagicMock(return_value = fake_folder)):
                runner = CliRunner()
                result = runner.invoke(app, ["create", "test"])
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_folder_exists_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, ".aperturedb", "adb.json")
            with patch.multiple(os,
                                getcwd = MagicMock(return_value = fake_folder)):
                runner = CliRunner()
                result = runner.invoke(
                    app, ["create", "test", "--no-as-global"])
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_empty_file_exists_global(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with open(fake_file, "w") as outstream:
                pass
            with patch.multiple(typer,
                                get_app_dir = MagicMock(return_value = fake_folder)):
                runner = CliRunner()
                result = runner.invoke(app, ["create", "test"])
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_empty_file_exists_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, ".aperturedb", "adb.json")
            os.mkdir(os.path.join(fake_folder, ".aperturedb"))
            with open(fake_file, "w") as outstream:
                pass
            with patch.multiple(os,
                                getcwd = MagicMock(return_value = fake_folder)):
                runner = CliRunner()
                result = runner.invoke(
                    app, ["create", "test", "--no-as-global"])
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")
