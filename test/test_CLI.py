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
                            get_app_dir=MagicMock(return_value=fake_folder)):
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
                            getcwd=MagicMock(return_value=fake_folder)):
            runner = CliRunner()
            result = runner.invoke(app, ["create", "test", "--no-as-global"])
            assert result.exit_code == 0
            self.check_contents(fake_file, None, "test")

    def test_create_folder_exists_global(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):
                runner = CliRunner()
                result = runner.invoke(app, ["create", "test"])
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_from_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):
                runner = CliRunner()
                result = runner.invoke(app, ["create", "--from-json"],
                                       input='{"name": "test", "host": "test", "port": 1234, "username": "test", "password": "test"}')
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_from_json_with_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):
                runner = CliRunner()
                result = runner.invoke(app, ["create", "--from-json", "test"],
                                       input='{"host": "test", "port": 1234, "username": "test", "password": "test"}')
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_from_json_with_two_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):
                runner = CliRunner()
                result = runner.invoke(app, ["create", "--from-json", "test"],
                                       input='{"name": "test2", "host": "test", "port": 1234, "username": "test", "password": "test"}')
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_create_folder_exists_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, ".aperturedb", "adb.json")
            with patch.multiple(os,
                                getcwd=MagicMock(return_value=fake_folder)):
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
                                get_app_dir=MagicMock(return_value=fake_folder)):
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
                                getcwd=MagicMock(return_value=fake_folder)):
                runner = CliRunner()
                result = runner.invoke(
                    app, ["create", "test", "--no-as-global"])
                assert result.exit_code == 0
                self.check_contents(fake_file, None, "test")

    def test_ls_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):
                with patch.multiple(os,
                                    getcwd=MagicMock(return_value=fake_folder)):
                    runner = CliRunner()
                    result = runner.invoke(app, ["ls"])
                    assert result.exit_code == 0
                    assert "adb is not configured yet." in result.stdout
                    assert "Please run adb config" in result.stdout

    def test_ls_empty_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_folder = tmp
            fake_file = os.path.join(tmp, "adb.json")
            with open(fake_file, "w") as outstream:
                pass
            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):
                with patch.multiple(os,
                                    getcwd=MagicMock(return_value=fake_folder)):
                    runner = CliRunner()
                    result = runner.invoke(app, ["ls"])
                    assert "Failed to decode json" in result.stdout

    def test_ls_global_only(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_folder = tmp_global
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=fake_folder)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):

                        runner = CliRunner()
                        result = runner.invoke(app, ["ls"])
                        assert result.exit_code == 0

                        for key in config:
                            assert key in result.stdout

    def test_ls_local_only(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(config, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(app, ["ls"])
                        assert result.exit_code == 0

                        for key in config:
                            assert key in result.stdout

    def test_ls_both(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(config, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(app, ["ls"])
                        assert result.exit_code == 0

                        for key in config:
                            assert key in result.stdout

    def test_active_name_local_as_local(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    lc = {}
                    lc["second"] = config["first"]
                    lc["active"] = "second"
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(lc, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(
                            app, ["activate", "second", "--no-as-global"])
                        assert result.exit_code == 0

    def test_active_name_global_as_global(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(config, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(
                            app, ["activate", "first", "--as-global"])
                        assert result.exit_code == 0

    def test_active_name_local_as_global(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    lc = {}
                    lc["second"] = config["first"]
                    lc["active"] = "second"
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(config, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(
                            app, ["activate", "second", "--as-global"])
                        assert result.exit_code == 2

    def test_active_name_global_as_local(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    lc = {}
                    lc["second"] = config["first"]
                    lc["active"] = "second"
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(lc, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(app, ["activate", "first"])
                        assert result.exit_code == 0

    def test_active_name_not_present(self, config):
        with tempfile.TemporaryDirectory() as tmp_global:
            fake_file = os.path.join(tmp_global, "adb.json")
            with open(fake_file, "w") as outstream:
                outstream.write(json.dumps(config, indent=2))

            with patch.multiple(typer,
                                get_app_dir=MagicMock(return_value=tmp_global)):

                with tempfile.TemporaryDirectory() as tmp_local:
                    fake_folder = tmp_local
                    fake_file = os.path.join(
                        tmp_local, ".aperturedb", "adb.json")
                    os.mkdir(os.path.join(fake_folder, ".aperturedb"))
                    lc = {}
                    lc["second"] = config["first"]
                    with open(fake_file, "w") as outstream:
                        outstream.write(json.dumps(config, indent=2))
                    with patch.multiple(os,
                                        getcwd=MagicMock(return_value=tmp_local)):
                        runner = CliRunner()
                        result = runner.invoke(app, ["activate", "blah"])
                        assert result.exit_code == 2
                        assert "Configuration blah not found" in result.stdout
