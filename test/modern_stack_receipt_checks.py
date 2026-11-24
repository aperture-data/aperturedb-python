"""Offline privacy and actual-image provenance controls for CI receipts."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "record_modern_stack", Path(__file__).with_name("record_modern_stack.py"))
receipt = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(receipt)
GUARD_SPEC = importlib.util.spec_from_file_location(
    "qualified_modern_stack", Path(__file__).with_name("require_modern_stack.py"))
qualified = importlib.util.module_from_spec(GUARD_SPEC)
GUARD_SPEC.loader.exec_module(qualified)
CONTAINER = "a" * 64
IMAGE = "sha256:" + "b" * 64
REFERENCE = receipt.REPOSITORY + "@sha256:" + "c" * 64
RELEASE = {"version": "1.0.0", "branch": "main", "commit": "d" * 40}


class FakeDocker:
    def __init__(self):
        self.containers = CONTAINER
        self.image_id = IMAGE
        self.metadata = {"id": IMAGE, "repo_digests": [REFERENCE]}
        self.release = RELEASE.copy()
        self.calls = []

    def __call__(self, arguments):
        self.calls.append(arguments)
        if arguments[1:3] == ["container", "ls"]:
            return self.containers
        if arguments[1:3] == ["container", "inspect"]:
            return self.image_id
        if arguments[1:3] == ["image", "inspect"]:
            return json.dumps(self.metadata)
        if arguments[1] == "exec":
            return self.release if isinstance(self.release, str) else json.dumps(self.release)
        raise AssertionError("Unexpected Docker operation")


class ModernStackReceipt(unittest.TestCase):
    def test_exact_service_image_source_and_read_only_commands(self):
        docker = FakeDocker()
        result = receipt.collect("sdk_gate_compat", docker)
        self.assertEqual(result, {"schema_version": 2, "state": "passed", "service": "aperturedb",
                                  "image_id": IMAGE, "official_database_repo_digests": [REFERENCE],
                                  "release": RELEASE})
        self.assertEqual(docker.calls, [
            ["docker", "container", "ls", "--all", "--no-trunc", "--quiet", "--filter",
             "label=com.docker.compose.project=sdk_gate_compat", "--filter",
             "label=com.docker.compose.service=aperturedb"],
            ["docker", "container", "inspect",
                "--format", "{{.Image}}", CONTAINER],
            ["docker", "image", "inspect", "--format", receipt.IMAGE_FORMAT, IMAGE],
            ["docker", "exec", CONTAINER, "head", "-c", "4097", "/VERSION.json"],
        ])

    def test_same_version_different_published_source_cannot_look_equal(self):
        first = receipt.collect("sdk_gate", FakeDocker())
        docker = FakeDocker()
        docker.image_id = "sha256:" + "e" * 64
        docker.metadata = {"id": docker.image_id, "repo_digests": [
            receipt.REPOSITORY + "@sha256:" + "f" * 64]}
        docker.release["commit"] = "1" * 40
        second = receipt.collect("sdk_gate", docker)
        self.assertEqual(first["release"]["version"],
                         second["release"]["version"])
        self.assertNotEqual(first["image_id"], second["image_id"])
        self.assertNotEqual(first["official_database_repo_digests"],
                            second["official_database_repo_digests"])
        self.assertNotEqual(first["release"]["commit"],
                            second["release"]["commit"])

    def test_only_official_database_references_are_retained(self):
        docker = FakeDocker()
        docker.metadata["repo_digests"] = ["docker.io/" + REFERENCE, "registry-1.docker.io/" + REFERENCE,
                                           "private.example/SECRET@sha256:" + "9" * 64]
        result = receipt.collect("sdk_gate", docker)
        self.assertEqual(
            result["official_database_repo_digests"], [REFERENCE])
        self.assertNotIn("SECRET", json.dumps(result))
        for references in (["evil.example/" + REFERENCE],
                           ["aperturedata/aperturedb-standalone@sha256:" + "c" * 64]):
            docker.metadata["repo_digests"] = references
            with self.assertRaises(receipt.IdentityError):
                receipt.collect("sdk_gate", docker)

    def test_nonunique_or_malformed_container_cannot_be_inspected(self):
        for identifiers in ("", CONTAINER + "\n" + CONTAINER, "not-a-container"):
            docker = FakeDocker()
            docker.containers = identifiers
            with self.assertRaises(receipt.IdentityError):
                receipt.collect("sdk_gate", docker)
            self.assertEqual(len(docker.calls), 1)
        docker = FakeDocker()
        with self.assertRaises(receipt.IdentityError):
            receipt.collect("sdk_gate\nSECRET", docker)
        self.assertEqual(docker.calls, [])

    def test_image_identity_and_metadata_shape_fail_closed(self):
        for change in (lambda d: setattr(d, "image_id", "SECRET"),
                       lambda d: d.metadata.update(id="sha256:" + "e" * 64),
                       lambda d: d.metadata.update(Env=["SECRET"]),
                       lambda d: d.metadata.update(repo_digests=None)):
            docker = FakeDocker()
            change(docker)
            with self.assertRaises(receipt.IdentityError):
                receipt.collect("sdk_gate", docker)

    def test_version_fields_are_strict_and_private_values_never_survive(self):
        for release in ({**RELEASE, "extra": "SECRET"}, {**RELEASE, "version": True},
                        {**RELEASE, "branch": "SECRET"}, {**
                                                          RELEASE, "commit": "short"},
                        '{"version":"1.0.0","branch":"main","commit":NaN}',
                        '{"version":"1.0.0","version":"SECRET"}', "x" * 4097):
            docker = FakeDocker()
            docker.release = release
            with tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "identity.json"
                output.write_text('{"state":"passed","release":"stale"}')
                self.assertEqual(receipt.record("sdk_gate", output, docker), 1)
                saved = json.loads(output.read_text())
                self.assertEqual(saved["state"], "failed")
                self.assertEqual(
                    set(saved), {"schema_version", "state", "service", "reason"})
                self.assertNotIn("SECRET", output.read_text())
                self.assertNotIn("stale", output.read_text())

    def test_docker_failure_output_and_overlong_data_are_not_retained(self):
        for result in (subprocess.CompletedProcess([], 1, b"SECRET", b"PRIVATE_TOKEN"),
                       subprocess.CompletedProcess([], 0, b"x" * 4097, b"PRIVATE_TOKEN")):
            with patch.object(receipt.subprocess, "run", return_value=result):
                with self.assertRaises(receipt.IdentityError) as error:
                    receipt.read(["docker", "container", "ls"])
                self.assertEqual(str(error.exception), "docker_read_failed")

    def test_capture_is_atomic_and_not_run_does_not_claim_core_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "identity.json"
            output.write_text(
                '{"schema_version":2,"state":"not_run","service":"aperturedb"}')
            self.assertEqual(receipt.record(
                "sdk_gate", output, FakeDocker()), 0)
            self.assertEqual(json.loads(output.read_text())["state"], "passed")
            self.assertFalse(output.with_suffix(".tmp").exists())


class QualifiedStackReceipt(unittest.TestCase):
    def identity(self):
        return receipt.collect("sdk_gate", FakeDocker())

    def check(self, value, expected_branch=None):
        def selected_image(args):
            if args[1:3] == ["image", "inspect"] and args[-1] == REFERENCE:
                return IMAGE
            if args[1] == "run" and args[-4] == REFERENCE:
                return json.dumps(RELEASE)
            self.fail("Unexpected Docker operation")
        qualified.validate(value, REFERENCE, selected_image,
                           expected_branch=expected_branch)

    def test_compose_selected_source_and_image_pass(self):
        self.check(self.identity())

    def test_release_requires_main_branch_of_compose_selected_image(self):
        self.check(self.identity(), expected_branch="main")
        with patch.dict(RELEASE, branch="develop"):
            self.check(self.identity())
            with self.assertRaisesRegex(ValueError, "unexpected_server_branch"):
                self.check(self.identity(), expected_branch="main")

    def test_same_version_stale_source_fails(self):
        value = self.identity()
        value["release"]["commit"] = "7a40" + "0" * 36
        with self.assertRaises(ValueError):
            self.check(value)

    def test_wrong_official_root_unknown_image_or_standalone_fails(self):
        for change in ({"image_id": "sha256:" + "9" * 64},
                       {"official_database_repo_digests": [
                           "aperturedata/aperturedb@sha256:" + "9" * 64]},
                       {"official_database_repo_digests": ["aperturedata/aperturedb-standalone@sha256:" + "9" * 64]}):
            with self.assertRaises(ValueError):
                self.check(self.identity() | change)

    def test_incomplete_legacy_or_extra_private_shape_cannot_qualify(self):
        for value in ({"schema_version": 2, "state": "not_run", "service": "aperturedb"},
                      self.identity() | {"schema_version": 1}, self.identity() | {"Env": ["SECRET"]}):
            with self.assertRaises(ValueError):
                self.check(value)
        with patch.dict(RELEASE, version="0.20.0"), self.assertRaises(ValueError):
            self.check(self.identity())

    def test_actual_launcher_scopes_pin_and_rejects_stale_before_client_run(self):
        source = Path(__file__).resolve().parent
        for stale in (False, True):
            with self.subTest(stale=stale), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                scripts, binaries = root / "test", root / "bin"
                scripts.mkdir()
                binaries.mkdir()
                for name in ("run_compatibility_container.sh", "record_modern_stack.py", "require_modern_stack.py"):
                    shutil.copyfile(source / name, scripts / name)
                trace = root / "trace.jsonl"
                common = "import json,os,sys\nfrom pathlib import Path\n"
                (scripts / "stack.sh").write_text("#!/usr/bin/env python3\n" + common +
                                                  "if sys.argv[1]=='image': print(" + repr(
                                                      "docker.io/aperturedata/aperturedb:test@" + REFERENCE.split("@")[1]) + ")\n"
                                                  "with Path(os.environ['TRACE_PATH']).open('a') as stream:\n"
                                                  " stream.write(json.dumps({'kind':'stack','action':sys.argv[1],"
                                                  "'image':os.environ.get('APERTUREDB_IMAGE'),'ssl':os.environ.get('LNZ_FORCE_SSL')})+'\\n')\n")
                (scripts / "stack.sh").chmod(0o755)
                release = self.identity()["release"]
                if stale:
                    release["commit"] = "7a40" + "0" * 36
                docker = ("#!/usr/bin/env python3\n" + common + "args=sys.argv[1:]\n" +
                          "if args[:2]==['container','ls']: print('a'*64)\n" +
                          "elif args[:2]==['container','inspect']: print(" + repr(IMAGE) + ")\n" +
                          "elif args[:2]==['image','inspect'] and args[-1]==" + repr(REFERENCE) + ": print(" + repr(IMAGE) + ")\n" +
                          "elif args[0]=='run' and '--read-only' in args: print(" + repr(json.dumps(RELEASE)) + ")\n" +
                          "elif args[:2]==['image','inspect']: print(" + repr(json.dumps({
                              "id": IMAGE, "repo_digests": [REFERENCE]})) + ")\n" +
                          "elif args[0]=='exec': print(" + repr(json.dumps(release)) + ")\n" +
                          "elif args[0]=='run':\n with Path(os.environ['TRACE_PATH']).open('a') as stream:\n" +
                          "  stream.write(json.dumps({'kind':'client','image':os.environ.get('APERTUREDB_IMAGE'),'ssl':os.environ.get('LNZ_FORCE_SSL')})+'\\n')\n" +
                          "else: raise SystemExit(2)\n")
                (binaries / "docker").write_text(docker)
                (binaries / "docker").chmod(0o755)
                env = os.environ | {"PATH": str(binaries) + os.pathsep + os.environ["PATH"],
                                    "TRACE_PATH": str(trace), "RUNNER_NAME": "unit_guard",
                                    "ADB_COMPOSE": "qualified-compose.yml",
                                    "APERTUREDB_IMAGE": "legacy-must-remain-unchanged", "LNZ_FORCE_SSL": "legacy-mode-unchanged"}
                result = subprocess.run(["bash", str(scripts / "run_compatibility_container.sh")],
                                        env=env, capture_output=True, timeout=30)
                rows = [json.loads(line)
                        for line in trace.read_text().splitlines()]
                self.assertEqual(
                    [row["action"] for row in rows if row["kind"] == "stack"], ["image", "up", "down"])
                self.assertIsNone(rows[0]["image"])
                self.assertTrue(all(
                    row["image"] == REFERENCE and row["ssl"] == "true" for row in rows[1:] if row["kind"] == "stack"))
                clients = [row for row in rows if row["kind"] == "client"]
                if stale:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(clients, [])
                else:
                    self.assertEqual(result.returncode, 0)
                    self.assertEqual(
                        clients, [{"kind": "client", "image": "legacy-must-remain-unchanged", "ssl": "legacy-mode-unchanged"}])


if __name__ == "__main__":
    unittest.main(verbosity=2)
