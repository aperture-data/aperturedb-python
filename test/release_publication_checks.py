"""Offline HTTP secrecy and real-Git local/remote tag collision controls."""
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import urllib.error


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "sdk_github_release", ROOT / "scripts/github_release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
COMMIT = "a" * 40
TOKEN = "SENSITIVE_FAKE_TOKEN"


class PublicationControls(unittest.TestCase):
    def test_existing_release_updates_same_ID_preserving_notes_assets_flags(self):
        calls = []

        def request(method, path, body, token):
            calls.append((method, path, body, token))
            return 200, {"id": 7, "tag_name": "v9.9.9", "body": "PRIVATE_RELEASE_BODY"}
        result = release.publish("9.9.9", COMMIT, TOKEN, request)
        self.assertEqual(
            result, {"state": "updated", "release_id": 7, "tag": "v9.9.9", "commit": COMMIT})
        self.assertEqual(calls, [("GET", "/releases/tags/v9.9.9", None, TOKEN),
                                 ("PATCH", "/releases/7", {"target_commitish": COMMIT}, TOKEN)])
        self.assertNotIn("PRIVATE_RELEASE_BODY", json.dumps(result))

    def test_absent_release_creates_fixed_tag(self):
        calls = []

        def request(method, path, body, token):
            calls.append((method, path, body))
            return (404, None) if method == "GET" else (201, {"id": 8, "tag_name": "v9.9.9"})
        self.assertEqual(release.publish(
            "9.9.9", COMMIT, TOKEN, request)["state"], "created")
        self.assertEqual(calls[1][0:2], ("POST", "/releases"))
        self.assertEqual(calls[1][2]["target_commitish"], COMMIT)

    def test_failure_output_never_contains_token_commands_or_raw_bodies(self):
        for replies in [[(403, {"body": "PRIVATE"})],
                        [(200, {"id": 7, "tag_name": "v9.9.9"}),
                         (500, {"body": "PRIVATE"})],
                        [(200, {"id": True, "tag_name": "v9.9.9"})],
                        [(200, {"id": 7, "tag_name": "v9.9.9", "immutable": True})]]:
            stream = io.StringIO()
            with patch.dict(os.environ, {"TOKEN": TOKEN}, clear=True), redirect_stdout(stream):
                iterator = iter(replies)
                self.assertEqual(release.main(["--version", "9.9.9", "--commit", COMMIT],
                                              lambda *_: next(iterator)), 1)
            self.assertNotIn(TOKEN, stream.getvalue())
            self.assertNotIn("PRIVATE", stream.getvalue())
            self.assertNotIn("Authorization", stream.getvalue())
            self.assertEqual(json.loads(stream.getvalue())["state"], "failed")

    def test_token_is_header_only_and_redirects_cannot_forward_it(self):
        observed = []

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def read(self, limit):
                return b'{"id":7,"tag_name":"v9.9.9"}'

        class Opener:
            def open(self, request, timeout):
                observed.append(request)
                return Response()
        with patch.object(release.urllib.request, "build_opener", return_value=Opener()):
            release.github("PATCH", "/releases/7",
                           {"target_commitish": COMMIT}, TOKEN)
        request = observed[0]
        self.assertTrue(request.full_url.startswith(
            release.API + "/releases/"))
        self.assertEqual(request.get_header(
            "Authorization"), "Bearer " + TOKEN)
        self.assertNotIn(TOKEN, request.full_url)
        self.assertNotIn(TOKEN.encode(), request.data)
        self.assertIsNone(release.NoRedirect().redirect_request(
            request, None, 302, "", {}, "https://evil.invalid"))

    def test_HTTP_error_body_is_not_read(self):
        body = io.BytesIO(b"SENSITIVE_RESPONSE")
        body.read = lambda *_: self.fail("Error body must not be read")
        error = urllib.error.HTTPError(release.API, 403, "PRIVATE", {}, body)

        class Opener:
            def open(self, *args, **kwargs):
                raise error
        with patch.object(release.urllib.request, "build_opener", return_value=Opener()):
            self.assertEqual(release.github(
                "GET", "/releases/tags/v9.9.9", None, TOKEN), (403, None))

    def test_invalid_version_commit_status_and_identity_fail_closed(self):
        for version, commit in [("SECRET/path", COMMIT), ("9.9.9", "SECRET")]:
            with self.assertRaises(release.PublicationError):
                release.publish(version, commit, TOKEN,
                                lambda *_: self.fail("No request allowed"))
        for status in (True, 200.0, float("inf")):
            with self.assertRaises(release.PublicationError):
                release.publish("9.9.9", COMMIT, TOKEN,
                                lambda *_: (status, {}))
        replies = iter([(200, {"id": 7, "tag_name": "v9.9.9"}),
                        (200, {"id": 8, "tag_name": "v9.9.9"})])
        with self.assertRaises(release.PublicationError):
            release.publish("9.9.9", COMMIT, TOKEN, lambda *_: next(replies))

    def git_case(self, remote_matches):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            origin, work = base / "origin.git", base / "work"
            work.mkdir()
            env = os.environ | {"GIT_CONFIG_NOSYSTEM": "1",
                                "GIT_CONFIG_GLOBAL": "/dev/null"}

            def git(*args):
                return subprocess.check_output(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", *args],
                                               cwd=work, env=env, stderr=subprocess.DEVNULL, text=True).strip()
            git("init", "--bare", str(origin))
            git("init", "-b", "main")
            (work / "aperturedb").mkdir()
            (work / "aperturedb/__init__.py").write_text('__version__ = "9.9.9"\n')
            git("add", ".")
            git("commit", "-m", "old")
            old = git("rev-parse", "HEAD")
            git("tag", "v9.9.9")
            (work / "changed").write_text("new source\n")
            git("add", ".")
            git("commit", "-m", "new")
            current = git("rev-parse", "HEAD")
            git("remote", "add", "origin", str(origin))
            expected = current if remote_matches else old
            git("push", "origin", expected + ":refs/tags/v9.9.9")
            result = subprocess.run(["bash", str(ROOT / "tag.sh")], cwd=work,
                                    env=env | {"TAG_BASE": old}, capture_output=True)
            self.assertEqual(git("rev-parse", "refs/tags/v9.9.9"), current)
            self.assertEqual(git("--git-dir=" + str(origin),
                             "rev-parse", "refs/tags/v9.9.9"), expected)
            if remote_matches:
                self.assertEqual(result.returncode, 0)
            else:
                self.assertNotEqual(result.returncode, 0)

    def test_stale_local_tag_replaced_after_root_moves_remote(self):
        self.git_case(True)

    def test_differing_remote_tag_is_not_force_updated(self):
        self.git_case(False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
