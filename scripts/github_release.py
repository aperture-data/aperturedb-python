"""Create or update the fixed SDK release without printing credentials or bodies."""
import argparse
import json
import os
import re
import urllib.error
import urllib.request


API = "https://api.github.com/repos/aperture-data/aperturedb-python"
VERSION = re.compile(r"(?:0|[1-9][0-9]{0,5})\.(?:0|[1-9][0-9]{0,5})\."
                     r"(?:0|[1-9][0-9]{0,5})")


class PublicationError(Exception):
    def __init__(self, reason, status=None):
        self.reason = reason
        self.status = status


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, new_url):
        return None


def github(method, path, payload, token):
    encoded = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(API + path, data=encoded, method=method, headers={
        "Accept": "application/vnd.github+json", "Authorization": "Bearer " + token,
        "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "aperturedb-sdk-release",
    })
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            status, raw = response.status, response.read(131073)
    except urllib.error.HTTPError as error:
        # Never read or print an error body, URL, headers or Request object.
        status = error.code
        error.close()
        return status, None
    except (OSError, urllib.error.URLError):
        raise PublicationError("network_failure") from None
    if len(raw) > 131072:
        raise PublicationError("response_too_large")
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeError):
        raise PublicationError("invalid_response") from None
    return status, body


def release_fields(body, tag):
    if (not isinstance(body, dict) or type(body.get("id")) is not int
            or not 0 < body["id"] < 2**63 or body.get("tag_name") != tag):
        raise PublicationError("release_response_mismatch")
    immutable = body.get("immutable", False)
    if type(immutable) is not bool:
        raise PublicationError("invalid_response")
    if immutable:
        raise PublicationError("immutable_release")
    return body["id"]


def publish(version, commit, token, transport=github):
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise PublicationError("invalid_version")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise PublicationError("invalid_commit")
    if not isinstance(token, str) or not 0 < len(token) <= 8192 or "\r" in token or "\n" in token:
        raise PublicationError("missing_or_invalid_token")
    tag = "v" + version
    status, existing = transport("GET", "/releases/tags/" + tag, None, token)
    if type(status) is not int or not 100 <= status <= 599:
        raise PublicationError("invalid_http_status")
    if status == 200:
        release_id = release_fields(existing, tag)
        # Keep the existing URL/ID, title, notes, assets and release flags.
        status, body = transport("PATCH", "/releases/" + str(release_id),
                                 {"target_commitish": commit}, token)
        operation, expected = "updated", 200
    elif status == 404:
        status, body = transport("POST", "/releases", {
            "tag_name": tag, "target_commitish": commit, "name": tag,
            "body": "Release " + tag, "draft": False, "prerelease": False}, token)
        release_id = None
        operation, expected = "created", 201
    else:
        raise PublicationError("github_http_failure", status)
    if type(status) is not int or status != expected:
        raise PublicationError("github_http_failure", status)
    confirmed = release_fields(body, tag)
    if release_id is not None and confirmed != release_id:
        raise PublicationError("release_response_mismatch")
    return {"state": operation, "release_id": confirmed, "tag": tag, "commit": commit}


def main(arguments=None, transport=github):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args(arguments)
    try:
        result = publish(args.version, args.commit,
                         os.environ.get("TOKEN"), transport)
        code = 0
    except PublicationError as error:
        result = {"state": "failed", "reason": error.reason}
        if type(error.status) is int and 100 <= error.status <= 599:
            result["http_status"] = error.status
        code = 1
    except Exception:
        result = {"state": "failed", "reason": "publication_unavailable"}
        code = 1
    print(json.dumps(result, allow_nan=False), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
