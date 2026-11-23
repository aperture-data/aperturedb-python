"""Record only the actual modern database image and its published source fields."""
import argparse
import json
from pathlib import Path
import re
import subprocess


DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
CONTAINER = re.compile(r"[0-9a-f]{64}")
PROJECT = re.compile(r"[a-z0-9][a-z0-9_-]{0,127}")
VERSION = re.compile(r"(?:0|[1-9][0-9]{0,5})\.(?:0|[1-9][0-9]{0,5})\."
                     r"(?:0|[1-9][0-9]{0,5})(?:-rc\.(?:0|[1-9][0-9]{0,5}))?")
REPOSITORY = "aperturedata/aperturedb"
REFERENCE = re.compile(r"(?:(?:docker\.io|registry-1\.docker\.io)/)?"
                       + REPOSITORY + r"@(sha256:[0-9a-f]{64})")
IMAGE_FORMAT = '{"id":{{json .Id}},"repo_digests":{{json .RepoDigests}}}'


class IdentityError(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise IdentityError(reason)


def read(arguments):
    """The caller supplies only fixed, read-only Docker commands."""
    try:
        result = subprocess.run(arguments, capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        raise IdentityError("docker_read_failed") from None
    require(result.returncode == 0 and len(
        result.stdout) <= 4096, "docker_read_failed")
    try:
        return result.stdout.decode("utf-8").strip()
    except UnicodeError:
        raise IdentityError("invalid_metadata_encoding") from None


def object_json(value):
    def unique(pairs):
        result = {}
        for key, item in pairs:
            require(key not in result, "invalid_metadata_shape")
            result[key] = item
        return result

    def constant(_):
        raise IdentityError("invalid_metadata_shape")

    try:
        result = json.loads(value, object_pairs_hook=unique,
                            parse_constant=constant)
    except (ValueError, TypeError):
        raise IdentityError("invalid_metadata_shape") from None
    require(isinstance(result, dict), "invalid_metadata_shape")
    return result


def parse_release(raw):
    require(len(raw.encode("utf-8")) <= 4096, "invalid_release_metadata")
    release = object_json(raw)
    require(set(release) == {"version", "branch",
            "commit"}, "invalid_release_metadata")
    require(isinstance(release["version"], str) and VERSION.fullmatch(release["version"])
            and release["branch"] in ("main", "develop")
            and isinstance(release["commit"], str) and re.fullmatch(r"[0-9a-f]{40}", release["commit"]),
            "invalid_release_metadata")
    return release


def collect(project, run=read):
    require(isinstance(project, str) and PROJECT.fullmatch(
        project), "invalid_project_name")
    # Filters identify the Compose service without reading its environment/configuration.
    identifiers = run(["docker", "container", "ls", "--all", "--no-trunc", "--quiet",
                       "--filter", "label=com.docker.compose.project=" + project,
                       "--filter", "label=com.docker.compose.service=aperturedb"]).splitlines()
    require(len(identifiers) == 1 and CONTAINER.fullmatch(
        identifiers[0]), "service_container_not_unique")
    container = identifiers[0]
    image_id = run(["docker", "container", "inspect",
                   "--format", "{{.Image}}", container])
    require(DIGEST.fullmatch(image_id), "invalid_image_identity")
    metadata = object_json(
        run(["docker", "image", "inspect", "--format", IMAGE_FORMAT, image_id]))
    require(set(metadata) == {"id", "repo_digests"}
            and metadata["id"] == image_id, "invalid_image_identity")
    references = metadata["repo_digests"]
    require(isinstance(references, list) and 1 <= len(references) <= 32
            and all(isinstance(value, str) and len(value) <= 256 for value in references),
            "invalid_repo_digests")
    official = sorted({REPOSITORY + "@" + match[1] for value in references
                       if (match := REFERENCE.fullmatch(value))})
    require(official, "official_database_digest_missing")
    # Bound the only file read inside the container. Nothing executes from that file.
    raw = run(["docker", "exec", container, "head",
              "-c", "4097", "/VERSION.json"])
    release = parse_release(raw)
    # Docker's observed .Image identity can use different store representations;
    # retain it exactly and separately retain the official immutable RepoDigests.
    return {"schema_version": 2, "state": "passed", "service": "aperturedb",
            "image_id": image_id, "official_database_repo_digests": official,
            "release": release}


def record(project, output, run=read):
    try:
        result = collect(project, run)
        code = 0
    except IdentityError as error:
        result = {"schema_version": 2, "state": "failed",
                  "service": "aperturedb", "reason": error.args[0]}
        code = 1
    except Exception:
        result = {"schema_version": 2, "state": "failed",
                  "service": "aperturedb", "reason": "identity_unavailable"}
        code = 1
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    temporary.replace(output)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    raise SystemExit(record(arguments.project, arguments.output))


if __name__ == "__main__":
    main()
