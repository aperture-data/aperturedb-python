"""Verify the running database against the immutable image selected by Compose."""
import argparse
from pathlib import Path

from record_modern_stack import DIGEST, IdentityError, REFERENCE, object_json, parse_release, read, require


def validate(receipt, expected_image, run=read, *, expected_branch=None):
    require(REFERENCE.fullmatch(expected_image), "invalid_expected_image")
    require(set(receipt) == {"schema_version", "state", "service", "image_id",
                             "official_database_repo_digests", "release"}
            and type(receipt["schema_version"]) is int and receipt["schema_version"] == 2
            and receipt["state"] == "passed" and receipt["service"] == "aperturedb",
            "qualified_modern_stack_mismatch")
    references = receipt["official_database_repo_digests"]
    require(isinstance(references, list) and 1 <= len(references) <= 32
            and all(isinstance(ref, str) and REFERENCE.fullmatch(ref) for ref in references)
            and references == sorted(set(references)) and expected_image in references,
            "qualified_modern_stack_mismatch")
    expected_id = run(["docker", "image", "inspect",
                      "--format", "{{.Id}}", expected_image])
    require(DIGEST.fullmatch(expected_id) and receipt["image_id"] == expected_id,
            "qualified_modern_stack_mismatch")
    release = parse_release(run(["docker", "run", "--rm", "--pull=never", "--network", "none",
                                 "--read-only", "--entrypoint", "head", expected_image,
                                 "-c", "4097", "/VERSION.json"]))
    require(receipt["release"] == release and int(release["version"].split(".")[0]) >= 1,
            "qualified_modern_stack_mismatch")
    if expected_branch is not None:
        require(release["branch"] == expected_branch,
                "unexpected_server_branch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--expected-image", required=True)
    parser.add_argument("--expected-branch", choices=("main", "develop"))
    args = parser.parse_args()
    try:
        with args.receipt.open("rb") as stream:
            raw = stream.read(16385)
        require(len(raw) <= 16384, "qualified_modern_stack_mismatch")
        validate(object_json(raw), args.expected_image,
                 expected_branch=args.expected_branch)
    except (IdentityError, OSError):
        raise SystemExit("Qualified modern stack identity mismatch") from None


if __name__ == "__main__":
    main()
