"""Fail before publication unless the actual stack is the qualified core build."""
import argparse
from pathlib import Path

from record_modern_stack import IdentityError, REFERENCE, object_json


CORE_COMMIT = "24c85fc7be26ba458597b0e612f9320aedfa6871"
CORE_IMAGE = "aperturedata/aperturedb@sha256:7c8691279db1439fae54163ae36ad993e8273f6023e853f0e9ccc2c11da3b4c2"
CORE_IDS = {"sha256:7c8691279db1439fae54163ae36ad993e8273f6023e853f0e9ccc2c11da3b4c2",
            "sha256:25bac6ca0943c4c6baa4648a4e61d4ea4f88088b052a9f10fb45c4c41e07dd02",
            "sha256:56dd877691ab2a81bf2fdb3193b69f993fb1206f65521f452b7756a00e6cf0d8"}


def validate(receipt):
    valid = (isinstance(receipt, dict)
             and set(receipt) == {"schema_version", "state", "service", "image_id",
                                  "official_database_repo_digests", "release"}
             and type(receipt["schema_version"]) is int and receipt["schema_version"] == 2
             and receipt["state"] == "passed" and receipt["service"] == "aperturedb"
             and isinstance(receipt["image_id"], str) and receipt["image_id"] in CORE_IDS
             and receipt["release"] == {"version": "1.0.0", "branch": "main", "commit": CORE_COMMIT})
    if valid:
        references = receipt["official_database_repo_digests"]
        valid = (isinstance(references, list) and 1 <= len(references) <= 32
                 and all(isinstance(value, str) and REFERENCE.fullmatch(value) for value in references)
                 and references == sorted(set(references)) and CORE_IMAGE in references)
    if not valid:
        raise IdentityError("qualified_modern_stack_mismatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    try:
        with args.receipt.open("rb") as stream:
            raw = stream.read(16385)
        if len(raw) > 16384:
            raise IdentityError("qualified_modern_stack_mismatch")
        validate(object_json(raw))
    except Exception:
        raise SystemExit("Qualified modern stack identity mismatch") from None


if __name__ == "__main__":
    main()
