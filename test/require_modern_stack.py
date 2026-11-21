"""Fail before publication unless the actual stack is the qualified core build."""
import argparse
from pathlib import Path

from record_modern_stack import IdentityError, REFERENCE, object_json


CORE_COMMIT = "4f75b992572ffcf064b32195080be55fb4471881"
CORE_IMAGE = "aperturedata/aperturedb@sha256:2f348a70535aefba49ad105e6241c97f4c97ca86e121340f98907aa6a32e4107"
CORE_IDS = {"sha256:2f348a70535aefba49ad105e6241c97f4c97ca86e121340f98907aa6a32e4107",
            "sha256:8ea380eb838ba3a5a67edefe6e602a12ed6375baebece9a67779fe0ffc8a960d",
            "sha256:3f3088257713aed33d0d7c59e5febaef51483a122fed863ec9246705c0d27476"}


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
