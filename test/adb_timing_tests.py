import subprocess
from time import monotonic


def check_cli_timing():
    options = ["--ca-cert", "/ca/ca.crt", "--no-interactive", "--overwrite"]
    commands = [
        ["create", "sdk_native", "--host", "lenz", "--port", "55551", *options],
        ["create", "sdk_https", "--host", "http-proxy",
            "--port", "8443", "--use-rest", *options],
        ["ls"],
        ["activate", "sdk_native"],
    ]
    for args in commands:
        command = ["adb", "config", *args]
        start = monotonic()
        subprocess.run(command, check=True)
        elapsed = monotonic() - start
        assert elapsed <= 2.5, f"{command} took {elapsed:.2f} seconds"


if __name__ == "__main__":
    check_cli_timing()
