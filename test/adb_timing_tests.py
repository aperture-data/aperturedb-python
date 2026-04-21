import os
from datetime import datetime
if __name__ == '__main__':
    for command in [
        "adb config create aperturedb1 --host aperturedb --port 5555 --no-interactive --overwrite",
        "adb config create aperturedb2 --host aperturedb --port 5555 --no-interactive --overwrite",
        "adb config ls",
        "adb config activate aperturedb2",

    ]:
        print(command)
        start = datetime.now()
        os.system(command)
        diff = datetime.now() - start
        print(diff)
        assert diff.total_seconds() <= 1.5, f"Command {command} took too long"
