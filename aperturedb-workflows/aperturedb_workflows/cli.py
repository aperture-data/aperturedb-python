"""Console entry points for the packaged shell runtime."""
import os
import sys
from importlib.resources import files


def start():
    """Start the status server, configure the database and run /app/app.sh."""
    script = files(__package__) / "start.sh"
    os.execvp("bash", ["bash", str(script), *sys.argv[1:]])
