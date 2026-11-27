import time
from typing import List
import requests
import os
import re
import sys

import datetime
from .status_tools import StatusUpdater
from typer import Typer
import subprocess


class LogProcessor:
    def __init__(self, process: any):
        self.process = process
        self.status_updater = StatusUpdater()

    def process_logs(self):
        # Continuously read from stderr
        last_time = time.time()
        completed = 100.0
        for line in iter(self.process.stderr.readline, ''):
            msg = line.strip()
            need_update = False

            if msg.startswith("Progress:"):
                match_percent = re.search(r"Progress:\s*(\d+(?:\.\d+)?)%", msg)
                if match_percent:
                    # handle normal format
                    completed = float(match_percent.group(1))
                    need_update = True
                elif re.search(r"Progress:\s*\d+(?:\.\d+)?items\s*\[", msg):
                    # zero-item format - ignore
                    pass
                else:
                    raise ValueError(f"Unexpected progress format: {msg}")

            # print("Progress:", line.strip(), flush=True)
            # update status if progress is updated (rate limited 1 update/s) or if the process is completed
            if need_update and (time.time() - last_time >= 1 or completed == 100.0):
                print(f">>[{datetime.datetime.now().isoformat()}]",
                      msg, flush=True)
                self.status_updater.post_update(completed=completed)
                last_time = time.time()
            else:
                timestamp = datetime.datetime.now().isoformat()
                print(f"[{timestamp}]{msg}", flush=True)

        # Wait for the process to complete
        self.process.wait()


app = Typer()


@app.command()
def run_monitored(command: str):
    process = subprocess.Popen(
        command, shell=True, stderr=subprocess.PIPE, text=True)
    lp = LogProcessor(process)
    lp.process_logs()

    while process.poll() is None:
        # Process hasn't exited yet, let's wait some
        time.sleep(0.5)

    if process.returncode != 0:
        sys.exit(process.returncode)


if __name__ == "__main__":
    app()
