#!/usr/bin/env python3
import logging
import datetime
import os
import json
import requests
from string import Template
import platform
import faulthandler
import signal
import sys

__version__ = "0.4.45"

logger = logging.getLogger(__name__)

# https://docs.python.org/3/library/faulthandler.html
# Register SIGUSR1 to dump the stack trace
# Good for debugging a running process

if os.getenv("ADB_DEBUGGABLE", None) != None:
    if sys.platform == "win32":
        logger.warn("Unable to configure debugging support for win32")
    else:
        faulthandler.register(signal.SIGUSR1.value)

# set log level
formatter = logging.Formatter(
    "%(asctime)s : %(levelname)s : %(name)s : %(thread)d : %(lineno)d : %(message)s")

log_file_level = logging.getLevelName(os.getenv("LOG_FILE_LEVEL", "WARN"))
log_console_level = logging.getLevelName(
    os.getenv("LOG_CONSOLE_LEVEL", "ERROR"))

# Set the logger filter to the minimum (more chatty) of the two handler levels
# This reduces problems if the environment adds a root handler (e.g. Google Colab)
logger_level = min(log_file_level, log_console_level)
if any(log_control in os.environ
       for log_control in ["LOG_CONSOLE_LEVEL", "LOG_FILE_LEVEL"]):
    logger.setLevel(logger_level)

# define file handler and set formatter
error_file_name = "error.${now}.log"

if "ADB_LOG_FILE" in os.environ:
    error_file_name = None if len(
        os.environ["ADB_LOG_FILE"]) == 0 else os.environ["ADB_LOG_FILE"]

if error_file_name is not None:
    error_file_tmpl = Template(error_file_name)
    template_items = {
        # python isodate has ':', not valid in files in windows.
        "now": str(datetime.datetime.now().isoformat()).replace(':', ''),
        "node": str(platform.node())
    }
    error_file_handler = logging.FileHandler(error_file_tmpl.safe_substitute(
        **template_items), delay=True)
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(log_file_level)
    logger.addHandler(error_file_handler)

error_console_handler = logging.StreamHandler()
error_console_handler.setLevel(log_console_level)
error_console_handler.setFormatter(formatter)
logger.addHandler(error_console_handler)

try:
    latest_version = json.loads(requests.get(
        "https://pypi.org/pypi/aperturedb/json").text)["info"]["version"]
except Exception as e:
    logger.warning(
        f"Failed to get latest version: {e}. You are using version {__version__}")
    latest_version = None
if __version__ != latest_version:
    logger.warning(
        f"The latest version of aperturedb is {latest_version}. You are using version {__version__}. It is recommended to upgrade.")
