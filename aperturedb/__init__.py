#!/usr/bin/env python3
import logging
import datetime
import os
import json
import requests

logger = logging.getLogger(__name__)

__version__ = "0.3.5"

# set log level
logger.setLevel(logging.DEBUG)
formatter    = logging.Formatter(
    "%(asctime)s : %(levelname)s : %(name)s : %(thread)d : %(lineno)d : %(message)s")

log_file_level = logging.getLevelName(os.getenv("LOG_FILE_LEVEL", "WARN"))
log_console_level = logging.getLevelName(
    os.getenv("LOG_CONSOLE_LEVEL", "ERROR"))

# define file handler and set formatter
error_file_handler = logging.FileHandler(
    f"error.{datetime.datetime.now().isoformat()}.log", delay=True)
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
    print(
        f"The latest version of aperturedb is {latest_version}. You are using version {__version__}. It is recommended to upgrade.")
