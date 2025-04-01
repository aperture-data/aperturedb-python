"""
Common library functions for ApertureDB.
This will not have a big class structure, but rather a collection of functions
This is the place to put functions that are reused in codebase.
"""
import importlib
import math
import os
import sys
from typing import Any, Callable, Optional, Tuple, Dict, Union
import logging
import json

from aperturedb.Configuration import Configuration
from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb.types import Blobs, CommandResponses, Commands

logger = logging.getLogger(__name__)


def import_module_by_path(filepath: str) -> Any:
    """
    This function imports a module given a path to a python file.
    """
    module_name = os.path.basename(filepath)[:-3]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def __create_connector(configuration: Configuration):
    if configuration.use_rest:
        connector = ConnectorRest(
            host=configuration.host,
            port=configuration.port,
            user=configuration.username,
            password=configuration.password,
            use_ssl=configuration.use_ssl,
            config=configuration)
    else:
        connector = Connector(
            host=configuration.host,
            port=configuration.port,
            user=configuration.username,
            password=configuration.password,
            use_ssl=configuration.use_ssl,
            config=configuration)
    logger.debug(
        f"Created connector using: {configuration}. Will connect on query.")
    return connector


def _create_configuration_from_json(config: Union[Dict, str],
                                    name: Optional[str] = None, name_required: bool = False) -> Connector:
    """
    **Create a connector to the database from a JSON configuration.**

    Args:
        config (Dict or str): The configuration.

    Returns:
        Connector: The connector to the database.
    """
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError as e:
            # Cannot print the JSON string because it may contain a password.
            assert False, f"problem decoding JSON config string: {e}"
    assert isinstance(
        config, dict), f"config must be a dict or a JSON object string: {type(config)}"

    # Remove the password from the configuration before logging.
    clean_config = {k: v for k, v in config.items() if k != "password"}

    # These fields are required.
    assert "host" in config, f"host is required in the configuration: {clean_config}"
    assert "username" in config, f"username is required in the configuration: {clean_config}"
    assert "password" in config, f"password is required in the configuration: {clean_config}"

    # These fields have no default in the Configuration class.
    if 'port' not in config:
        config["port"] = 55555

    if name is not None:
        config["name"] = name  # will overwrite the name in the config

    if name_required:
        assert "name" in config, f"name is required in the configuration: {clean_config}"
    elif 'name' not in config:
        config["name"] = "from_json"

    configuration = Configuration(**config)
    return configuration


def _get_colab_secret(name: str) -> Optional[str]:
    try:
        from google.colab import userdata
        return userdata.get(name)
    except ImportError:  # Not in Colab environment
        return None
    except AttributeError:  # In Colab environment but not in a notebook
        logger.debug(
            "In Colab environment but not in a notebook. Cannot read secrets.")
        return None
    except userdata.NotebookAccessError:  # Permission to access secrets not granted
        logger.debug(
            "Permission to access secrets not granted to this notebook.")
        return None
    except userdata.SecretNotFoundError:  # This secret does not exist
        logger.debug(
            f"Secret '{name}' not found in Google Colab.")
        return None
    except Exception as e:  # Unexpected error
        logger.error(
            f"Unexpected error while reading secret '{name}' from Google Colab: {e}")
        return None


def _get_dotenv_secret(name: str) -> Optional[str]:
    try:
        from dotenv import dotenv_values
        config = dotenv_values(".env")
        return config.get(name)
    except ImportError:  # dotenv not installed
        logger.warning(
            "dotenv not installed. Cannot read secrets from .env file.")
        return None


def _store_config(config: Configuration, name: str):
    logger.info(
        f"Storing and activating configuration '{name}' in the global configuration: {config}")

    from aperturedb.cli.configure import create
    create(
        name=name,
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        use_ssl=config.use_ssl,
        use_rest=config.use_rest,
        interactive=False,
        overwrite=True,
        active=True,
    )


def create_connector(
    name: Optional[str] = None,
    key: Optional[str] = None,
    create_config_for_colab_secret=True,
) -> Connector:
    """
    **Create a connector to the database.**

    This function chooses a configuration in the folowing order:
    1. The configuration named by the `name` parameter or `key` parameter
    2. The configuration described in the `APERTUREDB_KEY` environment variable.
    3. The configuration described in the `APERTUREDB_JSON` environment variable.
    4. The configuration described in the `APERTUREDB_JSON` Google Colab secret.
    5. The configuration described in the `APERTUREDB_JSON` secret in a `.env` file.
    6. The configuration named by the `APERTUREDB_CONFIG` environment variable.
    7. The active configuration.

    If there are both global and local configurations with the same name, the global configuration is preferred.

    See :ref:`adb config <adb_config>`_ command-line tool for more information.

    Arguments:
        name (str, optional): The name of the configuration to use. Default is None.
        create_config_for_colab_secret (bool, optional): Whether to create a configuration from the Google Colab secret. Default is True.

    Returns:
        Connector: The connector to the database.

    Note about Google Colab secret: This secret is available in the context of a notebook running on Google Colab.  In particular, it is not available to the `adb` CLI tool running in a Colab notebook or any scripts run within a notebook.  To resolve this issue, a configuration is automatically created and activated in this case.  Use the `create_config_for_colab_secret` parameter to disable this behavior.
    """
    from aperturedb.cli.configure import ls
    _all_configs = None

    def get_all_configs():
        nonlocal _all_configs
        if _all_configs is None:
            _all_configs = ls(log_to_console=False)
        return _all_configs

    def lookup_config_by_name(name: str, source: str) -> Configuration:
        configs = get_all_configs()

        if "global" in configs and name in configs["global"]:
            return configs["global"][name]
        if "local" in configs and name in configs["local"]:
            return configs["local"][name]
        assert False, f"Configuration '{name}' not found ({source})."

    if key is not None:
        if name is not None:
            raise ValueError(
                "Specify only name or key when creating a connector")
        logger.info(f"Using configuration from key parameter")
        config = Configuration.reinflate(key)
    elif name is not None:
        logger.info(f"Using configuration '{name}' explicitly")
        config = lookup_config_by_name(name, "explicit")
    elif (data := os.environ.get("APERTUREDB_KEY")) is not None and data != "":
        logger.info(
            f"Using configuration from APERTUREDB_KEY environment variable")
        config = Configuration.reinflate(data)
    elif (data := os.environ.get("APERTUREDB_JSON")) is not None and data != "":
        logger.info(
            f"Using configuration from APERTUREDB_JSON environment variable")
        config = _create_configuration_from_json(data)
    elif (data := _get_colab_secret("APERTUREDB_JSON")) is not None and data != "":
        logger.info(
            f"Using configuration from APERTUREDB_JSON Google Colab secret")
        config = _create_configuration_from_json(data)
        if create_config_for_colab_secret:
            logger.info(
                f"Creating and activating configuration from APERTUREDB_JSON Google Colab secret")
            _store_config(config, 'google_colab')
    elif (data := _get_dotenv_secret("APERTUREDB_JSON")) is not None and data != "":
        logger.info(
            f"Using configuration from APERTUREDB_JSON secret in .env file")
        config = _create_configuration_from_json(data)
    elif (name := os.environ.get("APERTUREDB_CONFIG")) is not None and name != "":
        logger.info(
            f"Using configuration '{name}' from APERTUREDB_CONFIG environment variable")
        config = lookup_config_by_name(name, "envar")
    elif "active" in get_all_configs():
        name = get_all_configs()["active"]
        config = lookup_config_by_name(name, "active")
        logger.info(f"Using active configuration '{name}'")
    else:
        assert False, "No configuration found."
    logger.info(f"Configuration: {config}")
    return __create_connector(config)


def execute_query(client: Connector, query: Commands,
                  blobs: Blobs = [],
                  success_statuses: list[int] = [0],
                  response_handler: Optional[Callable] = None, commands_per_query: int = 1, blobs_per_query: int = 0,
                  strict_response_validation: bool = False, cmd_index=None) -> Tuple[int, CommandResponses, Blobs]:
    """
    Execute a batch of queries, doing useful logging around it.
    Calls the response handler if provided.

    This should be used (without the parallel machinery) instead of
    Connector.query to keep the response handling consistent, better logging, etc.

    Args:
        client (Connector): The database connector.
        query (Commands): List of commands to execute.
        blobs (Blobs, optional): List of blobs to send.
        success_statuses (list[int], optional): The list of success statuses. Defaults to [0].
        response_handler (Callable, optional): The response handler. Defaults to None.
        commands_per_query (int, optional): The number of commands per query. Defaults to 1.
        blobs_per_query (int, optional): The number of blobs per query. Defaults to 0.
        strict_response_validation (bool, optional): Whether to strictly validate the response. Defaults to False.

    Returns:
        int: The result code.
            - 0 : if all commands succeeded
            - 1 : if there was -1 in the response
            - 2 : For any other code.
        CommandResponses: The response.
        Blobs: The blobs.
    """
    result = 0
    logger.debug(f"Query={query}")
    r, b = client.query(query, blobs)
    logger.debug(f"Response={r}")

    if client.last_query_ok():
        if response_handler is not None:
            try:
                map_response_to_handler(response_handler,
                                        query, blobs, r, b, commands_per_query, blobs_per_query,
                                        cmd_index)
            except BaseException as e:
                logger.exception(e)
                if strict_response_validation:
                    raise e
    else:
        # Transaction failed entirely.
        logger.error(f"Failed query = {query} with response = {r}")
        result = 1

    statuses = {}
    if isinstance(r, dict):
        statuses[r['status']] = [r]
    elif isinstance(r, list):
        # add each result to a list of the responses, keyed by the response
        # code.
        [statuses.setdefault(result[cmd]['status'], []).append(result)
         for result in r for cmd in result]
    else:
        logger.error("Response in unexpected format")
        result = 1

    # last_query_ok means result status >= 0
    if result != 1:
        warn_list = []
        for status, results in statuses.items():
            if status not in success_statuses:
                for wr in results:
                    warn_list.append(wr)
        if len(warn_list) != 0:
            logger.warning(
                f"Partial errors:\r\n{json.dumps(query)}\r\n{json.dumps(warn_list)}")
            result = 2

    return result, r, b


def map_response_to_handler(handler, query, query_blobs,  response, response_blobs,
                            commands_per_query, blobs_per_query, cmd_index_offset):
    # We could potentially always call this handler function
    # and let the user deal with the error cases.
    blobs_returned = 0
    for i in range(math.ceil(len(query) / commands_per_query)):
        start = i * commands_per_query
        end = start + commands_per_query
        blobs_start = i * blobs_per_query
        blobs_end = blobs_start + blobs_per_query

        b_count = 0
        if issubclass(type(response), list):
            for req, resp in zip(query[start:end], response[start:end]):
                for k in req:
                    blob_returning_commands = ["FindImage", "FindBlob", "FindVideo",
                                               "FindDescriptor", "FindBoundingBox"]
                    if k in blob_returning_commands and "blobs" in req[k] and req[k]["blobs"]:
                        count = resp[k]["returned"]
                        b_count += count

        # The returned blobs need to be sliced to match the
        # returned entities per command in query.
        handler(
            query[start:end],
            query_blobs[blobs_start:blobs_end],
            response[start:end] if issubclass(
                type(response), list) else response,
            response_blobs[blobs_returned:blobs_returned + b_count] if
            len(response_blobs) >= blobs_returned + b_count else None,
            None if cmd_index_offset is None else cmd_index_offset + i)
        blobs_returned += b_count


def issue_deprecation_warning(old_name, new_name):
    """
    Issue a deprecation warning for a function and class.
    """
    logger.warning(
        f"{old_name} is deprecated and will be removed in a future release. Use {new_name} instead.")
