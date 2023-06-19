import json
import os
from json import JSONEncoder
from pathlib import Path
from typing import Union

import typer
from typing_extensions import Annotated

from aperturedb.cli.console import console
from aperturedb.Configuration import Configuration


class ObjEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__

APP_FULL_NAME = "CLI for aperturedb"
APP_NAME = "aperturedb"
APP_NAME_CLI = "adb"

app = typer.Typer()

def _config_file_path(as_global: bool):
    config_path: Path = Path(os.path.join(
         os.getcwd(),
         f".{APP_NAME}",
         f"{APP_NAME_CLI}.json"))
    if as_global:
        app_dir = typer.get_app_dir(APP_NAME)
        config_path: Path = Path(app_dir) / f"{APP_NAME_CLI}.json"
    return config_path

def check_configured(as_global: bool, show_error: bool = False):
    config_path = _config_file_path(as_global)
    if show_error:
        recommend = "Please run adb config create <name>"
        if not config_path.is_file():
            console.log(f"{APP_NAME_CLI} is not configured yet. {recommend}")

def get_configurations(file: str):
    configs = {}
    with open(file) as config_file:
        configurations = json.loads(config_file.read())
        for c in configurations:
            config = configurations[c]
            configs[c] = Configuration(
            name=c,
            host=config["host"],
            port=config["port"],
            username=config["username"],
            password=config["password"])
    return configs

@app.command()
def ls(name: Annotated[Union[str, None], typer.Argument(help="Name of configuration to get")] = None):
    """
    List the configurations with their details.
    """
    all_configs = {}
    for as_global in [True, False]:
        config_path = _config_file_path(as_global)
        try:
            configs = get_configurations(config_path.as_posix())
            all_configs["global" if as_global else "local"] = configs
        except FileNotFoundError:
            check_configured(as_global)
        except json.JSONDecodeError:
            check_configured(as_global)

    if len(all_configs["global"]) == 0 and len(all_configs["local"]) == 0:
        console.log(f"No configurations found. Please run adb config create <name>")
        return
    else:
        if name is None:
            console.log(all_configs)
        else:
            config = all_configs["global"][name] if name in all_configs["global"] else all_configs["local"][name]
            console.log(config)

@app.command()
def create(
        name: Annotated[str, typer.Argument(help="Name of this configuration for easy reference")],
        active: Annotated[bool, typer.Option(help="Set as active")] = False,
        as_global: Annotated[bool, typer.Option(help="Project level vs global level")] = True):
    """
    Create a new configuration for the client.
    """
    db_host = "localhost"
    db_port = 55555
    db_username = "admin"
    db_password = "admin"
    config_path = _config_file_path(as_global)
    configs = {}
    try:
        configs = get_configurations(config_path.as_posix())
    except FileNotFoundError as e:
        config_path: Path = _config_file_path(as_global)
        parent_dir = os.path.dirname(config_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        active = True
    except json.JSONDecodeError:
        active = True


    db_host = typer.prompt(f"Enter {APP_NAME} host name", default=db_host)
    db_port = typer.prompt(f"Enter {APP_NAME} port number", default=db_port)
    db_username = typer.prompt(f"Enter {APP_NAME} username", default=db_username)
    db_password = typer.prompt(f"Enter {APP_NAME} password", hide_input=True, default=db_password)

    gen_config = Configuration(
        name=name,
        host=db_host,
        port=db_port,
        username=db_username,
        password=db_password
    )
    configs[name] = gen_config
    if active:
        configs["active"] = gen_config

    with open(config_path.as_posix(), "w") as config_file:
        config_file.write(json.dumps(configs, indent=2, cls=ObjEncoder))

@app.command()
def activate(
    name: Annotated[str, typer.Argument(help="Name of this configuration for easy reference")],
    as_global: Annotated[bool, typer.Option(help="Project level vs global level")] = True):
    """
    Set the default configuration.
    """
    config_path = _config_file_path(as_global)
    configs = {}
    try:
        configs = get_configurations(config_path.as_posix())
        if name not in configs:
            console.log(f"Configuration {name} not found")
            return
        configs["active"] = configs[name]
    except FileNotFoundError:
        check_configured()
    except json.JSONDecodeError:
        check_configured()

    with open(config_path.as_posix(), "w") as config_file:
        config_file.write(json.dumps(configs, indent=2, cls=ObjEncoder))
