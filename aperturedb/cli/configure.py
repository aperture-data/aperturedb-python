import json
import os
from pathlib import Path

import typer
from typing_extensions import Annotated
from typing import Optional

from aperturedb.cli.console import console
from aperturedb.Configuration import Configuration
from aperturedb.CommonLibrary import _create_configuration_from_json


class ObjEncoder(json.JSONEncoder):
    """
    A bit of boiler plate to allow us to serialize our Configuration object.
    """

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
    file_exists = config_path.exists()
    if show_error:
        recommend = "Please run adb config create <name>"
        if not file_exists:
            console.log(f"{APP_NAME_CLI} is not configured yet. {recommend}")
    return file_exists


def get_configurations(file: str):
    configs = {}
    active = None
    with open(file) as config_file:
        configurations = json.loads(config_file.read())
        for c in filter(lambda key: key != "active", configurations):
            config = configurations[c]
            configs[c] = Configuration(
                name=c,
                host=config["host"],
                port=config["port"],
                username=config["username"],
                password=config["password"],
                use_rest=config["use_rest"],
                use_ssl=config["use_ssl"])
    active = configurations["active"]
    return configs, active


@app.command()
def ls(log_to_console: bool = True):
    """
    List the configurations with their details.
    """
    all_configs = {}
    for as_global in [True, False]:
        config_path = _config_file_path(as_global)
        context = "global" if as_global else "local"
        try:
            configs, active = get_configurations(config_path.as_posix())
            all_configs[context] = configs
            all_configs["active"] = active
        except FileNotFoundError:
            check_configured(as_global)
        except json.JSONDecodeError:
            check_configured(as_global)
            console.log(f"Failed to decode json '{config_path.as_posix()}'")

    if "global" in all_configs or "local" in all_configs:
        if "global" in all_configs and len(all_configs["global"]) == 0 \
                and "local" in all_configs and len(all_configs["local"]) == 0:
            console.log(
                f"No configurations found. Please run adb config create <name>")
            return
        else:
            if log_to_console:
                console.log(f"Available configurations:")
                console.log(all_configs)

    else:
        console.log(all_configs)
        # Tried to parse global config as well as local, but failed.
        # Show user the error and bail.
        check_configured(as_global=True, show_error=True)

    return all_configs


@app.command()
def create(
        name: Annotated[Optional[str], typer.Argument(
            help="Name of this configuration for easy reference")] = None,
        active: Annotated[bool, typer.Option(help="Set as active")] = False,
        as_global: Annotated[bool, typer.Option(
            help="Project level vs global level")] = True,
        host: Annotated[str, typer.Option(help="Host name")] = "localhost",
        port: Annotated[int, typer.Option(help="Port number")] = 55555,
        username: Annotated[str, typer.Option(help="Username")] = "admin",
        password: Annotated[str, typer.Option(help="Password")] = "admin",
        use_rest: Annotated[bool, typer.Option(help="Use REST")] = False,
        use_ssl: Annotated[bool, typer.Option(help="Use SSL")] = True,
        interactive: Annotated[bool, typer.Option(
            help="Interactive mode")] = True,
        overwrite: Annotated[bool, typer.Option(
            help="overwrite existing configuration")] = False,
        from_json: Annotated[bool, typer.Option(
            help="create config from a JSON string")] = False,
        from_key: Annotated[bool, typer.Option(help="create config from an encoded string")] = False):
    """
    Create a new configuration for the client.

    If --from-json or --from-key is used, then the options --host, --port, --username, --password, --use-rest, and --use-ssl will be ignored.
    The user will be prompted to enter the JSON string.
    This will be treated as a password entry.

    See https://docs.aperturedata.dev/Setup/client/configuration for more information on JSON configurations.
    """

    def check_for_overwrite(name):
        if name in configs and not overwrite:
            console.log(
                f"Configuration named '{name}' already exists. Use --overwrite to overwrite.",
                style="bold yellow")
            raise typer.Exit(code=2)

    db_host = host
    db_port = port
    db_username = username
    db_password = password
    db_use_rest = use_rest
    db_use_ssl = use_ssl

    config_path = _config_file_path(as_global)
    configs = {}
    try:
        configs, ac = get_configurations(config_path.as_posix())
    except FileNotFoundError as e:
        config_path: Path = _config_file_path(as_global)
        parent_dir = os.path.dirname(config_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        active = True
    except json.JSONDecodeError:
        active = True

    if name is not None:
        check_for_overwrite(name)

    if from_json:
        assert interactive, "Interactive mode must be enabled for --from-json"
        json_str = typer.prompt("Enter JSON string", hide_input=True)
        gen_config = _create_configuration_from_json(
            json_str, name=name, name_required=True)
        check_for_overwrite(gen_config.name)
        name = gen_config.name
    elif from_key:
        assert interactive, "Interactive mode must be enabled for --from-key"
        encoded_str = typer.prompt("Enter encoded string", hide_input=True)
        gen_config = Configuration.reinflate(encoded_str)
        name = gen_config.name

    else:
        if interactive:
            if name is None:
                name = typer.prompt(
                    "Enter configuration name", default=name)
                assert name is not None, "Configuration name must be specified"
                check_for_overwrite(name)
            db_host = typer.prompt(
                f"Enter {APP_NAME} host name", default=db_host)
            db_port = typer.prompt(
                f"Enter {APP_NAME} port number", default=db_port)
            db_username = typer.prompt(
                f"Enter {APP_NAME} username", default=db_username)
            db_password = typer.prompt(
                f"Enter {APP_NAME} password", hide_input=True, default=db_password)
            db_use_rest = typer.confirm(
                f"Use REST [Note: Only if ApertureDB is setup to receive HTTP requests]", default=db_use_rest)
            db_use_ssl = typer.confirm(
                f"Use SSL [Note: ApertureDB's defaults do not allow non SSL traffic]", default=db_use_ssl)

        gen_config = Configuration(
            name=name,
            host=db_host,
            port=db_port,
            username=db_username,
            password=db_password,
            use_ssl=db_use_ssl,
            use_rest=db_use_rest
        )

    assert name is not None, "Configuration name must be specified"
    configs[name] = gen_config
    if active:
        configs["active"] = name
    else:
        configs["active"] = ac

    with open(config_path.as_posix(), "w") as config_file:
        config_file.write(json.dumps(configs, indent=2, cls=ObjEncoder))


@app.command()
def activate(
        name: Annotated[str, typer.Argument(help="Name of this configuration for easy reference")],
        as_global: Annotated[bool, typer.Option(help="Project level vs global level")] = True):
    """
    Set the default configuration.
    """
    global_config_path = _config_file_path(True)
    gc, ga = get_configurations(global_config_path)

    config_path = _config_file_path(as_global)
    configs = {}
    try:
        configs, ac = get_configurations(config_path.as_posix())
        if name not in configs and name not in gc:
            console.log(f"Configuration {name} not found")
            raise typer.Exit(code=2)
        configs["active"] = name
    except FileNotFoundError:
        check_configured(as_global=False) or \
            check_configured(as_global=True, show_error=True)
    except json.JSONDecodeError:
        check_configured(as_global=False) or \
            check_configured(as_global=True, show_error=True)

    with open(config_path.as_posix(), "w") as config_file:
        config_file.write(json.dumps(configs, indent=2, cls=ObjEncoder))


@app.command()
def tokenize(name: Annotated[str, typer.Argument(
        help="Name of the configuration to tokenize")],
        as_global: Annotated[bool, typer.Option(help="Project level vs global level")] = True):
    """
    Makes a token from the configuration
    """
    global_config_path = _config_file_path(True)
    gc, ga = get_configurations(global_config_path)

    config_path = _config_file_path(as_global)
    configs = {}
    to_tokenize = None
    try:
        configs, ac = get_configurations(config_path.as_posix())
        if name not in configs and name not in gc:
            console.log(f"Configuration {name} not found")
            raise typer.Exit(code=2)
        to_tokenize = configs[name]
    except FileNotFoundError:
        check_configured(as_global=False) or \
            check_configured(as_global=True, show_error=True)
    except json.JSONDecodeError:
        check_configured(as_global=False) or \
            check_configured(as_global=True, show_error=True)

    print("{} Encoded: {}"
          .format(to_tokenize.name,  to_tokenize.deflate()))
