import json
import os
from pathlib import Path

import typer
from typing_extensions import Annotated
from typing import Optional

from aperturedb.cli.console import console
import aperturedb.cli.keys as keys
from aperturedb.Configuration import Configuration
from aperturedb.CommonLibrary import _create_configuration_from_json, __create_connector
import re


# alnum for first character, then anum with - or _ for rest. Max Length 64
CONFIG_NAME_RE = re.compile(
    r'^([a-zA-Z0-9]([a-zA-Z0-9-_]){0,63})'
)


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


def _config_file_path(as_global: bool) -> Path:
    config_path: Path = Path(os.path.join(
        os.getcwd(),
        f".{APP_NAME}",
        f"{APP_NAME_CLI}.json"))
    if as_global:
        app_dir = typer.get_app_dir(APP_NAME)
        config_path: Path = Path(app_dir) / f"{APP_NAME_CLI}.json"
    return config_path


def _write_config(config_path: Path, config: dict):
    with open(config_path.as_posix(), "w") as config_file:
        config_file.write(json.dumps(config, indent=2, cls=ObjEncoder))


def has_environment_configuration():
    for known_variable in ["APERTUREDB_KEY", "APERTUREDB_JSON"]:
        if (data := os.environ.get(known_variable)) is not None and data != "":
            return True
    return False


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
                token=config["token"] if "token" in config else None,
                use_rest=config["use_rest"],
                use_ssl=config["use_ssl"],
                ca_cert=config.get("ca_cert", None),
                verify_hostname=config.get("verify_hostname", True))
            if "user_keys" in config:
                configs[c].set_user_keys(config["user_keys"])
    active = configurations["active"]
    return configs, active


def get_all_configs():
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

    for env_key, plain_json in [["APERTUREDB_JSON", True], ["APERTUREDB_KEY", False]]:
        if (data := os.environ.get(env_key)) is not None and data != "":
            if plain_json:
                config = _create_configuration_from_json(data)
            else:
                config = Configuration.reinflate(data)
            if not "environment" in all_configs:
                all_configs["environment"] = {}
            all_configs["environment"][env_key] = config
            all_configs["active"] = f"env:{env_key}"
    return all_configs


def get_active_config(all_configs):
    active = all_configs["active"]
    if active.startswith("env:"):
        return all_configs["environment"][active[4:]]
    else:
        return all_configs["local"][active] if "locan" in all_configs and active in all_configs["local"] \
            else all_configs["global"][active]


@app.command()
def ls(log_to_console: bool = True):
    """
    List the configurations with their details.
    """
    all_configs = get_all_configs()

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
        ca_cert: Annotated[Optional[str],
                           typer.Option(help="CA certificate")] = "",
        verify_hostname: Annotated[bool, typer.Option(
            help="Verify hostname")] = True,
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
    db_ca_cert = ca_cert
    db_verify_hostname = verify_hostname
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
        name = name if name is not None else gen_config.name
    elif from_key:
        assert interactive, "Interactive mode must be enabled for --from-key"
        encoded_str = typer.prompt("Enter encoded string", hide_input=True)
        gen_config = Configuration.reinflate(encoded_str)
        name = name if name is not None else gen_config.name

    else:
        if not CONFIG_NAME_RE.match(name):
            console.log(
                f"Configuration name {name} must be alphanumerical with dashes of 1-64 characters in length", style="bold yellow")
            raise typer.Exit(code=2)
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
            db_ca_cert = typer.prompt(
                f"Enter {APP_NAME} CA certificate's path (if custom CA is used)", default=db_ca_cert)
            if db_ca_cert != "":
                db_ca_cert = os.path.abspath(db_ca_cert)
                assert os.path.exists(
                    db_ca_cert), f"CA certificate file {db_ca_cert} does not exist"
            else:
                db_ca_cert = None
            db_verify_hostname = typer.confirm(
                f"Verify hostname", default=db_verify_hostname)

        gen_config = Configuration(
            name=name,
            host=db_host,
            port=db_port,
            username=db_username,
            password=db_password,
            use_ssl=db_use_ssl,
            use_rest=db_use_rest,
            ca_cert=db_ca_cert,
            verify_hostname=db_verify_hostname
        )

    assert name is not None, "Configuration name must be specified"
    configs[name] = gen_config
    if active:
        configs["active"] = name
    else:
        configs["active"] = ac

    _write_config(config_path, configs)


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

    _write_config(config_path, configs)


@app.command()
def remove(
        name: Annotated[Optional[str], typer.Argument(
            help="Name of this configuration to remove")],
        remove_if_active: Annotated[bool, typer.Option(
            help="If true; if active, remove and assign other configuration to be active; If false refuse to delete if active")] = False,
        new_active: Annotated[str, typer.Option(
            help="If deleting active, use name as new active")] = None,
        as_global: Annotated[bool, typer.Option(
            help="Project level vs global level")] = True):
    """
    Remove a configuration from a Configuriation file
    """
    config_path = _config_file_path(as_global)
    configs = {}
    config_level = "global" if as_global else "project"
    try:
        configs, ac = get_configurations(config_path.as_posix())
    except FileNotFoundError as e:
        console.log(
            f"No configuration available at {config_level} level.")
        raise typer.Exit(code=2)
    except json.JSONDecodeError:
        console.log(
            f"Configuration file at {config_level} level was malformed.")
        raise typer.Exit(code=2)

    if not name in configs:
        console.log(
            f"Configuration file at {config_level} level does not have a config with the name {name}.")
        raise typer.Exit(code=2)

    change_active = False
    if name == ac:
        change_active = True
        if new_active is None and not remove_if_active:
            console.log(
                f"Configuration {name} is active and no options for removing active were supplied.")
            raise typer.Exit(code=2)
        if new_active is not None and not new_active in configs:
            console.log(
                f"Configuration {new_active} does not exist in Configuration file at {config_level} and cannot be set active")
            raise typer.Exit(code=2)

    del configs[name]
    if change_active:
        if new_active:
            ac = new_active
        else:
            ac = next(iter(configs))
    configs["active"] = ac
    _write_config(config_path, configs)


@app.command()
def get_key(name: Annotated[str, typer.Argument(
        help="Name of the configuration to get a key for")] = None,
        user: Annotated[str, typer.Option(
            help="User to get a key for (default is config user)")] = None,
        as_global: Annotated[bool, typer.Option(help="Project level vs global level")] = True):
    """
    Makes a token from the configuration
    """

    config_path = _config_file_path(as_global)
    configs = {}
    user_key = None
    try:
        configs, active = get_configurations(config_path.as_posix())
        if not active and name is None:
            console.log(
                f"No configuration specified and no active configuration found")
            raise typer.Exit(code=2)
        if name is None:
            name = active
        if name not in configs and name not in gc:
            console.log(f"Configuration {name} not found")
            raise typer.Exit(code=2)
        configs["active"] = active

        if user is None:
            key_user = configs[name].username
        else:
            key_user = user

        if configs[name].has_user_keys():
            user_key = configs[name].get_user_key(key_user)

        if user_key is None:
            conn = __create_connector(configs[name])

            user_key = keys.generate_user_key(conn, key_user)
            configs[name].add_user_key(key_user, user_key)
            _write_config(config_path, configs)
    except FileNotFoundError:
        check_configured(as_global=False) or \
            check_configured(as_global=True, show_error=True)
    except json.JSONDecodeError:
        check_configured(as_global=False) or \
            check_configured(as_global=True, show_error=True)

    print(f"{user_key}")


@app.command()
def get(
        tag: Annotated[str, typer.Argument(help="Tag of information to get")],
        ignore_unset: Annotated[bool, typer.Option(help="ignore unsetl")] = False):
    """
    Retrieve detail of a config.
    """

    all_configs = get_all_configs()
    used_config = None
    remaining = ""
    if tag[0] == ".":
        used_config = get_active_config(all_configs)
        remaining = tag
    else:
        # match config name to config element seperator: .
        m = re.match(f"{CONFIG_NAME_RE.pattern}\\.", tag)
        if len(m.groups()) < 1 or len(m.groups(0)) == 0:
            console.log(f"Configuration name {tag} is invalid")
            raise typer.Exit(code=2)
        else:
            name = m.group(1)
            has_local = "local" in all_configs
            if (has_local and not name in all_configs["local"]) or \
                    not name in all_configs["global"]:
                console.log(f"No Configuration {name}")
                raise typer.Exit(code=2)
            else:
                used_config = all_configs["local"][name] \
                    if has_local and name in all_configs["local"] \
                    else all_configs["global"][name]
                remaining = tag[len(name):]

    if remaining[0] != ".":
        console.log(
            f"Cannot create configuration data to retrieve from {remaining}")
        raise typer.Exit(code=2)
    else:
        # match config name to end of string
        m = re.match(f"{CONFIG_NAME_RE.pattern}$", remaining[1:])
        if len(m.groups()) < 1 or len(m.groups(0)) == 0:
            console.log(f"Configuration item {remaining[1:]} is invalid")
            raise typer.Exit(code=2)
        else:
            config_item = m.group(0)
            print_ok = True

            # check if attribut exists or is valid to print.
            if not config_item in dir(used_config):
                print_ok = False
            else:
                attrib = getattr(used_config, config_item)
                # we allow only retreiving string, int or bool values from the
                # Configuration.
                allowed_types = [str, int, bool, type(None)]
                allowed_commands = ["auth_mode"]
                if config_item in allowed_commands:
                    attrib = attrib()
                elif not any([isinstance(attrib, allowed_type) for allowed_type in allowed_types]):
                    print_ok = False

            if print_ok:
                if attrib is None and not ignore_unset:
                    console.log(f"Configuration Item {config_item} is unset")
                    raise typer.Exit(code=2)
                else:
                    print(attrib)
            else:
                console.log(f"No Configuration Item {config_item}")
                raise typer.Exit(code=2)
