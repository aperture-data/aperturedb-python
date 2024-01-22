from enum import Enum
from typing import Annotated

import typer

from aperturedb.Utils import Utils, create_connector
from aperturedb.cli.console import console

app = typer.Typer()


class CommandTypes(str, Enum):
    STATUS = "status"
    SUMMARY = "summary"
    REMOVE_ALL = "remove_all"
    REMOVE_INDEXES = "remove_indexes"


def confirm(command: CommandTypes, force: bool):
    if force:
        return True
    console.print("Danger", style="bold red")
    console.log(f"This will execute {command}.")
    response = typer.prompt("Are you sure you want to continue? [y/N]")
    if response.lower() != "y":
        typer.echo("Aborting...")
        raise typer.Abort()
    return True


@app.command()
def execute(command: CommandTypes,
            force: Annotated[bool, typer.Option(help="Do not confirm")] = False):
    utils = Utils(create_connector())
    available_commands = {
        CommandTypes.STATUS: lambda: print(utils),
        CommandTypes.SUMMARY: utils.summary,
        CommandTypes.REMOVE_ALL: lambda: confirm(
            CommandTypes.REMOVE_ALL, force) and utils.remove_all_objects(),
        CommandTypes.REMOVE_INDEXES: lambda: confirm(
            CommandTypes.REMOVE_INDEXES, force) and utils.remove_all_indexes(),
    }

    available_commands[command]()
