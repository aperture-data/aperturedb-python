from enum import Enum

import typer

from aperturedb.Utils import Utils, create_connector
from aperturedb.cli.console import console

app = typer.Typer()


class CommandTypes(str, Enum):
    SUMMARY = "summary"
    REMOVE_ALL = "remove_all"


def confirm(command: CommandTypes):
    console.print("Danger", style="bold red")
    console.log(f"This will execute {command}.")
    response = typer.prompt("Are you sure you want to continue? [y/N]")
    if response.lower() != "y":
        typer.echo("Aborting...")
        raise typer.Abort()


@app.command()
def execute(command: CommandTypes):
    utils = Utils(create_connector())
    available_commands = {
        CommandTypes.SUMMARY: utils.summary,
        CommandTypes.REMOVE_ALL: lambda: confirm(
            CommandTypes.REMOVE_ALL) and utils.remove_all_objects
    }

    available_commands[command]()
