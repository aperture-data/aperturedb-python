from aperturedb.Utils import create_connector, Utils
import typer
from enum import Enum

app = typer.Typer()


class CommandTypes(str, Enum):
    SUMMARY = "summary"
    REMOVE_ALL = "remove_all"


@app.command()
def execute(command: CommandTypes):
    utils = Utils(create_connector())
    available_commands = {
        CommandTypes.SUMMARY: utils.summary,
        CommandTypes.REMOVE_ALL: utils.remove_all_objects
    }

    available_commands[command]()
