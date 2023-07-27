import json
import typer
from aperturedb.ParallelQuery import execute_batch
from aperturedb.cli.console import console
from aperturedb.Utils import create_connector

app = typer.Typer()


@app.command()
def from_json_file(filepath: str):
    db = create_connector()
    with open(filepath) as inputstream:
        transaction = json.loads(inputstream.read())
        result, response, blobs = execute_batch(
            db=db,
            q=transaction,
            blobs=[])
        console.log(result)
        console.log(response)
