import json

import typer

app = typer.Typer()


@app.command()
def from_json_file(filepath: str):
    with open(filepath) as inputstream:
        transaction = json.loads(inputstream.read())
