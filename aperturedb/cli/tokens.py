from enum import Enum
from typing import Annotated

import typer

from aperturedb.cli.console import console
from aperturedb.CommonLibrary import create_connector, execute_query
from aperturedb.Utils import Utils

app = typer.Typer()


@app.command(help="List User Authentication Tokens")
def list(user: Annotated[str, typer.Argument(help="The user the display tokens for")]):
    token_list_query = [{"GetUserDetails": {"username": user}}]
    client = create_connector()
    result, response, blobs = execute_query(
        client=client,
        query=token_list_query,
        blobs=[])
    utokens = response[0]['GetUserDetails']['tokens']
    if len(utokens) == 0:
        console.log(f"No Tokens for {user}")
    else:
        console.log(utokens)


@app.command(help="Generate an Authentication token for a user")
def generate():
    conn = create_connector()
    u = Utils(conn)
    token = u.generate_token()
    print(f"{token}")
    return token


@app.command(help="Assign an Authentication token to a user")
def assign(user: Annotated[str, typer.Argument(help="user to assign the token to")],
           token: Annotated[str, typer.Argument(help="Token to be assigned")]):
    conn = create_connector()
    u = Utils(conn)
    try:
        u.assign_token(user, token)
        console.log(f"Assigned token to {user}")
    except Exception as e:
        console.log(f"Failed to assign token: {e}", style="red")


@app.command(help="Remove an Authentication token from a user")
def remove(user: Annotated[str, typer.Argument(help="User to remove a token from")],
           token: Annotated[str, typer.Argument(help="Token to be removed")]):
    conn = create_connector()
    u = Utils(conn)
    try:
        u.remove_token(user, token)
        console.log("Action complete")
    except Exception as e:
        console.log(f"Failed to remove token: {e}", style="red")
