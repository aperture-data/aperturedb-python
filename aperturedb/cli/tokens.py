from enum import Enum
from typing import Annotated

import typer

from aperturedb.cli.console import console
from aperturedb.Configuration import Configuration

app = typer.Typer()


@app.command(help="List Tokens")
def list( user: Annotated[str, typer.Argument(help="The message to log")]):
    from aperturedb.CommonLibrary import create_connector
    conn = create_connector()
    res,_ = conn.query([{"GetUserDetails":{"username": user}}])
    utokens = res[0]['GetUserDetails']['tokens']
    if len(utokens) == 0:
        print(f"No Tokens for {user}")
    else:
        print(utokens)
    pass


@app.command(help="Create Token for a user")
def for_user(user: Annotated[str, typer.Argument(help="The user to generate a token for")]):
    from aperturedb.CommonLibrary import create_connector
    conn = create_connector()
    token = generate(False)
    assign(user,token)
    web_token = Configuration.create_web_token( conn.config.host, conn.config.port, token )
    print(f"User Web-token: {web_token}")

@app.command(help="Generate a Token")
def generate(silent: Annotated[bool, typer.Argument(help="Only return token, don't print it.")] = False):
    from aperturedb.CommonLibrary import create_connector
    token_cmd = [{"GenerateToken":{}}]
    conn = create_connector()
    r,_ = conn.query(token_cmd)
    created = r[0]["GenerateToken"]
    if not silent:
        print(f"Created Access Token: {created['token']}")
    return created['token']

@app.command(help="Assign a token to a user")
def assign( user: Annotated[str, typer.Argument(help="user to assign the token to")],
     token: Annotated[str, typer.Argument(help="Token")]):
    from aperturedb.CommonLibrary import create_connector
    token_cmd = [{"UpdateUser":{"username":user,"add_tokens":[token]}}]
    conn = create_connector()
    r,_ = conn.query(token_cmd)
    assinged =r[0]["UpdateUser"]
    if assinged["status"] == 0:
        print(f"Assigned token to {user}")
    else:
        print(f"Error assigning token: assigned['info']")

@app.command(help="Remove a token from a user")
def remove( user: Annotated[str, typer.Argument(help="User to remove a token from")],
     token: Annotated[str, typer.Argument(help="Token")]):
    from aperturedb.CommonLibrary import create_connector
    token_cmd = [{"UpdateUser":{"username":user,"remove_tokens":[token]}}]
    conn = create_connector()
    r,_ = conn.query(token_cmd)
    removed =r[0]["UpdateUser"]
    if removed["status"] == 0:
        print("Action complete")
    else:
        print(f"Error removing token: removed['info']")
