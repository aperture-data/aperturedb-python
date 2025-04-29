from enum import Enum
from typing import Annotated

import typer

from aperturedb.cli.console import console
from aperturedb.Configuration import Configuration
from aperturedb.CommonLibrary import create_connector
from aperturedb.Connector import Connector

app = typer.Typer()


@app.command(help="List User Authentication Tokens")
def list(user: Annotated[str, typer.Argument(help="The user the display tokens for")]):
    conn = create_connector()
    res,_ = conn.query([{"GetUserDetails":{"username": user}}])
    utokens = res[0]['GetUserDetails']['tokens']
    if len(utokens) == 0:
        print(f"No Tokens for {user}")
    else:
        print(utokens)

@app.command(help="Generate an Authentication token for a user")
def generate(silent: Annotated[bool, typer.Argument(help="Only return token, don't print it.")] = False):
    conn = create_connector()
    token = generate_token(conn)
    if not silent:
        print(f"Created Access Token: {token}")
    return token

def generate_token(conn:Connector):
    token_cmd = [{"GenerateToken":{}}]
    r,_ = conn.query(token_cmd)
    created = r[0]["GenerateToken"]
    return created['token']

@app.command(help="Assign an Authentication token to a user")
def assign( user: Annotated[str, typer.Argument(help="user to assign the token to")],
     token: Annotated[str, typer.Argument(help="Token to be assigned")]):
    conn = create_connector()
    try:
        assign_token(conn,user,token)
        print(f"Assigned token to {user}")
    except Exception as e:
        print(f"Failed to assign token: {e}")

def assign_token( conn:Connector, user:str, token:str):
    token_cmd = [{"UpdateUser":{"username":user,"add_tokens":[token]}}]
    r,_ = conn.query(token_cmd)
    if isinstance(r,dict):
        raise Exception(f"Error assigning token: {r}")
    assigned = r[0]["UpdateUser"]
    if assigned["status"] != 0:
        raise Exception(f"Error assigning token: Non-zero status: {assigned['status']}")

@app.command(help="Remove an Authentication token from a user")
def remove( user: Annotated[str, typer.Argument(help="User to remove a token from")],
     token: Annotated[str, typer.Argument(help="Token to be removed")]):
    from aperturedb.CommonLibrary import create_connector
    token_cmd = [{"UpdateUser":{"username":user,"remove_tokens":[token]}}]
    conn = create_connector()
    r,_ = conn.query(token_cmd)
    removed =r[0]["UpdateUser"]
    if removed["status"] == 0:
        print("Action complete")
    else:
        print(f"Error removing token: removed['info']")
