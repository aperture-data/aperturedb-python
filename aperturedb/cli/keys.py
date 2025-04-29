from enum import Enum
from typing import Annotated

import typer

from aperturedb.cli.console import console
import aperturedb.cli.tokens as tokens
from aperturedb.Configuration import Configuration
from aperturedb.Connector import Connector
from aperturedb.CommonLibrary import create_connector

app = typer.Typer()


@app.command(help="List Keys")
def list(user: Annotated[str, typer.Argument(help="The message to log")]):
    conn = create_connector()
    res, _ = conn.query([{"GetUserDetails": {"username": user}}])
    utokens = res[0]['GetUserDetails']['tokens']
    if len(utokens) == 0:
        print(f"No Tokens for {user}")
    else:
        print(utokens)
    pass


@app.command(help="Create Key for a user")
def generate(user: Annotated[str, typer.Argument(help="The user to generate a key for")]):
    conn = create_connector()
    key = generate_user_key(conn, user)
    print(f"{key}")


def generate_user_key(conn: Connector, user: str):
    token = tokens.generate_token(conn)
    tokens.assign_token(conn, user, token)
    key = Configuration.create_web_token(
        conn.config.host, conn.config.port, token)
    return key
