from typing import Annotated

import typer

from aperturedb.cli.console import console
from aperturedb.Configuration import Configuration
from aperturedb.Connector import Connector

app = typer.Typer()


@app.command(help="Create Key for a user")
def generate(user: Annotated[str, typer.Argument(help="The user to generate a key for")]):
    from aperturedb.CommonLibrary import create_connector
    conn = create_connector()
    key = generate_user_key(conn, user)
    console.log(f"Key for {user} is", key, highlight=False)


def generate_user_key(conn: Connector, user: str):
    from aperturedb.Utils import Utils
    u = Utils(conn)
    token = u.generate_token()
    u.assign_token(user, token)
    key = Configuration.create_aperturedb_key(
        conn.config.host, conn.config.port, token, conn.config.use_rest,
        conn.config.use_ssl)
    return key
