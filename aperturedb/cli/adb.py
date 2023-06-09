import typer
from . import ingest

app = typer.Typer()
app.add_typer(ingest.app, name="ingest")

# @app.command()
# def goodbye(name: str, formal: bool = False):
#     if formal:
#         print(f"Goodbye Ms. {name}. Have a good day.")
#     else:
#         print(f"Bye {name}!")
