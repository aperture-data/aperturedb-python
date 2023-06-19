import typer

from aperturedb.cli import configure, ingest

app = typer.Typer()

app.add_typer(ingest.app, name="ingest")
app.add_typer(configure.app, name="config")

@app.callback()
def check_context(ctx: typer.Context):
    if ctx.invoked_subcommand != "config":
        configure.check_configured()

if __name__ == "__main__":
    app()