import typer

from aperturedb.cli import configure, ingest

app = typer.Typer()

app.add_typer(ingest.app, name="ingest", help="Ingest data into ApertureDB.")
app.add_typer(configure.app, name="config",
              help="Configure ApertureDB client.")


@app.callback()
def check_context(ctx: typer.Context):
    if ctx.invoked_subcommand != "config":
        configure.check_configured(as_global=False) or \
            configure.check_configured(as_global=True, show_error=True)


if __name__ == "__main__":
    app()