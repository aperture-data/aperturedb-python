import typer

from aperturedb.cli import configure, ingest, utilities, transact

app = typer.Typer(pretty_exceptions_show_locals=False)

app.add_typer(ingest.app, name="ingest", help="Ingest data into ApertureDB.")
app.add_typer(configure.app, name="config",
              help="Configure ApertureDB client.")
app.add_typer(utilities.app, name="utils", help="Utilities")
app.add_typer(transact.app, name="transact",
              help="Run a transaction against ApertureDB.")


@app.callback()
def check_context(ctx: typer.Context):
    if ctx.invoked_subcommand != "config":
        configure.check_configured(as_global=False) or \
            configure.check_configured(as_global=True, show_error=True)


if __name__ == "__main__":
    app()
