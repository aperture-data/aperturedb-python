import os
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.CommonLibrary import create_connector
import typer


app = typer.Typer()


@app.command()
def main(use_dask: bool = False, csv_path: str = "data.csv"):
    db = create_connector()

    data = EntityDataCSV(filename=os.path.join(
        os.path.dirname(__file__), csv_path), use_dask=use_dask)
    loader = ParallelLoader(db=db)
    loader.ingest(generator=data, batchsize=2000, numthreads=8, stats=True)


if __name__ == "__main__":
    app()
