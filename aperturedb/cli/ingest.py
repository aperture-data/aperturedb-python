import logging
import typer
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.BBoxDataCSV import BBoxDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.Connector import Connector

logger = logging.getLogger(__file__)
app = typer.Typer()

@app.command()
def from_csv(filepath: str, batchsize: int = 1, numthreads:int = 1, stats: bool = True):
    with open(filepath) as infile:
        headers = infile.readline().split(",")
        csv_identifiers = {
            "filename": ImageDataCSV,
            "url": ImageDataCSV,
            "s3_url": ImageDataCSV,
            "gs_url": ImageDataCSV
        }
        identifier = csv_identifiers[headers[0]]
        logger.info(f"csv processed as {identifier}")

        data = identifier(filename=filepath)

        db = Connector(port=55557, user="admin", password="admin")
        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=batchsize, numthreads=numthreads, stats=stats)
