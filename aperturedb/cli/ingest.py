from enum import Enum
import logging
import typer
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.BBoxDataCSV import BBoxDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.Connector import Connector
from aperturedb.Utils import connector

logger = logging.getLogger(__file__)
app = typer.Typer()


class IngestType(str, Enum):
    image = "image"
    bbox = "bbox"
    entity = "entity"
    blob = "blob"


@app.command()
def from_csv(filepath: str, batchsize: int = 1, numthreads: int = 1,
             stats: bool = True, ingest_type: IngestType = IngestType.image):

    ingest_types = {
        IngestType.image: ImageDataCSV,
        IngestType.bbox: BBoxDataCSV,
        IngestType.entity: EntityDataCSV,
        IngestType.blob: BlobDataCSV
    }

    data = ingest_types[ingest_type](filepath)

    db = connector()
    loader = ParallelLoader(db)
    loader.ingest(data, batchsize=batchsize,
                  numthreads=numthreads, stats=stats)
