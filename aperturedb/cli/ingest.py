from enum import Enum
import logging
import typer
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.BBoxDataCSV import BBoxDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.PolygonDataCSV import PolygonDataCSV
from aperturedb.VideoDataCSV import VideoDataCSV
from aperturedb.DescriptorDataCSV import DescriptorDataCSV
from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV

from aperturedb.Utils import create_connector
from aperturedb.Query import ObjectType

logger = logging.getLogger(__file__)
app = typer.Typer()


IngestType = Enum('IngestType', {k: str(k) for k in ObjectType._member_names_})


@app.command()
def from_csv(filepath: str, batchsize: int = 1, numthreads: int = 1,
             stats: bool = True,
             ingest_type: IngestType = IngestType.IMAGE):

    ingest_types = {
        IngestType.BLOB: BlobDataCSV,
        IngestType.BOUNDING_BOX: BBoxDataCSV,
        IngestType.CONNECTION: ConnectionDataCSV,
        IngestType.DESCRIPTOR: DescriptorDataCSV,
        IngestType.DESCRIPTOR_SET: DescriptorSetDataCSV,
        IngestType.ENTITY: EntityDataCSV,
        IngestType.IMAGE: ImageDataCSV,
        IngestType.POLYGON: PolygonDataCSV,
        IngestType.VIDEO: VideoDataCSV
    }

    data = ingest_types[ingest_type](filepath)

    db = create_connector()
    loader = ParallelLoader(db)
    loader.ingest(data, batchsize=batchsize,
                  numthreads=numthreads, stats=stats)
