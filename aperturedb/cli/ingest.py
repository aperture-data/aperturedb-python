from enum import Enum
import logging
import typer
from typing_extensions import Annotated
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
from aperturedb.cli.console import console

logger = logging.getLogger(__file__)
app = typer.Typer()


IngestType = Enum('IngestType', {k: str(k) for k in ObjectType._member_names_})


@app.command()
def from_csv(filepath: Annotated[str, typer.Argument(help="Path to csv for ingestion")],
             batchsize: Annotated[int, typer.Option(
                 help="Size of the batch")] = 1,
             num_workers: Annotated[int, typer.Option(
                 help="Number of workers for ingestion")] = 1,
             stats: Annotated[bool, typer.Option(
                 help="Show realtime statistics with summary")] = True,
             use_dask: Annotated[bool, typer.Option(
                 help="Use dask based parallelization")] = True,
             ingest_type: Annotated[IngestType, typer.Option(
                 help="Parser for CSV file to be used")] = IngestType.IMAGE,
             blobs_relative_to_csv: Annotated[bool, typer.Option(
                 help="If true, the blob path is relative to the csv file")] = True,
             ):

    ingest_types = {
        IngestType.BLOB: BlobDataCSV,
        IngestType.BOUNDING_BOX: BBoxDataCSV,
        IngestType.CONNECTION: ConnectionDataCSV,
        IngestType.DESCRIPTOR: DescriptorDataCSV,
        IngestType.DESCRIPTORSET: DescriptorSetDataCSV,
        IngestType.ENTITY: EntityDataCSV,
        IngestType.IMAGE: ImageDataCSV,
        IngestType.POLYGON: PolygonDataCSV,
        IngestType.VIDEO: VideoDataCSV
    }

    data = ingest_types[ingest_type](filepath, use_dask=use_dask,
                                     blobs_relative_to_csv=blobs_relative_to_csv)
    db = create_connector()
    console.log(db)

    loader = ParallelLoader(db)
    loader.ingest(data, batchsize=batchsize,
                  numthreads=num_workers, stats=stats)
