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
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.image_properties import ImageProperties

import importlib
import os
import sys

logger = logging.getLogger(__file__)
app = typer.Typer()


IngestType = Enum('IngestType', {k: str(k) for k in ObjectType._member_names_})


def _debug_samples(data, sample_count, module_name):
    import json
    print(f"len(data)={len(data)}")
    for i, r in enumerate(data[0:sample_count]):
        q, b = r
        with open(module_name + f"_{i}" + ".json", "w") as f:
            f.write(json.dumps(q, indent=2))
        for j, blob in enumerate(b):
            with open(module_name + f"_{i}_{j}" + ".jpg", "wb") as f:
                f.write(blob)


def _apply_pipeline(data):
    data = CommonProperties(data)
    data = ImageProperties(data)
    return data


@app.command()
def from_generator(filepath: Annotated[str, typer.Argument(
    help="Path to python module for ingestion [BETA]")],
    sample_count: Annotated[int, typer.Option(
        help="Number of samples to ingest")] = 1,
    debug: Annotated[bool, typer.Option(
        help="Debug mode")] = False,
    batchsize: Annotated[int, typer.Option(
        help="Size of the batch")] = 1,
    num_workers: Annotated[int, typer.Option(
        help="Number of workers for ingestion")] = 1,
    apply_image_properties: Annotated[bool, typer.Option(
        help="Apply image properties to the AddImage command")] = True,
):
    """
    Ingest data from a Data generator.
    """
    db = create_connector()
    loader = ParallelLoader(db)
    module_name = os.path.basename(filepath)[:-3]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    mclass = getattr(module, module_name)
    data = mclass()
    if apply_image_properties:
        data = _apply_pipeline(data)

    if debug:
        _debug_samples(data, sample_count, module_name)
    else:
        loader.ingest(
            data[:sample_count],
            stats=True,
            batchsize=batchsize,
            numthreads=num_workers)


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
             apply_image_properties: Annotated[bool, typer.Option(
                 help="Apply image properties to the AddImage command")] = True,
             ):
    """
    Ingest data from a pre generated CSV file.
    """

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
    if apply_image_properties:
        data = _apply_pipeline(data)
    db = create_connector()
    console.log(db)

    loader = ParallelLoader(db)
    loader.ingest(data, batchsize=batchsize,
                  numthreads=num_workers, stats=stats)
