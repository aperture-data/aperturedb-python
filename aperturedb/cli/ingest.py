from enum import Enum
import logging
from typing import List, Optional
import typer
import os
import time

from typing_extensions import Annotated

from aperturedb.Query import ObjectType
from aperturedb.cli.console import console


from tqdm import tqdm

logger = logging.getLogger(__file__)
app = typer.Typer()


IngestType = Enum('IngestType', {k: str(k) for k in ObjectType._member_names_})

# This list needs to be updated when new transformers are added
# This enables CLI params.


class TransformerType(str, Enum):
    common_properties = "common_properties"
    image_properties = "image_properties"
    clip_pytorch_embeddings = "clip_pytorch_embeddings"
    facenet_pytorch_embeddings = "facenet_pytorch_embeddings"


def _debug_samples(data, sample_count, module_name):
    import json
    print(f"len(data)={len(data)}")
    for i, r in enumerate(data[0:sample_count]):
        query, blobs = r
        with open(module_name + f"_{i}" + ".raw", "w") as f:
            f.write(str(query))
        with open(module_name + f"_{i}" + ".json", "w") as f:
            f.write(json.dumps(query, indent=2))
        for j, blob in enumerate(blobs):
            with open(module_name + f"_{i}_{j}" + ".jpg", "wb") as f:
                f.write(blob)


def _apply_pipeline(data, transformers: List[str], **kwargs):
    pipeline = _create_pipeline(transformers)
    console.log("Applying Pipeline: \r\n" +
                "\r\n=>".join([f"{stage.__name__}[{kwargs}]" for stage in pipeline]))
    for transformer in pipeline:
        data = transformer(data, **kwargs)
        data.sample_count = data.data.sample_count
    return data


def _create_pipeline(transformers: List[str]):
    from aperturedb.transformers.common_properties import CommonProperties
    from aperturedb.transformers.image_properties import ImageProperties
    from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    from aperturedb.CommonLibrary import import_module_by_path

    # Actual collection of transformers, packaged with aperturedb.
    built_in_transformers = {
        "common_properties": CommonProperties,
        "image_properties": ImageProperties,
        "clip_pytorch_embeddings": CLIPPyTorchEmbeddings,
        "facenet_pytorch_embeddings": FacenetPyTorchEmbeddings
    }
    pipeline = []
    for transformer_name in transformers:
        transformer = None
        if transformer_name in built_in_transformers:
            transformer = built_in_transformers[transformer_name]
        else:
            try:
                transformer_module = import_module_by_path(transformer_name)
                transformer = getattr(transformer_module,
                                      transformer_module.__name__)
            except Exception as e:
                console.log(
                    f"Could not load transformer {transformer_name}: {e}")
                exit(1)
        pipeline.append(transformer)
    return pipeline


@app.command()
def from_generator(filepath: Annotated[str, typer.Argument(
    help="Path to python module for ingestion [BETA]")],
    sample_count: Annotated[int, typer.Option(
        help="Number of samples to ingest (-1 for all)")] = -1,
    debug: Annotated[bool, typer.Option(
        help="Debug mode")] = False,
    batchsize: Annotated[int, typer.Option(
        help="Size of the batch")] = 1,
    num_workers: Annotated[int, typer.Option(
        help="Number of workers for ingestion")] = 1,
    transformer: Annotated[Optional[List[TransformerType]], typer.Option(
        help="Apply transformer to the pipeline [Can be specified multiple times]")] = None,
    user_transformer: Annotated[Optional[List[str]], typer.Option(
        help="Apply user transformer to the pipeline as path to file [Can be specified multiple times]")] = None,
):
    """
    Ingest data from a Data generator [BETA].
    """
    from aperturedb.ParallelLoader import ParallelLoader
    from aperturedb.CommonLibrary import create_connector, import_module_by_path

    client = create_connector()
    loader = ParallelLoader(client)

    module = import_module_by_path(filepath)

    # The assumption is that the class name is same as that of the module name
    module_name  = os.path.basename(filepath)[:-3]
    mclass = getattr(module, module_name)

    # This is the dynamically loaded data generator.
    # tested with CelebADataKaggle.py, CocoDataPytorch.py and Cifar10DataTensorflow.py in examples
    start = time.time()
    data = mclass()
    data.sample_count = len(data) if sample_count == -1 else sample_count
    console.log(f"Data generator loaded in {time.time() - start} seconds")

    if transformer or user_transformer:
        transformer = transformer or []
        user_transformer = user_transformer or []
        all_transformers = transformer + user_transformer
        data = _apply_pipeline(data, all_transformers,
                               adb_data_source=f"{module_name}.{mclass.__name__}")

    if debug:
        _debug_samples(data, sample_count, module_name)
    else:
        loader.ingest(
            data,
            stats=True,
            batchsize=batchsize,
            numthreads=num_workers)

    while hasattr(data, "ncalls"):
        console.log(
            f"Calls to {data}.getitem = {data.ncalls} time={data.cumulative_time}")
        data = data.data


@app.command()
def from_csv(filepath: Annotated[str, typer.Argument(
    help="Path to csv for ingestion")],
    batchsize: Annotated[int, typer.Option(
        help="Size of the batch")] = 1,
    num_workers: Annotated[int, typer.Option(
        help="Number of workers for ingestion")] = 1,
    stats: Annotated[bool, typer.Option(
        help="Show realtime statistics with summary")] = True,
    use_dask: Annotated[bool, typer.Option(
        help="Use dask based parallelization")] = False,
    ingest_type: Annotated[IngestType, typer.Option(
        help="Parser for CSV file to be used")] = IngestType.IMAGE,
    blobs_relative_to_csv: Annotated[bool, typer.Option(
        help="If true, the blob path is relative to the CSV file")] = True,
    transformer: Annotated[Optional[List[TransformerType]], typer.Option(
        help="Apply transformer to the pipeline [Can be specified multiple times]")] = None,
    user_transformer: Annotated[Optional[List[str]], typer.Option(
        help="Apply user transformer to the pipeline as path to file [Can be specified multiple times.]")] = None,
    sample_count: Annotated[int, typer.Option(
        help="Number of samples to ingest (-1 for all)")] = -1,
    debug: Annotated[bool, typer.Option(
        help="Debug mode")] = False,
):
    """
    Ingest data from a pre generated CSV file.
    """
    from aperturedb.ImageDataCSV import ImageDataCSV
    from aperturedb.BBoxDataCSV import BBoxDataCSV
    from aperturedb.EntityDataCSV import EntityDataCSV
    from aperturedb.BlobDataCSV import BlobDataCSV
    from aperturedb.ConnectionDataCSV import ConnectionDataCSV
    from aperturedb.PolygonDataCSV import PolygonDataCSV
    from aperturedb.VideoDataCSV import VideoDataCSV
    from aperturedb.DescriptorDataCSV import DescriptorDataCSV
    from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV
    from aperturedb.ParallelLoader import ParallelLoader

    from aperturedb.CommonLibrary import create_connector

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
    data.sample_count = len(data) if sample_count == -1 else sample_count
    if transformer or user_transformer:
        transformer = transformer or []
        user_transformer = user_transformer or []
        all_transformers = transformer + user_transformer
        data = _apply_pipeline(data, all_transformers,
                               adb_data_source=f"{ingest_type}.{os.path.basename(filepath)}")

    client = create_connector()

    loader = ParallelLoader(client)
    if debug:
        _debug_samples(data, sample_count, filepath)
    else:
        loader.ingest(data, batchsize=batchsize,
                      numthreads=num_workers, stats=stats)


@app.command()
def generate_embedding_csv_from_image_csv(
    input_file: Annotated[str, typer.Argument(
        help="Path to csv for ingestion")],
    set_name: Annotated[str, typer.Argument(
        help="Descriptor set name to be associated with the embeddings")],
    transformer: Annotated[Optional[List[TransformerType]], typer.Option(
        help="Apply transformer to the pipeline [Can be specified multiple times]")] = None,
    user_transformer: Annotated[Optional[List[str]], typer.Option(
        help="Apply user transformer to the pipeline as path to file [Can be specified multiple times.]")] = None,
    sample_count: Annotated[int, typer.Option(
        help="Number of samples to ingest (-1 for all)")] = -1,
):
    """
    Create a DescriptorDataCSV from an ImageDataCSV, using the specified transformer.
    Also save embeddings in a separate file, and generate the ConnectionDataCSV
    for linking the images to the descriptors.
    The ingestion of the embeddings and the connection data is not done here.
    image_properties transformer must be used at actual ingestion.
    It is also applied by default for CSV generation.
    """
    import pandas as pd
    from aperturedb.ImageDataCSV import ImageDataCSV
    import numpy as np

    data = ImageDataCSV(input_file)
    data.sample_count = len(data) if sample_count == -1 else sample_count
    if transformer or user_transformer:
        transformer = transformer or []
        user_transformer = user_transformer or []
        all_transformers = transformer + user_transformer
        if len(all_transformers) == 0:
            console.log(
                "No transformer specified . Generating embeddings from raw images requires a transformer.")
            typer.Abort()
        all_transformers.insert(0, "image_properties")
        data = _apply_pipeline(data, all_transformers,
                               adb_data_source=f"{os.path.basename(input_file)}")
    filename = f"{os.path.basename(input_file)}_{all_transformers[-1]}"
    metadata = []
    connection = []
    embeddings = []
    errored = []

    for i in tqdm(range(data.sample_count)):
        d = data[i]
        nparr = np.frombuffer(d[1][1], dtype=np.float32)
        if not nparr.any():
            errored.append(d[0][0]["AddImage"]["properties"])

        embeddings.append(nparr)
        descriptor_id = f"{set_name}_{os.path.basename(input_file)}_{i}"
        metadata.append({
            "filename": f"{filename}.npy",
            "index": i,
            "set": set_name,
            "id": descriptor_id,
            "constraint_id": descriptor_id
        })
        connection.append({
            "ConnectionClass": "image_descriptor",
            "_Image@adb_image_sha256": d[0][0]["AddImage"]["properties"]["adb_image_sha256"],
            "_Descriptor@id": descriptor_id,
        })

    with open(f"{filename}.npy", "wb") as f:
        np.save(f, embeddings)
    pd.json_normalize(metadata).to_csv(
        f"{filename}_metadata.adb.csv", index=False)
    pd.json_normalize(connection).to_csv(
        f"{filename}_connection.adb.csv", index=False)
    if(len(errored) > 0):
        pd.json_normalize(errored).to_csv(
            f"{filename}_errored.csv", index=False)
