import json
from enum import Enum
import sys
import traceback

import typer
from typing_extensions import Annotated

from aperturedb.cli.console import console
from aperturedb.cli.mount_coco import mount_images_from_aperturdb
from aperturedb.Connector import Connector
from aperturedb.Images import Images
from aperturedb.ParallelQuery import execute_batch
from aperturedb.Utils import create_connector

app = typer.Typer()


class OutputTypes(str, Enum):
    STDOUT = "stdout"
    MOUNT_COCO = "mount_coco"
    SERVE_LS = "serve_ls"


def dump_to_stdout(db: Connector, transaction: dict, **kwargs):
    result, response, blobs = execute_batch(
        db=db,
        q=transaction,
        blobs=[])
    console.log(result)
    console.log(response)
    for i, blob in enumerate(blobs):
        console.log(f"len(blob[{i}]) = {len(blob[i])}")


def mount_as_coco_ds(db: Connector, transaction: dict, **kwargs):
    result, response, blobs = execute_batch(
        db=db,
        q=transaction,
        blobs=[])
    if result == 0:
        image_entities = []
        for cr in response:
            if "FindImage" in cr:
                image_entities.extend(cr["FindImage"]["entities"])
        try:

            images = Images(db, response=image_entities)
            # import  pdb; pdb.set_trace()
            console.log(f"Found {len(images)} images")
            mount_images_from_aperturdb(images)
        except Exception as e:
            console.log(traceback.format_exc())
    else:
        console.log(response)


@app.command()
def from_json_file(
    filepath: Annotated[str, typer.Argument(help="Path to query in json format")],
    output_type: Annotated[OutputTypes, typer.Option(
        help="Type of output")] = "stdout",
    output_path: Annotated[str,  typer.Option(
        help="Path to output (only for mount as output)")] = None
):
    db = create_connector()

    output_types = {
        OutputTypes.STDOUT: dump_to_stdout,
        OutputTypes.MOUNT_COCO: mount_as_coco_ds,
        OutputTypes.SERVE_LS: None
    }

    with open(filepath) as inputstream:
        transaction = json.loads(inputstream.read())
        old_argv = sys.argv[1:]
        sys.argv[1:] = [output_path]
        output_types[output_type](db, transaction, output_path=output_path)
        sys.argv[1:] = old_argv
