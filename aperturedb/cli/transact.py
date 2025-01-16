import json
from enum import Enum
import sys
import traceback

import typer
from typing_extensions import Annotated

from aperturedb.cli.console import console

from aperturedb.Connector import Connector
import logging

logger = logging.getLogger(__file__)

FUSE_AVAIALBLE = False


def load_fuse():
    global FUSE_AVAIALBLE
    try:
        from aperturedb.cli.mount_coco import mount_images_from_aperturedb
        FUSE_AVAIALBLE = True
    except ImportError as e:
        logger.warning(
            "fuse not found for this env. This is not critical for adb to continue.")


app = typer.Typer(callback=load_fuse)


class OutputTypes(str, Enum):
    STDOUT = "stdout"
    MOUNT_COCO = "mount_coco"
    RAW_JSON = "raw_json"


def dump_as_raw_json(client: Connector, transaction: dict, **kwargs):
    """
    Function to pass the result of a transaction as raw json to stdout.
    Does not handle blobs.

    Args:
        client (Connector): The client to the database
        transaction (dict): Query to be executed.
    """
    from aperturedb.CommonLibrary import execute_query

    result, response, blobs = execute_query(
        client=client,
        query=transaction,
        blobs=[])
    print(json.dumps(response, indent=2))


def dump_to_stdout(client: Connector, transaction: dict, **kwargs):
    from aperturedb.CommonLibrary import execute_query

    result, response, blobs = execute_query(
        client=client,
        query=transaction,
        blobs=[])
    console.log(result)
    console.log(response)
    for i, blob in enumerate(blobs):
        console.log(f"len(blob[{i}]) = {len(blob[i])}")


def mount_as_coco_ds(client: Connector, transaction: dict, **kwargs):
    from aperturedb.Images import Images
    from aperturedb.CommonLibrary import execute_query

    result, response, blobs = execute_query(
        client=client,
        query=transaction,
        blobs=[])
    if result == 0:
        image_entities = []
        for i, cr in enumerate(response):
            if "FindImage" in cr:
                if "entities" in cr["FindImage"]:
                    image_entities.extend(cr["FindImage"]["entities"])
                else:
                    console.log(f"No entities found in FindImage {i} response")
        try:
            from aperturedb.cli.mount_coco import mount_images_from_aperturedb
            images = Images(client, response=image_entities)
            console.log(f"Found {len(images)} images")
            mount_images_from_aperturedb(images)
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
    from aperturedb.CommonLibrary import create_connector

    client = create_connector()

    output_types = {
        OutputTypes.STDOUT: dump_to_stdout,
        OutputTypes.RAW_JSON: dump_as_raw_json
    }
    global FUSE_AVAIALBLE
    if FUSE_AVAIALBLE:
        output_types[OutputTypes.MOUNT_COCO] = mount_as_coco_ds

    with open(filepath) as inputstream:
        transaction = json.loads(inputstream.read())
        old_argv = sys.argv[1:]
        sys.argv[1:] = [output_path]
        output_types[output_type](client, transaction, output_path=output_path)
        sys.argv[1:] = old_argv
