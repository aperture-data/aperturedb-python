import dataclasses
import hashlib
import io
import json
import logging
import PIL.GifImagePlugin
import mlcroissant as mlc
import PIL.Image
import pandas as pd

from typing import Any, List, Tuple

from aperturedb.Subscriptable import Subscriptable
from aperturedb.Query import QueryBuilder
from aperturedb.DataModels import IdentityDataModel
from aperturedb.Query import generate_add_query


logger = logging.getLogger(__name__)


MAX_REF_VALUE = 99999
# This is useful to identify the class of the record in ApertureDB.
CLASS_PROPERTY_NAME = "adb_class_name"


class RecordSetModel(IdentityDataModel):
    name: str
    description: str = ""
    uuid: str = ""


class DatasetModel(IdentityDataModel):
    url: str = ""
    name: str = "Croissant Dataset automatically ingested into ApertureDB"
    description: str = f"A dataset loaded from a croissant json-ld"
    version: str = "1.0.0"
    record_sets: List[RecordSetModel] = dataclasses.field(default_factory=list)


def deserialize_record(record):
    """These are the types of records that we expect to deserialize, from croissant Records.

    Args:
        record (_type_): _description_

    Returns:
        _type_: _description_
    """
    deserialized = record
    if record is None:
        deserialized = "Not Available"
    if isinstance(record, bytes):
        deserialized = record.decode('utf-8')
    if isinstance(record, pd.Timestamp):
        deserialized = {"_date": record.to_pydatetime().isoformat()}
    if record == pd.NaT:
        deserialized = "Not Available Time"
    if isinstance(deserialized, str):
        if deserialized.startswith("[") or deserialized.startswith("{"):
            # If it looks like a list or dict, try to parse it as JSON
            try:
                deserialized = json.loads(deserialized)
            except json.JSONDecodeError:
                logger.info(f"Failed to parse JSON: {deserialized}")

            try:
                deserialized = json.loads(deserialized.replace("'", "\""))
            except Exception as e:
                logger.info(
                    f"Failed to parse JSON: {deserialized} with error {e}")

    if isinstance(deserialized, list):
        deserialized = [deserialize_record(item) for item in deserialized]
    if isinstance(deserialized, dict):
        deserialized = {k: deserialize_record(
            v) for k, v in deserialized.items()}

    return deserialized


def persist_metadata(dataset: mlc.Dataset, url: str) -> Tuple[List[dict], List[bytes]]:
    """
    Persist the metadata of a croissant dataset into ApertureDB.
    """
    ds = DatasetModel(
        url=url,
        name=dataset.metadata.name,
        description=dataset.metadata.description,
        version=dataset.metadata.version or "1.0.0",
        record_sets=[RecordSetModel(
            name=rs.name,
            description=rs.description,
            uuid=rs.uuid,
        ) for rs in dataset.metadata.record_sets]
    )
    q, b, _ = generate_add_query(ds)

    return q, b


def try_parse(value: str) -> Any:
    """Attempts to parse a string value into a more appropriate type."""
    parsed = value.strip()

    if parsed.startswith("http"):
        # Download the content from the URL
        from aperturedb.Sources import Sources
        sources = Sources(n_download_retries=3)
        result, buffer = sources.load_from_http_url(
            parsed, validator=lambda x: True)
        if result:
            parsed = PIL.Image.open(io.BytesIO(buffer))

    return parsed


def dict_to_query(row_dict, name: str, flatten_json: bool) -> Any:
    literals = {}
    subitems = {}
    known_image_blobs = {}
    unknown_blobs = {}
    o_literals = {}

    name = name.split("/")[-1]  # Use the last part of the name
    # If name is not specified, or begins with _, this ensures that it
    # complies with the ApertureDB naming conventions
    if not name or name.startswith("_"):
        safe_name = f"E_{name or 'Record'}"  # Uncomment if you want
        logger.warning(
            f"Entity Name '{name}' is not valid. Using {safe_name}.")
        name = safe_name

    for k, v in row_dict.items():
        k = k.split("/")[-1]  # Use the last part of the key
        if not k or k.startswith("_"):
            safe_key = f"F_{k or 'Field'}"
            logger.warning(
                f"Property name '{k}' is not valid. Using {safe_key}.")
            k = safe_key
        item = v
        # Pre processed items from croissant.
        if isinstance(item, PIL.Image.Image):
            buffer = io.BytesIO()
            item.save(buffer, format=item.format)
            known_image_blobs[k] = buffer.getvalue()
            continue

        record = deserialize_record(item)
        if isinstance(record, str):
            record = try_parse(record)

            # Post processed items from SDK.
            if isinstance(record, PIL.GifImagePlugin.GifImageFile):
                buffer = io.BytesIO()
                record.save(buffer, format=record.format)
                unknown_blobs[k] = buffer.getvalue()
                continue

            if isinstance(record, PIL.Image.Image):
                buffer = io.BytesIO()
                record.save(buffer, format=record.format)
                known_image_blobs[k] = buffer.getvalue()
                continue

        if flatten_json and isinstance(record, list):
            subitems[k] = record
        else:
            literals[k] = record
            # Original value from croissant. This is useful for debugging.
            o_literals[k] = item

    if flatten_json:
        str_rep = "".join([f"{str(k)}{str(v)}" for k, v in literals.items()])
        literals["adb_uuid"] = hashlib.sha256(
            str_rep.encode('utf-8')).hexdigest()

    literals[CLASS_PROPERTY_NAME] = name
    q = QueryBuilder.add_command(name, {
        "properties": literals,
        "connect": {
            "ref": MAX_REF_VALUE,
            "class": "hasRecord",
            "direction": "in",
        }
    })
    if flatten_json:
        q[list(q.keys())[-1]]["if_not_found"] = {
            "adb_uuid": ["==", literals["adb_uuid"]]
        }

    dependents = []
    if len(subitems) > 0 or len(known_image_blobs) > 0 or len(unknown_blobs) > 0:
        # We need to create a reference to this record
        q[list(q.keys())[-1]]["_ref"] = 1

    for key in subitems:
        for item in subitems[key]:
            subitem_query, blobs = dict_to_query(
                item, f"{name}.{key}", flatten_json)
            subitem_query[0][list(subitem_query[0].keys())[-1]]["connect"] = {
                "ref": 1,
                "class": key,
                "direction": "in",
            }
            dependents.extend(subitem_query)

    from aperturedb.Query import ObjectType
    blobs = []
    for blob in known_image_blobs:
        image_query = QueryBuilder.add_command(ObjectType.IMAGE, {
            "properties": {CLASS_PROPERTY_NAME: literals[CLASS_PROPERTY_NAME] + "." + "image"},
            "connect": {
                "ref": 1,
                "class": blob,
                "direction": "in"
            }
        })
        blobs.append(known_image_blobs[blob])
        dependents.append(image_query)

    for blob in unknown_blobs:
        blob_query = QueryBuilder.add_command(ObjectType.BLOB, {
            "properties": {CLASS_PROPERTY_NAME: literals[CLASS_PROPERTY_NAME] + "." + "blob"},
            "connect": {
                "ref": 1,
                "class": blob,
                "direction": "in"
            }
        })
        blobs.append(unknown_blobs[blob])
        dependents.append(blob_query)

    return [q] + dependents, blobs


class MLCroissantRecordSet(Subscriptable):
    def __init__(
            self,
            record_set: mlc.Records,
            name: str,
            flatten_json: bool,
            sample_count: int = 0,
            uuid: str = None):
        self.record_set = record_set
        self.uuid = uuid
        samples = []
        count = 0
        for record in record_set:
            samples.append({k: v for k, v in record.items()})
            count += 1
            if count == sample_count:
                break

        self.samples = samples
        self.sample_count = len(samples)
        self.name = name
        self.flatten_json = flatten_json
        self.indexed_entities = set()

    def getitem(self, subscript):
        row_dict = self.samples[subscript]

        find_recordset_query = QueryBuilder.find_command(
            "RecordSetModel", {
                "_ref": MAX_REF_VALUE,
                "constraints": {
                    "uuid": ["==", self.uuid]
                }
            })

        q, blobs = dict_to_query(row_dict, self.name, self.flatten_json)
        indexes_to_create = []
        for command in q:
            cmd = list(command.keys())[-1]
            if cmd in ["AddImage", "AddBlob", "AddVideo"]:
                continue
            indexable_entity = command[list(command.keys())[-1]]["class"]
            if indexable_entity not in self.indexed_entities:
                index_command = {
                    "CreateIndex": {
                        "class": indexable_entity,
                        "index_type": "entity",
                        "property_key": "adb_uuid",
                    }
                }
                indexes_to_create.append(index_command)
        return indexes_to_create + [find_recordset_query] + q, blobs

    def __len__(self):
        return len(self.samples)
