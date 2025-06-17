import io
import json
from typing import Any, List, Tuple

import PIL
import PIL.Image
import mlcroissant as mlc
import pandas as pd


from aperturedb.Subscriptable import Subscriptable
from aperturedb.Query import QueryBuilder
from aperturedb.CommonLibrary import execute_query


import dataclasses
import hashlib

from aperturedb.DataModels import IdentityDataModel
from aperturedb.Query import generate_add_query


class RecordSetModel(IdentityDataModel):
    name: str
    description: str = ""
    uuid: str = ""


class DatasetModel(IdentityDataModel):
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
        try:
            deserialized = json.loads(deserialized)
        except:
            pass
    if isinstance(deserialized, list):
        deserialized = [deserialize_record(item) for item in deserialized]
    if isinstance(deserialized, dict):
        deserialized = {k: deserialize_record(
            v) for k, v in deserialized.items()}

    return deserialized


def persist_metadata(dataset: mlc.Dataset) -> Tuple[List[dict], List[bytes]]:

    ds = DatasetModel(
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


def dict_to_query(row_dict, name: str, flatten_json: bool) -> Any:
    literals = {}
    subitems = {}
    blobs = {}
    o_literalse = {}

    # If name is not specified, or begins with _, this enures that it
    # complies with the ApertureDB naming conventions
    name = f"E_{name or 'Record'}"

    for k, v in row_dict.items():
        k = f"F_{k}"
        item = v
        if isinstance(item, PIL.Image.Image):
            buffer = io.BytesIO()
            item.save(buffer, format=item.format)
            blobs[k] = buffer.getvalue()
            continue

        record = deserialize_record(item)
        if flatten_json and isinstance(record, list):
            subitems[k] = record
        else:
            literals[k] = record
            o_literalse[k] = item

    if flatten_json:
        str_rep = "".join([f"{str(k)}{str(v)}" for k, v in literals.items()])
        literals["adb_uuid"] = hashlib.sha256(
            str_rep.encode('utf-8')).hexdigest()

    literals["adb_class_name"] = name
    q = QueryBuilder.add_command(name, {
        "properties": literals,
    })
    if flatten_json:
        q[list(q.keys())[-1]]["if_not_found"] = {
            "adb_uuid": ["==", literals["adb_uuid"]]
        }

    dependents = []
    if len(subitems) > 0 or len(blobs) > 0:
        q[list(q.keys())[-1]]["_ref"] = 1

    for key in subitems:
        for item in subitems[key]:
            subitem_query = dict_to_query(item, f"{name}.{key}", flatten_json)
            subitem_query[0][list(subitem_query[0].keys())[-1]]["connect"] = {
                "ref": 1,
                "class": key,
                "direction": "out",
            }
            dependents.extend(subitem_query)

    from aperturedb.Query import ObjectType
    image_blobs = []
    for blob in blobs:
        image_query = QueryBuilder.add_command(ObjectType.IMAGE, {
            "properties": literals,
            "connect": {
                "ref": 1,
                "class": blob,
                "direction": "out"
            }
        })
        image_blobs.append(blobs[blob])
        dependents.append(image_query)

    return [q] + dependents, image_blobs


class MLCroissantRecordSet(Subscriptable):
    def __init__(self, record_set: mlc.RecordSet, name: str, flatten_json: bool, sample_count: int = 0):
        self.record_set = record_set
        samples = []
        count = 0
        for record in record_set:
            samples.append({k: v for k, v in record.items()})
            count += 1
            if count == sample_count:
                break

        self.df = pd.json_normalize(samples)
        self.sample_count = len(samples)
        self.name = name
        self.flatten_json = flatten_json
        self.indexed_entities = set()

    def getitem(self, subscript):
        row = self.df.iloc[subscript]
        # Convert the row to a dictionary
        row_dict = row.to_dict()

        q, blobs = dict_to_query(row_dict, self.name, self.flatten_json)
        indexes_to_create = []
        for command in q:
            cmd = list(command.keys())[-1]
            if cmd == "AddImage":
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
        return indexes_to_create + q, blobs

    def __len__(self):
        return len(self.df)
