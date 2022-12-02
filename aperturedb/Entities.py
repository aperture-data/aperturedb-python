from __future__ import annotations
from typing import Any, Dict, List, Union
from aperturedb.Query import Query, ObjectType

from aperturedb.Subscriptable import Subscriptable
from aperturedb.Constraints import Constraints
from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import execute_batch
import pandas as pd


class Entities(Subscriptable):
    """
    This class is the common class to query any entity from apertureDB.
    The specialized subclasses, which provide a more userfriendly interface, are:
    :class:`~aperturedb.Entities.Images`
    :class:`~aperturedb.Entities.Polygons`
    """
    db_object = "Entity"
    find_command = f"Find{db_object}"
    update_command = f"Update{db_object}"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query
                 ) -> List[Entities]:
        """
        Using the Entities.retrieve method, is a simpple layer, with typical native queries converted
        using :class:`~aperturedb.Query.Query`

        Args:
            db (Connector): _description_
            spec (Query): _description_

        Raises:
            e: _description_

        Returns:
            List[Entities]: _description_
        """
        cls.known_entities = load_entities_registry(
            custom_entities=spec.command_properties(prop="with_class"))
        cls.db = db
        if cls.db_object != "Entity":
            spec.with_class = cls.db_object

        query = spec.query()
        print(f"query={query}")
        res, r, b = execute_batch(query, [], db)
        if res > 0:
            print(f"resp={r}")
        results = []
        for wc, req, resp in zip(spec.command_properties(prop="with_class"), spec.command_properties(prop="find_command"), r):
            subresponse = resp[req]['entities']
            if not isinstance(subresponse, list):
                flattened = []
                for previtem in results[-1]:
                    flattened.extend(subresponse[previtem["_uniqueid"]])
                subresponse = flattened
            try:
                entities = cls.known_entities[wc](
                    db=db, response=subresponse, type=wc)
                results.append(entities)
            except Exception as e:
                print(e)
                print(cls.known_entities)
                raise e
        return results

    def __init__(self, db: Connector = None, response: dict = None, type: str = None) -> None:
        super().__init__()
        self.db = db
        self.response = response
        self.type = type
        self.decorator = None
        self.get_image = False

    def getitem(self, idx):
        item = self.response[idx]
        if self.decorator is not None:
            for k, v in self.decorator(idx, self.adjacent).items():
                item[k] = v

        return item

    def __len__(self):
        return len(self.response)

    def filter(self, predicate):
        return self.known_entities[self.type](db=self.db, response=list(filter(predicate, self.response)), type=self.type)

    def __add__(self, other: Entities) -> Entities:
        return Entities(response = self.response + other.response, type=self.type)

    def __sub__(self, other: Entities) -> Entities:
        return Entities(response = [x for x in self.response if x not in other.response], type=self.type)

    def sort(self, key) -> Entities:
        return Entities(response = sorted(self.response, key=key), type=self.type)

    def inspect(self) -> pd.DataFrame:
        return pd.json_normalize([item for item in self])

    def update_properties(self, extra_properties: List[dict]) -> bool:
        for entity, properties in zip(self, extra_properties):
            query = [
                {
                    self.update_command: {
                        "constraints": {
                            "_uniqueid": ["==", entity["_uniqueid"]]
                        },
                        "properties": properties
                    }
                }
            ]
            res, r, b = execute_batch(query, [], self.db)

    def get_connected_entities(self,  etype: Union[ObjectType, str], constraints: Constraints = None) -> List[Entities]:
        """
        Gets all entities adjacent to and clustered around items of the collection

        Args:
            pk (str): _description_
            type (ObjectType): _description_
            constraints (Constraints, optional): _description_. Defaults to None.

        Returns:
            List[Entities]: _description_
        """
        result = []
        for entity in self:
            query = [
                {
                    self.find_command: {
                        "_ref": 1,
                        "unique": False,
                        "constraints": {
                            "_uniqueid": ["==", entity["_uniqueid"]]
                        }
                    }
                }, {
                    "FindEntity": {
                        "is_connected_to": {
                            "ref": 1
                        },
                        "with_class": type.value,
                        "constraints": constraints.constraints,
                        "results": {
                            "all_properties": True
                        }
                    }
                }
            ]
            res, r, b = execute_batch(query, [], self.db)
            result.append(self.known_entities[type.value](
                db=self.db, response=r[1]["FindEntity"]["entities"], type=type.value))
        return result


def load_entities_registry(custom_entities: List[str] = None) -> dict:
    from aperturedb.Polygons import Polygons
    from aperturedb.Images import Images
    from aperturedb.Blobs import Blobs
    from aperturedb.BoundingBoxes import BoundingBoxes
    from aperturedb.Videos import Videos

    known_entities = {
        ObjectType.POLYGON.value: Polygons,
        ObjectType.IMAGE.value: Images,
        ObjectType.VIDEO.value: Videos,
        ObjectType.BOUNDING_BOX.value: BoundingBoxes,
        ObjectType.BLOB.value: Blobs,
    }
    for entity in set(custom_entities):
        if entity not in known_entities:
            known_entities[entity] = Entities
    return known_entities
