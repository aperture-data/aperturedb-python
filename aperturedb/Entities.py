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
                 spec: Query,
                 with_adjacent: Dict[str, Query] = None
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

        # Since adjacent items are usually a way to filter the results,
        # the native query is constructed in the reverse order, with
        # first filtering out the relevant itmes based on adjacent items.
        fs = None
        count = 0
        if with_adjacent:
            for k, v in with_adjacent.items():
                if fs is None:
                    fs = v
                else:
                    fs = fs.connected_to(v)
                count += 1
            # Eventually, connect the specification of Images to the specification of the adjacent items.
            fs = fs.connected_to(spec)
        else:
            fs = spec

        spec = fs

        cls.known_entities = load_entities_registry(
            custom_entities=spec.command_properties(prop="with_class"))
        cls.db = db

        query = spec.query()
        print(f"query={query}")
        res, r, b = execute_batch(query, [], db)
        if res > 0:
            print(f"resp={r}")
        results = []
        for wc, req, blobs, resp in zip(
                spec.command_properties(prop="with_class"),
                spec.command_properties(prop="find_command"),
                spec.command_properties(prop="blobs"),
                r):
            subresponse = resp[req]['entities']
            if not isinstance(subresponse, list):
                flattened = []
                for previtem in results[-1]:
                    flattened.extend(subresponse[previtem["_uniqueid"]])
                subresponse = flattened
            try:
                entities = cls.known_entities[wc](
                    db=db, response=subresponse, type=wc)
                entities.blobs = blobs
                if wc[0] == "_":
                    entities.find_command = f"Find{wc[1:]}"
                results.append(entities)
            except Exception as e:
                print(e)
                print(cls.known_entities)
                raise e

        cls.__postprocess__(entities=results[-1], with_adjacent=with_adjacent)
        return results

    # This needs to be defined so that the application can access the adjacent items,
    # with every item of this iterable.
    @classmethod
    def __decorator(cls, index, adjacent):
        item = {}
        for k, v in adjacent.items():
            item[k] = v[index]
        return item

    @classmethod
    def __postprocess__(cls, entities: Entities, with_adjacent: object = None):
        adjacent = {}
        if with_adjacent:
            for k, v in with_adjacent.items():
                adjacent[k] = entities.get_connected_entities(
                    etype=v.with_class,
                    constraints=v.constraints)

            entities.decorator = cls.__decorator
            entities.adjacent = adjacent

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

    def inspect(self, **kwargs) -> pd.DataFrame:
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
        entity_class = etype.value if isinstance(etype, ObjectType) else etype
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
                        "with_class": entity_class,
                        "constraints": constraints.constraints,
                        "results": {
                            "all_properties": True
                        }
                    }
                }
            ]
            res, r, b = execute_batch(query, [], self.db)
            cl = Entities
            if entity_class in self.known_entities:
                cl = self.known_entities[entity_class]
            result.append(cl(
                db=self.db, response=r[1]["FindEntity"]["entities"], type=entity_class))
        return result

    def get_blob(self, entity) -> Any:
        """
        Helper to get blobs for FindImage, FindVideo and FindBlob commands.
        """
        query = [
            {
                self.find_command: {
                    "constraints": {
                        "_uniqueid": ["==", entity["_uniqueid"]]
                    },
                    "blobs": True,
                    "uniqueids": True,
                    "results": {
                        "count": True
                    }
                }
            }
        ]
        res, r, b = execute_batch(query, [], self.db)
        return b[0]


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
