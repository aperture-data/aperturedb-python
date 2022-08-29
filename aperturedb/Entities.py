from __future__ import annotations
from typing import List

from aperturedb.Subscriptable import Subscriptable
from aperturedb.Constraints import Constraints
from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import execute_batch
from aperturedb.Sort import Sort
import pandas as pd
from PIL import Image
import numpy as np

from enum import Enum


class EntityType(Enum):
    CUSTOM = ""
    IMAGE = "_Image"
    POLYGON = "_Polygon"


class Query():
    db_object = "Entity"
    find_command = f"Find{db_object}"
    update_command = f"Update{db_object}"
    next = None

    def connected_to(self,
                     spec: Query,
                     adj_to: int = 0) -> Query:
        spec.adj_to = self.adj_to + 1 if adj_to == 0 else adj_to
        self.next = spec
        return self

    def commands(self, v=""):
        chain = []
        p = self
        while p is not None:
            chain.append(getattr(p, v))
            p = p.next
        return chain

    @classmethod
    def spec(cls,
             constraints: Constraints = None,
             with_class: EntityType = EntityType.CUSTOM,
             custom_class_name: str = "",
             limit: int = -1,
             sort: Sort = None,
             list: List[str] = None
             ) -> Query:
        return Query(
            constraints=constraints,
            with_class=custom_class_name if with_class == EntityType.CUSTOM else with_class.value,
            limit=limit,
            sort = sort,
            list = list
        )

    def __init__(self,
                 constraints: Constraints = None,
                 with_class: str = "",
                 limit: int = -1,
                 sort: Sort = None,
                 list: List[str] = None,
                 adj_to: int = 0):
        self.constraints = constraints
        self.with_class = with_class
        self.limit = limit
        self.sort = sort
        self.list = list
        self.adj_to = adj_to + 1

    def query(self) -> List[dict]:
        # print(
        #     f"constraints={self.constraints}, limit={self.limit}, sort={self.sort}, list={self.list}")

        results_section = "results"
        cmd = {
            self.find_command: {
                "_ref": self.adj_to,
                results_section: {

                }
            }
        }
        if self.db_object == "Entity":
            cmd[self.find_command]["with_class"] = self.with_class
        if self.limit != -1:
            cmd[self.find_command][results_section]["limit"] = self.limit
        if self.sort:
            cmd[self.find_command][results_section]["sort"] = self.sort._sort
        if self.list is not None and len(self.list) > 0:
            cmd[self.find_command][results_section]["list"] = self.list
        else:
            cmd[self.find_command][results_section]["all_properties"] = True

        if self.constraints:
            cmd[self.find_command]["constraints"] = self.constraints.constraints

        query = [cmd]
        if self.next:
            next_commands = self.next.query()
            next_commands[0][self.next.find_command]["is_connected_to"] = {
                "ref": self.adj_to
            }
            query.extend(next_commands)
        return query


class Entities(Subscriptable):
    db_object = "Entity"
    find_command = f"Find{db_object}"
    update_command = f"Update{db_object}"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query
                 ) -> List[Entities]:
        cls.known_entities = load_entities_registry()
        cls.db = db
        query = spec.query()
        print(f"query={query}")
        res, r, b = execute_batch(query, [], db, None)
        if res > 0:
            print(f"resp={r}")
        results = []
        for wc, req, resp in zip(spec.commands(v="with_class"), spec.commands(v="find_command"), r):
            try:
                # entities = known_entities[wc](resp[req]['entities'])
                entities = cls.known_entities[wc](
                    db=db, response=resp[req]['entities'])
                # entities.response = resp[req]['entities']
                # print(resp[req]['entities'])
                # entities.pre_process()
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

    def pre_process(self) -> None:
        pass

    def getitem(self, idx):
        item = self.response[idx]
        if self.decorator is not None:
            for k, v in self.decorator(idx).items():
                item[k] = v
        if self.get_image == True:
            buffer = self.get_image_by_index(idx)
            if buffer is not None:
                # nparr = np.frombuffer(buffer, dtype=np.uint8)
                item['thumbnail'] = Image.fromarray(
                    self.get_np_image_by_index(idx))
        return item

    def __len__(self):
        return len(self.response)

    def filter(self, predicate):
        return self.known_entities[self.type](db=self.db, response=list(filter(predicate, self.response)), type=self.type)

    def __add__(self, other: Entities) -> Entities:
        return Entities(self.response + other.response)

    def __sub__(self, other: Entities) -> Entities:
        return Entities([x for x in self.response if x not in other.response])

    def sort(self, key) -> Entities:
        return Entities(sorted(self.response, key=key))

    def inspect(self) -> pd.DataFrame:
        return pd.json_normalize([item for item in self])

    def update_properties(self, extra_properties: List[dict]) -> bool:
        for entity, properties in zip(self, extra_properties):
            query = [
                {
                    self.find_command: {
                        "_ref": 1,
                        "constraints": {
                            "_uniqueid": ["==", entity["_uniqueid"]]
                        },
                        "results": {
                            "blobs": False
                        }
                    }
                }, {
                    self.update_command: {
                        "ref": 1,
                        "properties": properties
                    }
                }
            ]
            res, r, b = execute_batch(query, [], self.db, None)
            print(r)
            return None

    def get_connected_entities(self, pk: str, type: EntityType, constraints: Constraints = None) -> List[Entities]:
        """Gets all entities clustered around items of the collection

        Args:
            pk (str): _description_
            type (EntityType): _description_
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
                        "unique": True,
                        "constraints": {
                            pk: ["==", int(entity[pk])]
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
            res, r, b = execute_batch(query, [], self.db, None)

            result.append(self.known_entities[type.value](
                db=self.db, response=r[1]["FindEntity"]["entities"], type=type.value))
        return result


def load_entities_registry():
    from aperturedb.Polygons import Polygons
    from aperturedb.Images import Images

    known_entities = {
        EntityType.POLYGON.value: Polygons,
        EntityType.IMAGE.value: Images
    }
    return known_entities
