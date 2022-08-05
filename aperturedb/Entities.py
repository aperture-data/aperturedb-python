from __future__ import annotations
import re
from typing import List

from aperturedb.Subscriptable import Subscriptable
from aperturedb.Constraints import Constraints
from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import execute_batch
from aperturedb.Sort import Sort
import pandas as pd


class Entities(Subscriptable):
    db_object = "Entity"
    find_command = f"Find{db_object}"
    update_command = f"Update{db_object}"
    next = None

    def connected_to(self,
                     other: Entities) -> Entities:
        self.next = other

    @classmethod
    def spec(cls,
             constraints: Constraints = None,
             with_class: str = "",
             limit: int = -1,
             sort: Sort = None,
             list: List[str] = None) -> dict:
        print(
            f"constraints={constraints}, limit={limit}, sort={sort}, list={list}")

        results_section = "results"
        query = {
            cls.find_command: {
                results_section: {

                }
            }
        }
        if cls.db_object == "Entity":
            query[cls.find_command]["with_class"] = with_class
        if limit != -1:
            query[cls.find_command][results_section]["limit"] = limit
        if sort:
            query[cls.find_command][results_section]["sort"] = sort._sort
        if list is not None and len(list) > 0:
            query[cls.find_command][results_section]["list"] = list
        else:
            query[cls.find_command][results_section]["all_properties"] = True

        if constraints:
            query[cls.find_command]["constraints"] = constraints.constraints

        return query

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 constraints: Constraints = None,
                 with_class: str = "",
                 limit: int = -1,
                 sort: Sort = None,
                 list: List[str] = None
                 ) -> Entities:
        cls.db = db
        query = cls.spec(
            constraints=constraints,
            with_class=with_class,
            limit=limit,
            sort=sort,
            list=list)
        print(f"query={query}")
        res, r, b = execute_batch([query], [], db, None)
        if res > 0:
            print(f"resp={r}")

        return Entities(r[0][cls.find_command]['entities'])

    def __init__(self, json) -> None:
        super().__init__()
        self.response = json

    def getitem(self, idx):
        return self.response[idx]

    def __len__(self):
        return len(self.response)

    def filter(self, predicate):
        return Entities(list(filter(predicate, self.response)))

    def __add__(self, other: Entities) -> Entities:
        return Entities(self.response + other.response)

    def __sub__(self, other: Entities) -> Entities:
        return Entities([x for x in self.response if x not in other.response])

    def sort(self, key) -> Entities:
        return Entities(sorted(self.response, key=key))

    def inspect(self) -> pd.DataFrame:
        return pd.json_normalize(self.response)

    def update_properties(self, extra_properties: List[dict]) -> bool:
        for entity, properties in zip(self, extra_properties):
            query = [
                {
                    self.find_command: {
                        "_ref": 1,
                        "constraints": {
                            "_uniqueid": ["==", int(entity["_uniqueid"])]
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
