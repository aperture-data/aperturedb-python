from __future__ import annotations

from aperturedb.Subscriptable import Subscriptable
from aperturedb.Constraints import Constraints
from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import execute_batch
import pandas as pd


class Entities(Subscriptable):
    db_object = "Entity"

    @classmethod
    def retrieve(cls, db: Connector, constraints: Constraints = None, with_class="") -> Entities:
        find_command = f"Find{cls.db_object}"
        query = {find_command: {
            "with_class": with_class,
            "results": {
                "all_properties": True
            }
        }}

        if constraints:
            query[find_command]["constraints"] = constraints.constraints

        res, r, b = execute_batch([query], [], db, None)

        return Entities(r[0][find_command]['entities'])

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

    def display(self) -> pd.DataFrame:
        return pd.json_normalize(self.response)
