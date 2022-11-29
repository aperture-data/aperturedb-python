from __future__ import annotations
from typing import Dict

from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query


class Blobs(Entities):
    db_object = "_Blob"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query,
                 with_adjacent: Dict[str, Query] = None) -> Blobs:
        spec.with_class = cls.db_object

        results = Entities.retrieve(
            db=db, spec=spec, with_adjacent=with_adjacent)

        blobs = results[-1]
        return blobs
