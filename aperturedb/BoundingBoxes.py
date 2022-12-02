from __future__ import annotations
from typing import Dict

from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query


class BoundingBoxes(Entities):
    db_object = "_BoundingBox"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query,
                 with_adjacent: Dict[str, Query] = None) -> BoundingBoxes:
        spec.with_class = cls.db_object

        results = Entities.retrieve(
            db=db, spec=spec, with_adjacent=with_adjacent)

        bboxes = results[-1]
        return bboxes
