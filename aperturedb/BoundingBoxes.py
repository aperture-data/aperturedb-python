from __future__ import annotations

from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query


class BoundingBoxes(Entities):
    db_object = "_BoundingBox"

    @classmethod
    def retrieve(cls, db: Connector, spec: Query) -> BoundingBoxes:
        return super().retrieve(db, spec)[-1]
