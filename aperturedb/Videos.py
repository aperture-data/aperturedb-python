from __future__ import annotations

from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query


class Videos(Entities):
    db_object = "_Video"

    @classmethod
    def retrieve(cls, db: Connector, spec: Query) -> Videos:
        return super().retrieve(db, spec)[-1]
