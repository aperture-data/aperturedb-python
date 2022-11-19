from __future__ import annotations

from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query


class Blobs(Entities):
    db_object = "_Blob"

    @classmethod
    def retrieve(cls, db: Connector, spec: Query) -> Blobs:
        return super().retrieve(db, spec)[-1]
