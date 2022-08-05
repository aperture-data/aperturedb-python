
from __future__ import annotations
from typing import List
from aperturedb.Connector import Connector
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
from aperturedb.Sort import Sort


class Polygons(Entities):
    db_object = "_Polygon"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 constraints: Constraints = None,
                 limit: int = -1,
                 sort: Sort = None,
                 list: List[str] = None
                 ) -> Polygons:
        polygons = Entities.retrieve(
            db=db,
            with_class=cls.db_object,
            constraints=constraints,
            limit=limit,
            sort=sort,
            list=list)
        return polygons
