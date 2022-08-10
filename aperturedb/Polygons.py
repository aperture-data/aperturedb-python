
from __future__ import annotations
from typing import List
from aperturedb.Connector import Connector
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
from aperturedb.Sort import Sort
from aperturedb.ParallelQuery import execute_batch


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

    def intersection(self, other: Polygons) -> Polygons:
        result = set()
        for p1 in self:
            for p2 in other:
                query = [
                    {
                        "FindEntity": {
                            "_ref": 1,
                            "unique": True,
                            "constraints": {
                                "_uniqueid": ["==", int(p1["_uniqueid"])]
                            }
                        }
                    }, {
                        "FindEntity": {
                            "_ref": 2,
                            "unique": True,
                            "constraints": {
                                "_uniqueid": ["==", int(p2["_uniqueid"])]
                            }
                        }
                    }, {
                        "RegionIoU": {
                            "roi_1": 1,
                            "roi_2": 2,
                        }
                    }
                ]
                res, r, b = execute_batch(query, [], self.db, None)
                if r[2]["RegionIoU"]["IoU"][0][0] > 0.001:
                    result.add(int(p1["ann_id"]))
                    result.add(int(p2["ann_id"]))
        return list(result)
