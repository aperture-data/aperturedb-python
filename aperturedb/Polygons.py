from __future__ import annotations
from typing import List
from aperturedb.Connector import Connector
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities, Query
from aperturedb.Sort import Sort
from aperturedb.ParallelQuery import execute_batch


class Polygons(Entities):
    db_object = "_Polygon"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query
                 ) -> Polygons:
        spec.with_class = cls.db_object
        polygons = Entities.retrieve(
            db=db,
            spec=spec)
        return polygons

    def intersection(self, other: Polygons, threshold: float) -> Polygons:
        result = set()
        for p1 in self:
            for p2 in other:
                query = [
                    {
                        "FindEntity": {
                            "_ref": 1,
                            "unique": True,
                            "constraints": {
                                "_uniqueid": ["==", p1["_uniqueid"]]
                            }
                        }
                    }, {
                        "FindEntity": {
                            "_ref": 2,
                            "unique": True,
                            "constraints": {
                                "_uniqueid": ["==", p2["_uniqueid"]]
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
                if r[2]["RegionIoU"]["IoU"][0][0] > threshold:
                    result.add(int(p1["ann_id"]))
                    result.add(int(p2["ann_id"]))
        return list(result)
