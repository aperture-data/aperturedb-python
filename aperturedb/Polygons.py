from __future__ import annotations
from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query
from aperturedb.ParallelQuery import execute_batch


class Polygons(Entities):
    db_object = "_Polygon"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query) -> Polygons:
        polygons = Entities.retrieve(
            db=db,
            spec=spec)
        return polygons[-1]

    def intersection(self, other: Polygons, threshold: float) -> Polygons:
        """
        Find a set of polygons that intersect with another set of polygons.
        The threhold is user specified and is used to determine if two polygons
        sufficiently overlap to be considered intersecting.

        Args:
            other (Polygons): Set of polygons to intersect with.
            threshold (float): The threshold for determining if two polygons are sufficiently intersecting.

        Returns:
            Polygons: uniqued set of polygons that intersect with the other set of polygons.
        """
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
                res, r, b = execute_batch(query, [], self.db)
                if r[2]["RegionIoU"]["IoU"][0][0] > threshold:
                    result.add(int(p1["ann_id"]))
                    result.add(int(p2["ann_id"]))
        return list(result)
