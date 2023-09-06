from __future__ import annotations

from aperturedb.Entities import Entities


class BoundingBoxes(Entities):
    """
    **The object mapper representation of bounding boxes in ApertureDB.**

    This class is a layer on top of the native query.
    It facilitate interactions with bounding boxes in the database in pythonic way.
    """
    db_object = "_BoundingBox"
