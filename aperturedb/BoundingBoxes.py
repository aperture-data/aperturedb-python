from __future__ import annotations

from aperturedb.Entities import Entities


class BoundingBoxes(Entities):
    """
    **The object mapper representation of bounding boxes in ApertureDB.**

    This class is a layer on top of the native query.
    It facilitates interactions with bounding boxes in the database in the pythonic way.
    """
    db_object = "_BoundingBox"
