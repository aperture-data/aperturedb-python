from __future__ import annotations

from aperturedb.Entities import Entities


class Blobs(Entities):
    """
    **The object mapper representation of blobs in ApertureDB.**

    This class is a layer on top of the native query.
    It facilitate interactions with blobs in the database in pythonic way.
    """
    db_object = "_Blob"
