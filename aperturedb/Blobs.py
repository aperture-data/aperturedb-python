from __future__ import annotations

from aperturedb.Entities import Entities


class Blobs(Entities):
    """
    **The object mapper representation of blobs in ApertureDB.**

    This class is a layer on top of the native query.
    It facilitates interactions with blobs in the database in the pythonic way.
    """
    db_object = "_Blob"
