"""
**Module providing the Blobs entity wrapper.**

This module contains the `Blobs` class which acts as an object mapper representation of blobs in ApertureDB, 
inheriting from `Entities`.
"""
from __future__ import annotations

from aperturedb.Entities import Entities


class Blobs(Entities):
    """
    **The object mapper representation of blobs in ApertureDB.**

    This class is a layer on top of the native JSON query (e.g., [FindBlob](/query_language/Reference/blob_commands/FindBlob)).
    It facilitates interactions with blobs in the database in the pythonic way.
    """
    db_object = "_Blob"
