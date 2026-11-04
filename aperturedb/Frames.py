from __future__ import annotations

from aperturedb.Images import Images
from aperturedb.Query import ObjectType


class Frames(Images):
    """
    **The python wrapper of frame images in ApertureDB.**

    Frames in ApertureDB are quite similar to images and so
    are modeled in python as a subclass.


    Args:
        client: The database connector, perhaps as returned by `CommonLibrary.create_connector`
    """
    db_object = ObjectType.FRAME

    def __init__(self, client, batch_size=100, response=None, **kwargs):
        super().__init__(
            client, batch_size=batch_size, response=response, **kwargs)
