from aperturedb.Frames import Frames
from aperturedb.Query import ObjectType
from unittest.mock import MagicMock


def test_Frames_init():
    client = MagicMock()
    frames = Frames(client)
    assert frames.client == client
    assert frames.db_object == ObjectType.FRAME


def test_Frames_backward_compatibility_import():
    # Verify that importing Frames from aperturedb.Images resolves correctly
    import aperturedb.Images
    from aperturedb.Images import Frames as ImagesFrames

    assert ImagesFrames is Frames

    # Verify it is cached in the module's globals
    assert "Frames" in aperturedb.Images.__dict__
    assert aperturedb.Images.__dict__["Frames"] is Frames
