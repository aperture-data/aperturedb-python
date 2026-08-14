from aperturedb.Frames import Frames
from aperturedb.Query import ObjectType


class MockClient:
    def __init__(self):
        pass


def test_Frames_init():
    client = MockClient()
    frame = Frames(client)
    assert frame.client == client
    assert frame.db_object.value == "_Frame"
    assert frame.db_object == ObjectType.FRAME


def test_Frames_backward_compatibility_import():
    # Verify that importing Frames from aperturedb.Images resolves correctly
    import aperturedb.Images
    from aperturedb.Images import Frames as ImagesFrames

    assert ImagesFrames is Frames

    # Verify it is cached in the module's globals
    assert "Frames" in aperturedb.Images.__dict__
    assert aperturedb.Images.__dict__["Frames"] is Frames
