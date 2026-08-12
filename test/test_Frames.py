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
