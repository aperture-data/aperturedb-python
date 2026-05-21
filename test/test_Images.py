import numpy as np
from aperturedb.Images import Images, resolve, rotate
from unittest.mock import patch


def test_rotate():
    points = np.array([(10, 10), (20, 20)])
    rotated = rotate(points, 90, c_x=10, c_y=10)
    assert len(rotated) == 2
    assert rotated[0][0] == 10 and rotated[0][1] == 10
    assert rotated[1][0] == 0 and rotated[1][1] == 20


def test_resolve_resize():
    points = np.array([[10, 10], [20, 20]], dtype=float)
    meta = {"adb_image_width": 100, "adb_image_height": 100}
    operations = [{"type": "resize", "width": 50, "height": 50}]
    resolved = resolve(points, meta, operations)
    assert resolved[0][0] == 5
    assert resolved[0][1] == 5
    assert resolved[1][0] == 10
    assert resolved[1][1] == 10


def test_resolve_rotate():
    points = np.array([[10, 10]], dtype=float)
    meta = {"adb_image_width": 100, "adb_image_height": 100}
    operations = [{"type": "rotate", "angle": 90}]
    resolved = resolve(points, meta, operations)
    assert len(resolved) == 1
    assert resolved[0][0] == 90
    assert resolved[0][1] in (9, 10)  # Account for floating point truncation differences


class MockClient:
    def __init__(self):
        self.responses = []
        self.queries = []

    def query(self, q, blobs=None):
        if blobs is None:
            blobs = []
        self.queries.append(q)
        return self.responses.pop(0) if self.responses else ([{}], [])

    def last_query_ok(self):
        return True


def test_Images_init():
    client = MockClient()
    img = Images(client)
    assert img.client == client
    assert img.db_object.value == "_Image"


def test_Images_search():
    client = MockClient()
    with patch('aperturedb.Images.execute_query') as mock_execute:
        mock_execute.return_value = (
            0, [{"FindImage": {"entities": [{"_uniqueid": "123"}, {"_uniqueid": "456"}]}}], [])
        img = Images(client)
        img.search(limit=2)
        assert "123" in img.images_ids
        assert "456" in img.images_ids
        mock_execute.assert_called_once()
        query_passed = mock_execute.call_args[1][
            "query"] if "query" in mock_execute.call_args[1] else mock_execute.call_args[0][1]
        assert "FindImage" in query_passed[0]
        assert query_passed[0]["FindImage"]["results"]["limit"] == 2


def test_Images_search_by_property():
    client = MockClient()
    with patch('aperturedb.Images.execute_query') as mock_execute:
        mock_execute.return_value = (
            0, [{"FindImage": {"entities": [{"_uniqueid": "789"}]}}], [])
        img = Images(client)
        img.search_by_property("label", ["test_label"])
        assert "789" in img.images_ids
        query_passed = mock_execute.call_args[1][
            "query"] if "query" in mock_execute.call_args[1] else mock_execute.call_args[0][1]
        assert "constraints" in query_passed[0]["FindImage"]


def test_Images_get_image_by_index():
    client = MockClient()
    img = Images(client)
    img.images_ids = ["111"]

    with patch('aperturedb.Images.execute_query') as mock_execute:
        mock_execute.return_value = (0, [], [b'fakeimageblob'])
        # Override last_query_ok since MockClient does that
        client.last_query_ok = lambda: True

        res = img.get_image_by_index(0)
        assert res == b'fakeimageblob'
        assert "111" in img.images


def test_Images_get_np_image_by_index():
    client = MockClient()
    img = Images(client)
    img.images_ids = ["111"]

    with patch('aperturedb.Images.execute_query') as mock_execute:
        # Create a small valid jpeg or png mock blob
        import cv2
        fake_np = np.zeros((10, 10, 3), dtype=np.uint8)
        _, fake_blob = cv2.imencode('.jpg', fake_np)

        mock_execute.return_value = (0, [], [fake_blob.tobytes()])
        client.last_query_ok = lambda: True

        res = img.get_np_image_by_index(0)
        assert res.shape == (10, 10, 3)
