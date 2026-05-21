from unittest.mock import patch
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.bounding_box_properties import BoundingBoxProperties
from aperturedb.transformers.video_properties import VideoProperties
import hashlib

class DummyData:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, i):
        return self._data[i]

    def __len__(self):
        return len(self._data)


def test_variable_annotation_counts():
    data = [
        ([{"AddImage": {}}, {"AddBoundingBox": {}}], []),
        ([{"AddImage": {}}], []),
        ([{"AddImage": {}}, {"AddBoundingBox": {}}, {"AddBoundingBox": {}}, {"AddPolygon": {}}], []),
        ([{"AddImage": {}}, {"AddPolygon": {}}, {"AddPolygon": {}}], [])
    ]
    dummy_data = DummyData(data)

    cp = CommonProperties(dummy_data, adb_data_source="test_source")
    for i in range(len(data)):
        res = cp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddImage", "AddBoundingBox", "AddPolygon"]:
                assert cmd[cmd_name]["properties"]["adb_data_source"] == "test_source"

    bbp = BoundingBoxProperties(
        dummy_data, annotation_source="test_anno", annotation_mode="auto")
    for i in range(len(data)):
        res = bbp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddBoundingBox", "AddPolygon"]:
                assert cmd[cmd_name]["properties"]["annotation_source"] == "test_anno"
                assert cmd[cmd_name]["properties"]["annotation_mode"] == "auto"
            elif cmd_name == "AddImage":
                assert "properties" not in cmd[cmd_name] or "annotation_source" not in cmd[cmd_name]["properties"]


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_video_properties(mock_get_utils):
    mock_utils = mock_get_utils.return_value
    mock_utils.get_indexed_props.return_value = []
    
    dummy_video_data = b"fake_video_blob_content"
    data = [
        ([
            {"AddVideo": {}},
            {"AddBoundingBox": {}}
        ], [dummy_video_data]),
        ([
            {"AddBoundingBox": {}}
        ], []),
        ([
            {"AddImage": {}},
            {"AddVideo": {}}
        ], [b"image_blob", dummy_video_data]),
    ]
    
    dummy_data = DummyData(data)
    vp = VideoProperties(dummy_data)
    
    for i in range(len(data)):
        res = vp[i]
        blob_index = 0
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name == "AddVideo":
                props = cmd["AddVideo"]["properties"]
                assert props["adb_video_size"] == len(dummy_video_data)
                assert props["adb_video_sha256"] == hashlib.sha256(dummy_video_data).hexdigest()
                assert "adb_video_id" in props
            if cmd_name in ["AddImage", "AddVideo", "AddBlob", "AddDescriptor"]:
                blob_index += 1
