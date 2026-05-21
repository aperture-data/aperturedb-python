import pytest
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.bounding_box_properties import BoundingBoxProperties

class DummyData:
    def __init__(self, data):
        self._data = data
    def __getitem__(self, i):
        return self._data[i]
    def __len__(self):
        return len(self._data)

def test_variable_annotation_counts():
    # Item 0: 1 BBox
    # Item 1: 0 BBoxes
    # Item 2: 2 BBoxes, 1 Polygon
    # Item 3: 0 BBoxes, 2 Polygons
    
    data = [
        # Item 0
        ([
            {"AddImage": {}},
            {"AddBoundingBox": {}}
        ], []),
        # Item 1
        ([
            {"AddImage": {}}
        ], []),
        # Item 2
        ([
            {"AddImage": {}},
            {"AddBoundingBox": {}},
            {"AddBoundingBox": {}},
            {"AddPolygon": {}}
        ], []),
        # Item 3
        ([
            {"AddImage": {}},
            {"AddPolygon": {}},
            {"AddPolygon": {}}
        ], [])
    ]
    
    dummy_data = DummyData(data)
    
    # Test CommonProperties
    cp = CommonProperties(dummy_data, adb_data_source="test_source")
    
    # Process all items
    for i in range(len(data)):
        res = cp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddImage", "AddBoundingBox", "AddPolygon"]:
                assert cmd[cmd_name]["properties"]["adb_data_source"] == "test_source"

    # Test BoundingBoxProperties
    bbp = BoundingBoxProperties(dummy_data, annotation_source="test_anno", annotation_mode="auto")
    for i in range(len(data)):
        res = bbp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddBoundingBox", "AddPolygon"]:
                assert cmd[cmd_name]["properties"]["annotation_source"] == "test_anno"
                assert cmd[cmd_name]["properties"]["annotation_mode"] == "auto"
            elif cmd_name == "AddImage":
                assert "properties" not in cmd[cmd_name] or "annotation_source" not in cmd[cmd_name]["properties"]

