import pytest
from unittest.mock import patch, MagicMock
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.bounding_box_properties import BoundingBoxProperties
from aperturedb.transformers.video_properties import VideoProperties
from aperturedb.transformers.image_properties import ImageProperties
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
        ([{"AddImage": {}}, {"AddBoundingBox": {}}, {
         "AddBoundingBox": {}}, {"AddPolygon": {}}], []),
        ([{"AddImage": {}}, {"AddPolygon": {}}, {"AddPolygon": {}}], []),
        ([{"AddVideo": {}}, {"AddBoundingBox": {}}], [])
    ]
    dummy_data = DummyData(data)

    cp = CommonProperties(dummy_data, adb_data_source="test_source")
    for i in range(len(data)):
        res = cp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddImage", "AddBoundingBox", "AddPolygon", "AddVideo"]:
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
            elif cmd_name in ["AddImage", "AddVideo"]:
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
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name == "AddVideo":
                props = cmd["AddVideo"]["properties"]
                assert props["adb_video_size"] == len(dummy_video_data)
                assert props["adb_video_sha256"] == hashlib.sha256(
                    dummy_video_data).hexdigest()
                assert "adb_video_id" in props


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
@patch('aperturedb.transformers.image_properties.Image.open')
def test_image_properties(mock_image_open, mock_get_utils):
    mock_utils = mock_get_utils.return_value
    mock_utils.get_indexed_props.return_value = []

    mock_pil = MagicMock()
    mock_pil.width = 800
    mock_pil.height = 600
    mock_image_open.return_value = mock_pil

    dummy_image_data = b"fake_image_blob_content"
    data = [
        ([
            {"AddImage": {"_ref": 1}},
            {"AddVideo": {}}
        ], [dummy_image_data, b"video_blob"]),
        ([
            {"AddBoundingBox": {}}
        ], []),
        ([
            {"AddVideo": {}},
            {"AddImage": {"_ref": 2}}
        ], [b"video_blob", dummy_image_data]),
    ]

    dummy_data = DummyData(data)
    ip = ImageProperties(dummy_data)

    for i in range(len(data)):
        res = ip[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name == "AddImage":
                props = cmd["AddImage"]["properties"]
                assert props["adb_image_size"] == len(dummy_image_data)
                assert props["adb_image_sha256"] == hashlib.sha256(
                    dummy_image_data).hexdigest()
                assert props["adb_image_width"] == 800
                assert props["adb_image_height"] == 600
                assert "adb_image_id" in props


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_clip_pytorch_embeddings(mock_get_utils):
    # Mock the internal generate_embedding dynamically
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        mock_generate_embedding.return_value = [0.1, 0.2, 0.3, 0.4]
        mock_utils = mock_get_utils.return_value

        dummy_image_data = b"fake_image_blob_content"
        data = [
            ([
                {"AddImage": {"_ref": 1}},
                {"AddVideo": {}}
            ], [dummy_image_data, b"video_blob"])
        ]

        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        res = clip[0]
        assert mock_utils.add_descriptorset.called
        assert len(res[0]) == 3  # AddImage, AddVideo, AddDescriptor

        desc_cmd = [c for c in res[0] if "AddDescriptor" in c]
        assert len(desc_cmd) == 1
        assert desc_cmd[0]["AddDescriptor"]["connect"]["ref"] == 1

        # 2 blobs originally + 1 generated embedding blob
        assert len(res[1]) == 3
        assert res[1][-1] == [0.1, 0.2, 0.3, 0.4]


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_facenet_pytorch_embeddings(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        mock_get_embedding.return_value = [0.5, 0.6, 0.7, 0.8]
        mock_utils = mock_get_utils.return_value

        dummy_image_data = b"fake_image_blob_content"
        data = [
            ([
                {"AddVideo": {}},
                {"AddImage": {"_ref": 2}}
            ], [b"video_blob", dummy_image_data])
        ]

        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        res = facenet[0]
        assert mock_utils.add_descriptorset.called
        assert len(res[0]) == 3  # AddVideo, AddImage, AddDescriptor

        desc_cmd = [c for c in res[0] if "AddDescriptor" in c]
        assert len(desc_cmd) == 1
        assert desc_cmd[0]["AddDescriptor"]["connect"]["ref"] == 2

        assert len(res[1]) == 3
        assert res[1][-1] == [0.5, 0.6, 0.7, 0.8]
