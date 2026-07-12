import pytest
from unittest.mock import patch, MagicMock
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.bounding_box_properties import BoundingBoxProperties
from aperturedb.transformers.video_properties import VideoProperties
from aperturedb.transformers.image_properties import ImageProperties
from aperturedb.transformers.transformer import Transformer
import hashlib
import struct


class DummyData:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, i):
        return self._data[i]

    def __len__(self):
        return len(self._data)


def test_variable_annotation_counts():
    data_orig = [
        ([{"AddImage": {}}, {"AddBoundingBox": {}}], [b"dummy_image"]),
        ([{"AddImage": {}}], [b"dummy_image"]),
        ([{"AddImage": {}}, {"AddBoundingBox": {}}, {
         "AddBoundingBox": {}}, {"AddPolygon": {}}], [b"dummy_image"]),
        ([{"AddImage": {}}, {"AddPolygon": {}},
         {"AddPolygon": {}}], [b"dummy_image"]),
        ([{"AddVideo": {}}, {"AddBoundingBox": {}}], [b"dummy_video"])
    ]
    import copy

    data = copy.deepcopy(data_orig)
    dummy_data = DummyData(data)

    cp = CommonProperties(
        dummy_data,
        adb_data_source="test_source",
        adb_timestamp="2026-05-24",
        adb_main_object="test_object"
    )
    for i in range(len(data)):
        res = cp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddImage", "AddBoundingBox", "AddPolygon", "AddVideo"]:
                assert cmd[cmd_name]["properties"]["adb_data_source"] == "test_source"
                assert cmd[cmd_name]["properties"]["adb_timestamp"] == "2026-05-24"
                assert cmd[cmd_name]["properties"]["adb_main_object"] == "test_object"

    data_bbp = copy.deepcopy(data_orig)
    dummy_data_bbp = DummyData(data_bbp)
    bbp = BoundingBoxProperties(
        dummy_data_bbp, annotation_source="test_anno", annotation_mode="auto")
    for i in range(len(data_bbp)):
        res = bbp[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddBoundingBox", "AddPolygon"]:
                assert cmd[cmd_name]["properties"]["annotation_source"] == "test_anno"
                assert cmd[cmd_name]["properties"]["annotation_mode"] == "auto"
            elif cmd_name in ["AddImage", "AddVideo"]:
                assert "properties" not in cmd[cmd_name] or \
                    "annotation_source" not in cmd[cmd_name]["properties"]

    # Test empty or missing annotations
    data_empty = copy.deepcopy(data_orig)
    dummy_data_empty = DummyData(data_empty)
    bbp_empty = BoundingBoxProperties(
        dummy_data_empty, annotation_source=None, annotation_mode=None)
    for i in range(len(data_empty)):
        res = bbp_empty[i]
        for cmd in res[0]:
            cmd_name = list(cmd.keys())[0]
            if cmd_name in ["AddBoundingBox", "AddPolygon"]:
                assert "properties" not in cmd[cmd_name] or \
                    "annotation_source" not in cmd[cmd_name].get("properties", {})


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_video_properties(mock_get_utils):
    mock_utils = mock_get_utils.return_value
    mock_utils.get_indexed_props.return_value = []

    dummy_video_data = b"fake_video_blob_content"
    data = [
        ([
            {"AddVideo": {"properties": {"id": "test_id"}}},
            {"AddBoundingBox": {}}
        ], [dummy_video_data]),
        ([
            {"AddBoundingBox": {}}
        ], []),
        ([
            {"AddImage": {}},
            {"AddVideo": {}}
        ], [b"image_blob", dummy_video_data]),
        ([
            {"AddVideo": {}}
        ], [dummy_video_data]),
    ]

    dummy_data = DummyData(data)
    vp = VideoProperties(dummy_data)

    # Verify index creation
    mock_utils.get_indexed_props.assert_called_with("_Video")
    mock_utils.create_entity_index.assert_called_with(
        "_Video", "adb_data_source")

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
                if "id" in props and props["id"] == "test_id":
                    assert props["adb_video_id"] == "test_id"
                else:
                    # uuid hex is 32 chars
                    assert len(props["adb_video_id"]) == 32


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
@patch('aperturedb.transformers.image_properties.Image.open')
def test_image_properties(mock_image_open, mock_get_utils):
    mock_utils = mock_get_utils.return_value
    mock_utils.get_indexed_props.return_value = []

    mock_pil = MagicMock()
    mock_pil.width = 800
    mock_pil.height = 600
    mock_pil.__enter__.return_value = mock_pil
    mock_image_open.return_value = mock_pil

    dummy_image_data = b"fake_image_blob_content"
    data = [
        ([
            {"AddImage": {"_ref": 1, "properties": {"id": "test_image_id"}}},
            {"AddVideo": {}}
        ], [dummy_image_data, b"video_blob"]),
        ([
            {"AddBoundingBox": {}}
        ], []),
        ([
            {"AddVideo": {}},
            {"AddImage": {"_ref": 2}}
        ], [b"video_blob", dummy_image_data]),
        ([
            {"AddImage": {"_ref": 3}}
        ], [dummy_image_data]),
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
                if "id" in props and props["id"] == "test_image_id":
                    assert props["adb_image_id"] == "test_image_id"
                else:
                    # uuid hex is 32 chars
                    assert len(props["adb_image_id"]) == 32


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_clip_pytorch_embeddings(mock_get_utils):
    # Mock the internal generate_embedding dynamically
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding
        mock_utils = mock_get_utils.return_value

        dummy_image_data = b"fake_image_blob_content"
        data = [
            ([
                {"AddImage": {"_ref": 1}},
                {"AddVideo": {}}
            ], [dummy_image_data, b"video_blob"]),
            ([
                {"AddImage": {"_ref": 2}}
            ], [])
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
        assert res[1][-1] == dummy_embedding

        # Test malformed data
        res2 = clip[1]
        assert len(res2[0]) == 1  # AddImage only, Descriptor skipped
        assert len(res2[1]) == 0  # No blobs


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_facenet_pytorch_embeddings(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        dummy_embedding = struct.pack('<4f', 0.5, 0.6, 0.7, 0.8)
        mock_get_embedding.return_value = dummy_embedding
        mock_utils = mock_get_utils.return_value

        dummy_image_data = b"fake_image_blob_content"
        data = [
            ([
                {"AddVideo": {}},
                {"AddImage": {"_ref": 2}}
            ], [b"video_blob", dummy_image_data]),
            ([
                {"AddImage": {"_ref": 3}}
            ], [])
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
        assert res[1][-1] == dummy_embedding

        # Test malformed data
        res2 = facenet[1]
        assert len(res2[0]) == 1  # AddImage only, Descriptor skipped
        assert len(res2[1]) == 0  # No blobs


def test_base_transformer():
    data = [
        ([{"AddImage": {}}], [b"dummy"])
    ]
    dummy_data = DummyData(data)
    transformer = Transformer(dummy_data)

    assert len(transformer) == 1
    assert transformer._queries == 1
    assert transformer._blobs == 1
    assert transformer._blob_index == [0]

    # getitem is abstract
    with pytest.raises(NotImplementedError):
        _ = transformer[0]


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_clip_descriptorset_initialization_retry(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value
        # Fail the first time, succeed the second time
        mock_utils.add_descriptorset.side_effect = [False, True]
        mock_utils.get_descriptorset_list.return_value = []

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"]),
            ([{"AddImage": {"_ref": 2}}], [b"image2"])
        ]

        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        # First item: creation fails, should not be initialized
        res1 = clip[0]
        assert mock_utils.add_descriptorset.call_count == 1
        assert not clip._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Second item: creation succeeds, should be initialized
        res2 = clip[1]
        assert mock_utils.add_descriptorset.call_count == 2
        assert clip._descriptorset_initialized
        assert any("AddDescriptor" in c for c in res2[0])


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_clip_descriptorset_initialization_already_exists(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value

        # add_descriptorset returns False, but get_descriptorset_list shows it already exists
        mock_utils.add_descriptorset.return_value = False
        mock_utils.get_descriptorset_list.return_value = ["ViT-B/16"]

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"])
        ]

        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        # Should initialize successfully
        res1 = clip[0]
        assert clip._descriptorset_initialized
        assert any("AddDescriptor" in c for c in res1[0])


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_clip_descriptorset_initialization_backend_error(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value

        # Simulation 1: add_descriptorset returns False, get_descriptorset_list raises exception (e.g. backend transient error)
        mock_utils.add_descriptorset.return_value = False
        mock_utils.get_descriptorset_list.side_effect = Exception(
            "DB Connection Lost")

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"]),
            ([{"AddImage": {"_ref": 2}}], [b"image2"])
        ]

        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        # First item: creation fails, list raises, exception caught, initialized remains False
        res1 = clip[0]
        assert not clip._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Simulation 2: Now add_descriptorset succeeds
        mock_utils.add_descriptorset.return_value = True
        res2 = clip[1]
        assert clip._descriptorset_initialized
        assert "AddDescriptor" in res2[0][-1]


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_clip_descriptorset_initialization_add_backend_error(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value

        # Simulation 1: add_descriptorset raises exception directly
        mock_utils.add_descriptorset.side_effect = Exception(
            "DB Connection Lost during add")
        mock_utils.get_descriptorset_list.return_value = []

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"]),
            ([{"AddImage": {"_ref": 2}}], [b"image2"])
        ]

        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        # First item: creation fails by raising, exception caught, initialized remains False
        res1 = clip[0]
        assert not clip._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Simulation 2: Now add_descriptorset succeeds
        mock_utils.add_descriptorset.side_effect = None
        mock_utils.add_descriptorset.return_value = True
        res2 = clip[1]
        assert clip._descriptorset_initialized
        assert "AddDescriptor" in res2[0][-1]


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_clip_descriptorset_initialization_get_utils_error(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"dummy_blob_1"])
        ]
        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        # Simulate get_utils raising an error
        mock_get_utils.side_effect = ConnectionError("Backend unavailable")

        res = clip[0]
        assert not clip._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res[0])


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_clip_embedding_generation_error(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        mock_utils = MagicMock()
        mock_utils.add_descriptorset.return_value = True
        mock_get_utils.return_value = mock_utils

        # First item fails during embedding generation
        mock_generate_embedding.side_effect = RuntimeError("Bad blob")

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"dummy_blob_1"]),
            ([{"AddImage": {"_ref": 2}}], [b"dummy_blob_2"])
        ]
        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        res1 = clip[0]
        assert not clip._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Second item succeeds
        mock_generate_embedding.side_effect = None
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.return_value = dummy_embedding

        res2 = clip[1]
        assert clip._descriptorset_initialized
        assert "AddDescriptor" in res2[0][-1]


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_clip_embedding_generation_error_after_init(mock_get_utils):
    try:
        from aperturedb.transformers.clip_pytorch_embeddings import CLIPPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for CLIP")

    with patch('aperturedb.transformers.clip_pytorch_embeddings.generate_embedding') as mock_generate_embedding:
        mock_utils = MagicMock()
        mock_utils.add_descriptorset.return_value = True
        mock_get_utils.return_value = mock_utils

        data = [
            ([{"AddImage": {"_ref": 1}}, {"AddImage": {"_ref": 2}}],
             [b"dummy_blob_1", b"dummy_blob_bad"])
        ]
        dummy_data = DummyData(data)
        clip = CLIPPyTorchEmbeddings(dummy_data)

        # First generate_embedding succeeds, second fails
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_generate_embedding.side_effect = [
            dummy_embedding, RuntimeError("Bad blob")]

        res = clip[0]
        assert clip._descriptorset_initialized
        # Only one descriptor should be added
        descriptors = [c for c in res[0] if "AddDescriptor" in c]
        assert len(descriptors) == 1


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
def test_facenet_descriptorset_initialization_retry(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value
        # Fail both add and get first time, then succeed get the second time
        mock_utils.add_descriptorset.return_value = False
        mock_utils.get_descriptorset_list.side_effect = [
            [], ["facenet_pytorch_embeddings"]]

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"]),
            ([{"AddImage": {"_ref": 2}}], [b"image2"])
        ]

        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        res1 = facenet[0]
        assert mock_utils.add_descriptorset.call_count == 1
        assert not facenet._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        res2 = facenet[1]
        assert mock_utils.add_descriptorset.call_count == 2
        assert facenet._descriptorset_initialized
        assert any("AddDescriptor" in c for c in res2[0])


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_facenet_descriptorset_initialization_already_exists(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value

        # add_descriptorset returns False, but get_descriptorset_list shows it already exists
        mock_utils.add_descriptorset.return_value = False
        mock_utils.get_descriptorset_list.return_value = [
            "facenet_pytorch_embeddings"]

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"])
        ]

        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        # Should initialize successfully
        res1 = facenet[0]
        assert facenet._descriptorset_initialized
        assert any("AddDescriptor" in c for c in res1[0])


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_facenet_descriptorset_initialization_backend_error(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value

        # Simulation 1: add_descriptorset returns False, get_descriptorset_list raises exception (backend error)
        mock_utils.add_descriptorset.return_value = False
        mock_utils.get_descriptorset_list.side_effect = Exception(
            "DB Connection Lost")

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"]),
            ([{"AddImage": {"_ref": 2}}], [b"image2"])
        ]

        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        # First item: creation fails, exception caught, initialized remains False
        res1 = facenet[0]
        assert not facenet._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Simulation 2: Now add_descriptorset succeeds
        mock_utils.add_descriptorset.return_value = True
        res2 = facenet[1]
        assert facenet._descriptorset_initialized
        assert "AddDescriptor" in res2[0][-1]


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_facenet_descriptorset_initialization_add_backend_error(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.return_value = dummy_embedding

        mock_utils = mock_get_utils.return_value

        # Simulation 1: add_descriptorset raises exception directly
        mock_utils.add_descriptorset.side_effect = Exception(
            "DB Connection Lost during add")
        mock_utils.get_descriptorset_list.return_value = []

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"image1"]),
            ([{"AddImage": {"_ref": 2}}], [b"image2"])
        ]

        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        # First item: creation fails by raising, exception caught, initialized remains False
        res1 = facenet[0]
        assert not facenet._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Simulation 2: Now add_descriptorset succeeds
        mock_utils.add_descriptorset.side_effect = None
        mock_utils.add_descriptorset.return_value = True
        res2 = facenet[1]
        assert facenet._descriptorset_initialized
        assert "AddDescriptor" in res2[0][-1]


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_facenet_descriptorset_initialization_get_utils_error(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.return_value = dummy_embedding

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"dummy_blob_1"])
        ]
        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        # Simulate get_utils raising an error
        mock_get_utils.side_effect = ConnectionError("Backend unavailable")

        res = facenet[0]
        assert not facenet._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res[0])


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_facenet_embedding_generation_error(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        mock_utils = MagicMock()
        mock_utils.add_descriptorset.return_value = True
        mock_get_utils.return_value = mock_utils

        # First item fails during embedding generation
        mock_get_embedding.side_effect = RuntimeError("Bad blob")

        data = [
            ([{"AddImage": {"_ref": 1}}], [b"dummy_blob_1"]),
            ([{"AddImage": {"_ref": 2}}], [b"dummy_blob_2"])
        ]
        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        res1 = facenet[0]
        assert facenet._descriptorset_initialized
        assert not any("AddDescriptor" in c for c in res1[0])

        # Second item succeeds
        mock_get_embedding.side_effect = None
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.return_value = dummy_embedding

        res2 = facenet[1]
        assert facenet._descriptorset_initialized
        assert "AddDescriptor" in res2[0][-1]


@patch("aperturedb.transformers.transformer.Transformer.get_utils")
def test_facenet_embedding_generation_error_after_init(mock_get_utils):
    try:
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
    except (ImportError, SystemExit):
        pytest.skip("Missing deps for Facenet")

    with patch('aperturedb.transformers.facenet_pytorch_embeddings.FacenetPyTorchEmbeddings._get_embedding_from_blob') as mock_get_embedding:
        mock_utils = MagicMock()
        mock_utils.add_descriptorset.return_value = True
        mock_get_utils.return_value = mock_utils

        data = [
            ([{"AddImage": {"_ref": 1}}, {"AddImage": {"_ref": 2}}],
             [b"dummy_blob_1", b"dummy_blob_bad"])
        ]
        dummy_data = DummyData(data)
        facenet = FacenetPyTorchEmbeddings(dummy_data)

        # First generate_embedding succeeds, second fails
        dummy_embedding = struct.pack('<4f', 0.1, 0.2, 0.3, 0.4)
        mock_get_embedding.side_effect = [
            dummy_embedding, RuntimeError("Bad blob")]

        res = facenet[0]
        assert facenet._descriptorset_initialized
        # Only one descriptor should be added
        descriptors = [c for c in res[0] if "AddDescriptor" in c]
        assert len(descriptors) == 1


def test_common_properties_early_return():
    data = [([{"AddImage": {}}], [b"dummy"])]
    dummy_data = DummyData(data)
    cp = CommonProperties(dummy_data, adb_data_source=None,
                          adb_timestamp=None, adb_main_object=None)
    res = cp[0]
    assert "properties" not in res[0][0]["AddImage"]


def test_bounding_box_properties_early_return():
    data = [([{"AddBoundingBox": {}}], [])]
    dummy_data = DummyData(data)
    bbp = BoundingBoxProperties(
        dummy_data, annotation_source=None, annotation_mode=None)
    res = bbp[0]
    assert "properties" not in res[0][0]["AddBoundingBox"]


@patch('aperturedb.transformers.transformer.Transformer.get_utils')
@patch('aperturedb.transformers.video_properties.hashlib.sha256')
def test_video_properties_exception_handling(mock_sha256, mock_get_utils):
    mock_utils = mock_get_utils.return_value
    mock_utils.get_indexed_props.return_value = []

    mock_sha256.side_effect = Exception("Test Exception")

    dummy_video_data = b"fake_video_blob_content"
    data = [
        ([
            {"AddVideo": {}}
        ], [dummy_video_data])
    ]

    dummy_data = DummyData(data)
    vp = VideoProperties(dummy_data)

    res = vp[0]

    assert "adb_video_sha256" not in res[0][0]["AddVideo"]["properties"]


@patch('aperturedb.transformers.bounding_box_properties.logger')
def test_bounding_box_properties_exception_handling(mock_logger):
    # Pass a malformed command dictionary where "AddBoundingBox" is not a dict
    data = [([{"AddBoundingBox": "invalid_type_not_dict"}], [])]
    dummy_data = DummyData(data)

    bbp = BoundingBoxProperties(
        dummy_data, annotation_source="test_anno", annotation_mode="auto")

    # This should raise an AttributeError when calling setdefault on a string
    # But the exception should be caught and logged
    res = bbp[0]

    assert mock_logger.exception.called
    assert "Error applying bounding box properties" in mock_logger.exception.call_args[0][0]
    # The original command should remain unchanged
    assert res[0][0]["AddBoundingBox"] == "invalid_type_not_dict"


@patch('aperturedb.transformers.image_properties.logger')
@patch('aperturedb.transformers.transformer.Transformer.get_utils')
@patch('aperturedb.transformers.image_properties.Image.open')
def test_image_properties_exception_handling(mock_image_open, mock_get_utils, mock_logger):
    mock_utils = mock_get_utils.return_value
    mock_utils.get_indexed_props.return_value = []

    # Make Image.open raise an exception
    mock_image_open.side_effect = Exception("Image load error")

    dummy_image_data = b"fake_image_blob_content"
    data = [([{"AddImage": {}}], [dummy_image_data])]
    dummy_data = DummyData(data)

    ip = ImageProperties(dummy_data)
    res = ip[0]

    assert mock_logger.exception.called
    assert "Error applying image properties" in mock_logger.exception.call_args[0][0]

    props = res[0][0]["AddImage"]["properties"]
    # size, sha256, and id are computed before Image.open, so they should be present
    assert "adb_image_size" in props
    assert "adb_image_sha256" in props
    assert "adb_image_id" in props
    # width and height are computed after Image.open, so they should be missing
    assert "adb_image_width" not in props
    assert "adb_image_height" not in props


@patch('aperturedb.transformers.common_properties.logger')
def test_common_properties_exception_handling(mock_logger):
    # Pass a malformed command dictionary
    data = [([{"AddImage": "invalid_type_not_dict"}], [b"dummy"])]
    dummy_data = DummyData(data)

    cp = CommonProperties(dummy_data, adb_data_source="test_source")
    res = cp[0]

    assert mock_logger.exception.called
    assert "Error applying common properties" in mock_logger.exception.call_args[0][0]
    assert res[0][0]["AddImage"] == "invalid_type_not_dict"


def test_transformers_init():
    import aperturedb.transformers as transformers

    assert transformers.Transformer is not None
    assert transformers.CommonProperties is not None
    assert transformers.ImageProperties is not None
    assert transformers.VideoProperties is not None
    assert transformers.BoundingBoxProperties is not None

    try:
        assert transformers.FacenetPyTorchEmbeddings is not None
    except ImportError:
        pass

    try:
        assert transformers.CLIPPyTorchEmbeddings is not None
    except ImportError:
        pass

    with pytest.raises(AttributeError):
        _ = transformers.NonExistentTransformer
