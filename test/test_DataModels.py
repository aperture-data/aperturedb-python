from aperturedb.DataModels import FrameDataModel
from aperturedb.Query import ObjectType
import pytest


def test_FrameDataModel():
    # Verify that url is a required field because it inherits from BlobDataModel
    with pytest.raises(ValueError):
        # This should fail because url is missing
        FrameDataModel()

    # Verify that when url is provided, the model instantiates correctly
    frame = FrameDataModel(url="http://example.com/frame.jpg")
    assert frame.url == "http://example.com/frame.jpg"
    assert frame.type == ObjectType.FRAME
    assert frame.id is not None  # Should have a default UUID generated
