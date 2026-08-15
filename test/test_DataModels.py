from aperturedb.DataModels import FrameDataModel
from aperturedb.Query import ObjectType
from pydantic import ValidationError
import pytest

def test_FrameDataModel():
    # url is optional to avoid breaking existing users
    frame_no_url = FrameDataModel()
    assert frame_no_url.url is None

    # Verify that when url is provided, the model instantiates correctly
    frame = FrameDataModel(url="http://example.com/frame.jpg")
    assert frame.url == "http://example.com/frame.jpg"
    assert frame.type == ObjectType.FRAME
    assert frame.id is not None  # Should have a default UUID generated
