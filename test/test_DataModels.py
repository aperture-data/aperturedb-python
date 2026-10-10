from pydantic import ValidationError
from aperturedb.DataModels import FrameDataModel
from aperturedb.Query import ObjectType
import pytest


def test_FrameDataModel():
    # url is required, so instantiating without it should raise a ValidationError
    with pytest.raises(ValidationError):
        FrameDataModel()

    # Verify that when url is provided, the model instantiates correctly
    frame = FrameDataModel(url="http://example.com/frame.jpg")
    assert frame.url == "http://example.com/frame.jpg"
    assert frame.type == ObjectType.FRAME
    assert frame.id is not None  # Should have a default UUID generated
