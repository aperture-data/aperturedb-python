from aperturedb.Entities import load_entities_registry
from aperturedb.Query import ObjectType
from aperturedb.Frames import Frames
import pytest


def test_load_entities_registry_frames():
    registry = load_entities_registry()
    assert ObjectType.FRAME.value in registry
    assert registry[ObjectType.FRAME.value] is Frames
