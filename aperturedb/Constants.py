"""
Shared constants for the ApertureDB Python SDK.
"""

BLOB_ADD_COMMANDS = frozenset({
    "AddImage",
    "AddDescriptor",
    "AddVideo",
    "AddBlob",
    "AddFrame"
})

BLOB_FIND_COMMANDS = frozenset({
    "FindImage",
    "FindDescriptor",
    "FindVideo",
    "FindBlob",
    "FindFrame",
    "FindBoundingBox"
})

OPENCV_DECODE_FIND_COMMANDS = frozenset({
    "FindImage",
    "FindFrame"
})

PROPERTY_ADD_COMMANDS = frozenset({
    "AddImage",
    "AddVideo",
    "AddBoundingBox",
    "AddPolygon",
    "AddFrame"
})
