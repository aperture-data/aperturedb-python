"""
**Data Model Classes to support (pydantic) model based ingestiton.**
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing_extensions import Annotated, List
from typing import ClassVar, Optional
from uuid import uuid4
from aperturedb.Query import ObjectType, PropertyType, RangeType


class IdentityDataModel(BaseModel):
    """Base class for all entities in ApertureDB.
    Generates a default UUID for the entity.
    """

    id: Annotated[str, Field(default_factory=lambda: uuid4().hex)]
    # Change as per the docs for the error
    # https://docs.pydantic.dev/dev-v2/usage/errors/#model-field-overridden
    type: ClassVar[ObjectType] = ObjectType.ENTITY


class BlobDataModel(IdentityDataModel):
    """Base class for all blob entities in ApertureDB.
    """
    url: Annotated[str, Field(
        title="URL", description="URL to file, http, s3 or gs resource")]
    type = ObjectType.BLOB


class ImageDataModel(BlobDataModel):
    """Base class for all image objects in ApertureDB.
    """
    type = ObjectType.IMAGE


class ClipDataModel(IdentityDataModel):
    """Base class for all clip objects in ApertureDB.
    """
    type = ObjectType.CLIP
    range_type: Annotated[RangeType,
                          Field(title="Range Type", description="Range type",
                                default=RangeType.TIME),
                          PropertyType.SYSTEM]
    start: Annotated[float, Field(title="Start", description="Start point as frame, time(hh:mm:ss.uuuuuu) or fraction"),
                     PropertyType.SYSTEM]
    stop: Annotated[float, Field(title="Stop", description="Stop point as frame, time(hh:mm:ss.uuuuuu) or fraction"),
                    PropertyType.SYSTEM]


class VideoDataModel(BlobDataModel):
    """Data model for video objects in ApertureDB.
    """
    type = ObjectType.VIDEO


class DescriptorDataModel(IdentityDataModel):
    """Descriptor (Embedding) data model for ApertureDB.
    """
    type = ObjectType.DESCRIPTOR
    vector: Annotated[List[float], Field(
        title="Vector", description="Vector of floats"), PropertyType.SYSTEM]
    set: Annotated[DescriptorSetDataModel, Field(
        title="Set", description="Descriptor set"), PropertyType.SYSTEM]


class PolygonDataModel(IdentityDataModel):
    """Polygon data model for ApertureDB.
    """
    type = ObjectType.POLYGON


class FrameDataModel(IdentityDataModel):
    """Frame data model for ApertureDB.
    """
    type = ObjectType.FRAME


class DescriptorSetDataModel(IdentityDataModel):
    """Descriptor Set data model for ApertureDB.
    """
    type = ObjectType.DESCRIPTORSET
    name: Annotated[str, Field(title="Name", description="Name of the descriptor set"),
                    PropertyType.SYSTEM]
    dimensions: Annotated[int, Field(title="Dimension", description="Dimension of the descriptor set"),
                          PropertyType.SYSTEM]


class BoundingBoxDataModel(IdentityDataModel):
    """Bounding Box data model for ApertureDB.
    """
    type = ObjectType.BOUNDING_BOX
