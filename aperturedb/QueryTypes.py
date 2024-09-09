from __future__ import annotations
from pydantic import BaseModel, Field
from typing_extensions import Annotated, List
from typing import ClassVar
from uuid import uuid4
from aperturedb.Query import ObjectType, PropertyType, RangeType


class IdentityModel(BaseModel):
    id: Annotated[str, Field(default_factory=lambda: uuid4().hex)]
    # Change as per the docs for the error
    # https://docs.pydantic.dev/dev-v2/usage/errors/#model-field-overridden
    type: ClassVar[ObjectType] = ObjectType.ENTITY


class BlobModel(IdentityModel):
    url: Annotated[str, Field(
        title="URL", description="URL to file, http, s3 or gs resource")]
    type = ObjectType.BLOB


class ImageModel(BlobModel):
    type = ObjectType.IMAGE


class ClipModel(IdentityModel):
    type = ObjectType.CLIP
    range_type: Annotated[RangeType,
                          Field(title="Range Type", description="Range type",
                                default=RangeType.TIME),
                          PropertyType.SYSTEM]
    start: Annotated[float, Field(title="Start", description="Start point as frame, time(hh:mm:ss.uuuuuu) or fraction"),
                     PropertyType.SYSTEM]
    stop: Annotated[float, Field(title="Stop", description="Stop point as frame, time(hh:mm:ss.uuuuuu) or fraction"),
                    PropertyType.SYSTEM]


class VideoModel(BlobModel):
    type = ObjectType.VIDEO


class DescriptorModel(IdentityModel):
    type = ObjectType.DESCRIPTOR
    vector: Annotated[List[float], Field(
        title="Vector", description="Vector of floats"), PropertyType.SYSTEM]
    set: Annotated[DescriptorSetModel, Field(
        title="Set", description="Descriptor set"), PropertyType.SYSTEM]


class PolygonModel(IdentityModel):
    type = ObjectType.POLYGON


class FrameModel(IdentityModel):
    type = ObjectType.FRAME


class DescriptorSetModel(IdentityModel):
    type = ObjectType.DESCRIPTORSET
    name: Annotated[str, Field(title="Name", description="Name of the descriptor set"),
                    PropertyType.SYSTEM]
    dimensions: Annotated[int, Field(title="Dimension", description="Dimension of the descriptor set"),
                          PropertyType.SYSTEM]


class BoundingBoxModel(IdentityModel):
    type = ObjectType.BOUNDING_BOX
