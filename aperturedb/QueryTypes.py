from __future__ import annotations
from pydantic import BaseModel, Field
from typing_extensions import Annotated
from typing import ClassVar
from uuid import uuid4
from aperturedb.Query import ObjectType


class IdentityModel(BaseModel):
    id: Annotated[str, Field(default_factory=lambda: uuid4().hex)]
    # Change as per the docs for the error
    # https://docs.pydantic.dev/dev-v2/usage/errors/#model-field-overridden
    type: ClassVar[ObjectType] = ObjectType.ENTITY


class BlobModel(IdentityModel):
    file: Annotated[str, Field(title="Filepath", description="Path to file")]
    type = ObjectType.BLOB


class ImageModel(BlobModel):
    type = ObjectType.IMAGE


class VideoModel(BlobModel):
    type = ObjectType.VIDEO


class DesctiptorModel(BlobModel):
    type = ObjectType.DESCRIPTOR


class PolygonModel(IdentityModel):
    type = ObjectType.POLYGON


class FrameModel(IdentityModel):
    type = ObjectType.FRAME


class DescriptorSetModel(IdentityModel):
    type = ObjectType.DESCRIPTORSET


class BoundingBoxModel(IdentityModel):
    type = ObjectType.BOUNDING_BOX
