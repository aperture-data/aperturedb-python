from __future__ import annotations
from pydantic import BaseModel, Field
from typing_extensions import Annotated
from uuid import uuid4
from aperturedb.Query import ObjectType


class IdentityModel(BaseModel):
    id: Annotated[str, Field(default_factory=lambda: uuid4().hex)]
    type: Annotated[ObjectType, Field(
        description="Object type")] = ObjectType.ENTITY


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
