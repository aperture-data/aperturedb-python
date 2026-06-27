def __getattr__(name):
    if name == "Transformer":
        from .transformer import Transformer
        return Transformer
    elif name == "CommonProperties":
        from .common_properties import CommonProperties
        return CommonProperties
    elif name == "ImageProperties":
        from .image_properties import ImageProperties
        return ImageProperties
    elif name == "VideoProperties":
        from .video_properties import VideoProperties
        return VideoProperties
    elif name == "BoundingBoxProperties":
        from .bounding_box_properties import BoundingBoxProperties
        return BoundingBoxProperties
    elif name == "FacenetPyTorchEmbeddings":
        from .facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
        return FacenetPyTorchEmbeddings
    elif name == "CLIPPyTorchEmbeddings":
        from .clip_pytorch_embeddings import CLIPPyTorchEmbeddings
        return CLIPPyTorchEmbeddings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Transformer",
    "CommonProperties",
    "ImageProperties",
    "VideoProperties",
    "BoundingBoxProperties",
    "FacenetPyTorchEmbeddings",
    "CLIPPyTorchEmbeddings",
]
