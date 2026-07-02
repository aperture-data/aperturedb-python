def __getattr__(name):
    if name == "Transformer":
        from .transformer import Transformer
        val = Transformer
    elif name == "CommonProperties":
        from .common_properties import CommonProperties
        val = CommonProperties
    elif name == "ImageProperties":
        from .image_properties import ImageProperties
        val = ImageProperties
    elif name == "VideoProperties":
        from .video_properties import VideoProperties
        val = VideoProperties
    elif name == "BoundingBoxProperties":
        from .bounding_box_properties import BoundingBoxProperties
        val = BoundingBoxProperties
    elif name == "FacenetPyTorchEmbeddings":
        from .facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
        val = FacenetPyTorchEmbeddings
    elif name == "CLIPPyTorchEmbeddings":
        from .clip_pytorch_embeddings import CLIPPyTorchEmbeddings
        val = CLIPPyTorchEmbeddings
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    globals()[name] = val
    return val


__all__ = [
    "Transformer",
    "CommonProperties",
    "ImageProperties",
    "VideoProperties",
    "BoundingBoxProperties",
]
