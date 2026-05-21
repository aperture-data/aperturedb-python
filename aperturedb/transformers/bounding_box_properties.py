from aperturedb.transformers.transformer import Transformer
from aperturedb.Subscriptable import Subscriptable
import logging

logger = logging.getLogger(__name__)


class BoundingBoxProperties(Transformer):
    """
    This computes bounding box and polygon properties and adds them to the metadata.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data, **kwargs)
        self.annotation_source = kwargs.get("annotation_source", "coco")
        self.annotation_mode = kwargs.get("annotation_mode", "auto")

    def getitem(self, subscript):
        x = self.data[subscript]
        try:
            for ic in getattr(self, "_add_bounding_box_index", []):
                src_properties = x[0][ic]["AddBoundingBox"].setdefault(
                    "properties", {})
                if self.annotation_source:
                    src_properties["annotation_source"] = self.annotation_source
                if self.annotation_mode:
                    src_properties["annotation_mode"] = self.annotation_mode

            for ic in getattr(self, "_add_polygon_index", []):
                src_properties = x[0][ic]["AddPolygon"].setdefault(
                    "properties", {})
                if self.annotation_source:
                    src_properties["annotation_source"] = self.annotation_source
                if self.annotation_mode:
                    src_properties["annotation_mode"] = self.annotation_mode
        except Exception as e:
            logger.exception(e.with_traceback(None), stack_info=True)

        return x
