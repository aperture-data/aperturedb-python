from aperturedb.transformers.transformer import Transformer
from aperturedb.Subscriptable import Subscriptable
import logging

logger = logging.getLogger(__name__)


class BoundingBoxProperties(Transformer):
    """
    This transformer applies static annotation metadata (like annotation_source
    and annotation_mode) to bounding boxes and polygons.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data, **kwargs)
        self.annotation_source = kwargs.get("annotation_source", None)
        self.annotation_mode = kwargs.get("annotation_mode", None)

    def getitem(self, subscript):
        if not (self.annotation_source or self.annotation_mode):
            return self.data[subscript]

        x = self.data[subscript]
        # Iterate over current transaction commands to handle variable annotation counts
        for cmd_dict in x[0]:
            try:
                cmd_name = list(cmd_dict.keys())[0]
                if cmd_name in ["AddBoundingBox", "AddPolygon"]:
                    src_properties = cmd_dict[cmd_name].setdefault(
                        "properties", {})
                    if self.annotation_source:
                        src_properties["annotation_source"] = self.annotation_source
                    if self.annotation_mode:
                        src_properties["annotation_mode"] = self.annotation_mode
            except Exception:
                logger.exception(
                    "Error applying bounding box properties", stack_info=True)

        return x
