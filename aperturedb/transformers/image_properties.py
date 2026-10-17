from aperturedb.Constants import BLOB_ADD_COMMANDS
from aperturedb.transformers.transformer import Transformer
from aperturedb.Subscriptable import Subscriptable

from PIL import Image
import io
import logging
import uuid
import hashlib

logger = logging.getLogger(__name__)


class ImageProperties(Transformer):
    """
    This computes some image properties and adds them to the metadata.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data, **kwargs)

        try:
            utils = self.get_utils()
            if "adb_data_source" not in utils.get_indexed_props("_Image"):
                utils.create_entity_index("_Image", "adb_data_source")
        except Exception:
            logger.exception(
                "Error checking or creating index for _Image properties", stack_info=True)

    def getitem(self, subscript):
        x = self.data[subscript]
        blob_index = 0
        for cmd_dict in x[0]:
            cmd_name = None
            if isinstance(cmd_dict, dict) and len(cmd_dict) > 0:
                cmd_name = next(iter(cmd_dict.keys()))

            if cmd_name in BLOB_ADD_COMMANDS:
                if blob_index >= len(x[1]):
                    logger.warning(
                        "Missing blob for command %s (expected at index %d), stopping property processing for this transaction.",
                        cmd_name, blob_index)
                    break

            try:
                if cmd_name == "AddImage":
                    src_properties = cmd_dict["AddImage"].setdefault(
                        "properties", {})
                    # Compute the dynamic properties and apply them to metadata
                    blob = x[1][blob_index]
                    src_properties["adb_image_size"] = len(blob)
                    src_properties["adb_image_id"] = str(
                        src_properties["id"] if src_properties.get("id") not in (None, "")
                        else uuid.uuid4().hex
                    )
                    src_properties["adb_image_sha256"] = hashlib.sha256(
                        blob).hexdigest()

                    # Compute the image dimensions.
                    with Image.open(io.BytesIO(blob)) as pil_image:
                        src_properties["adb_image_width"] = pil_image.width
                        src_properties["adb_image_height"] = pil_image.height

            except Exception:
                # Importantly, do not raise an exception here, since it will kill ingestion.
                # Create a log message instead, for post-mortem analysis.
                logger.exception(
                    "Error applying image properties", stack_info=True)

            if cmd_name in BLOB_ADD_COMMANDS:
                blob_index += 1

        return x
