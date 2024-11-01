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
        utils = self.get_utils()

        if "adb_data_source" not in utils.get_indexed_props("_Image"):
            utils.create_entity_index("_Image", "adb_data_source")

    def getitem(self, subscript):
        x = self.data[subscript]
        try:
            # x is a transaction that has an add_image command and a blob
            for ic in self._add_image_index:
                blob_index = self._add_image_index.index(ic)
                src_properties = x[0][ic]["AddImage"]["properties"]
                # Compute the dynamic properties and apply them to metadata
                src_properties["adb_image_size"] = len(x[1][blob_index])
                src_properties["adb_image_sha256"] = hashlib.sha256(
                    x[1][blob_index]).hexdigest()

                # Compute the image dimensions.
                pil_image = Image.open(io.BytesIO(x[1][blob_index]))
                src_properties["adb_image_width"] = pil_image.width
                src_properties["adb_image_height"] = pil_image.height
                src_properties["adb_image_id"] = str(
                    src_properties["id"] if "id" in src_properties else uuid.uuid4().hex)

        except Exception as e:
            # Importantly, do not raise an exception here, since it will kill ingestion.
            # Create a log message instead, for post-mortem analysis.
            logger.exception(e.with_traceback(None), stack_info=True)

        return x
