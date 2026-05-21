from aperturedb.transformers.transformer import Transformer
from aperturedb.Subscriptable import Subscriptable

import logging
import uuid
import hashlib

logger = logging.getLogger(__name__)


class VideoProperties(Transformer):
    """
    This computes some video properties and adds them to the metadata.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data, **kwargs)
        utils = self.get_utils()

        if "adb_data_source" not in utils.get_indexed_props("_Video"):
            utils.create_entity_index("_Video", "adb_data_source")

        self._blob_index_map = {ic: i for i, ic in enumerate(self._blob_index)}

    def getitem(self, subscript):
        x = self.data[subscript]
        try:
            blob_index = 0
            for cmd_dict in x[0]:
                cmd_name = list(cmd_dict.keys())[0]
                if cmd_name == "AddVideo":
                    src_properties = cmd_dict["AddVideo"].setdefault(
                        "properties", {})
                    # Compute the dynamic properties and apply them to metadata
                    src_properties["adb_video_size"] = len(x[1][blob_index])
                    src_properties["adb_video_sha256"] = hashlib.sha256(
                        x[1][blob_index]).hexdigest()

                    src_properties["adb_video_id"] = str(
                        src_properties["id"] if "id" in src_properties else uuid.uuid4().hex)

                if cmd_name in ["AddImage", "AddDescriptor", "AddVideo", "AddBlob"]:
                    blob_index += 1

        except Exception as e:
            # Importantly, do not raise an exception here, since it will kill ingestion.
            # Create a log message instead, for post-mortem analysis.
            logger.exception(e.with_traceback(None), stack_info=True)

        return x
