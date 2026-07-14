import hashlib
import logging
from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
from .clip import generate_embedding, descriptor_set

logger = logging.getLogger(__name__)


class CLIPPyTorchEmbeddings(Transformer):
    """
    Generates the embeddings for the images using the CLIP Pytorch model.
    https://github.com/openai/CLIP
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        """
        Args:
            data: Subscriptable object
            search_set_name: Name of the [descriptorset](/query_language/Reference/descriptor_commands/desc_commands/AddDescriptor) to use for the search.
        """
        self.search_set_name = kwargs.pop(
            "search_set_name", descriptor_set)
        super().__init__(data, **kwargs)
        self._descriptorset_initialized = False

    def getitem(self, subscript):
        x = self.data[subscript]

        blob_index = 0
        new_descriptors = []
        new_blobs = []

        for cmd_dict in x[0]:
            cmd_name = None
            if isinstance(cmd_dict, dict) and len(cmd_dict) > 0:
                cmd_name = next(iter(cmd_dict.keys()))

            if cmd_name == "AddImage":
                try:
                    blob = x[1][blob_index]
                except IndexError:
                    logger.warning(
                        f"Missing blob for AddImage at index {blob_index}")
                    blob_index += 1
                    continue

                serialized = None

                if not getattr(self, "_descriptorset_initialized", False) and isinstance(cmd_dict["AddImage"], dict) and "_ref" in cmd_dict["AddImage"]:
                    try:
                        serialized = generate_embedding(blob)
                        dim = len(serialized) // 4
                        utils = self.get_utils()
                        success = utils.add_descriptorset(
                            self.search_set_name, dim=dim, metric=["CS"])
                        if success or self.search_set_name in utils.get_descriptorset_list():
                            self._descriptorset_initialized = True
                    except Exception as e:
                        logger.warning(
                            f"Failed to initialize descriptorset: {e}", exc_info=True)

                # If the image already has an image_sha256, we use it.
                if getattr(self, "_descriptorset_initialized", False) and isinstance(cmd_dict["AddImage"], dict) and "_ref" in cmd_dict["AddImage"]:
                    try:
                        if serialized is None:
                            serialized = generate_embedding(blob)
                        image_sha256 = cmd_dict["AddImage"].get("properties", {}).get(
                            "adb_image_sha256", None)
                        if not image_sha256:
                            image_sha256 = hashlib.sha256(blob).hexdigest()
                        new_blobs.append(serialized)
                        desc_cmd = {
                            "AddDescriptor": {
                                "set": self.search_set_name,
                                "properties": {
                                    "image_sha256": image_sha256,
                                },
                                "if_not_found": {
                                    "image_sha256": ["==", image_sha256],
                                },
                                "connect": {
                                    "ref": cmd_dict["AddImage"]["_ref"]
                                }
                            }
                        }
                        new_descriptors.append(desc_cmd)
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate embedding or descriptor: {e}", exc_info=True)
            if cmd_name in ["AddImage", "AddDescriptor", "AddVideo", "AddBlob"]:
                blob_index += 1

        x[0].extend(new_descriptors)
        x[1].extend(new_blobs)
        return x
