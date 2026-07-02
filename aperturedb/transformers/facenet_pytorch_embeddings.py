import hashlib
import logging
from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
from PIL import Image
import io
import time
from .facenet import generate_embedding

logger = logging.getLogger(__name__)


class FacenetPyTorchEmbeddings(Transformer):
    """
    Generates the embeddings for the images using the Facenet Pytorch model.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        """
        Args:
            data: Subscriptable object
            search_set_name: Name of the [descriptorset](/query_language/Reference/descriptor_commands/desc_commands/AddDescriptor) to use for the search.
        """
        self.search_set_name = kwargs.pop(
            "search_set_name", "facenet_pytorch_embeddings")
        super().__init__(data, **kwargs)
        self._descriptorset_initialized = False

    def _get_embedding_from_blob(self, image_blob: bytes):
        pil_image = Image.open(io.BytesIO(image_blob))
        embedding = generate_embedding(pil_image)
        serialized = embedding.cpu().detach().numpy().tobytes()
        return serialized

    def getitem(self, subscript):
        start = time.time()
        self.ncalls += 1
        x = self.data[subscript]

        blob_index = 0
        new_descriptors = []
        new_blobs = []

        for cmd_dict in x[0]:
            cmd_name = None
            if isinstance(cmd_dict, dict) and len(cmd_dict) > 0:
                cmd_name = next(iter(cmd_dict.keys()))

            if cmd_name == "AddImage":
                blob = x[1][blob_index]

                if not getattr(self, "_descriptorset_initialized", False):
                    utils = self.get_utils()
                    success = utils.add_descriptorset(
                        self.search_set_name, dim=512)
                    try:
                        if success or self.search_set_name in utils.get_descriptorset_list():
                            self._descriptorset_initialized = True
                    except Exception as e:
                        logger.warning(
                            f"Failed to check descriptorset list: {e}", exc_info=True)

                # If the image already has an image_sha256, we use it.
                if getattr(self, "_descriptorset_initialized", False):
                    serialized = self._get_embedding_from_blob(blob)
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
                            }
                        }
                    }
                    if "_ref" in cmd_dict["AddImage"]:
                        desc_cmd["AddDescriptor"]["connect"] = {
                            "ref": cmd_dict["AddImage"]["_ref"]
                        }
                    new_descriptors.append(desc_cmd)
            if cmd_name in ["AddImage", "AddDescriptor", "AddVideo", "AddBlob"]:
                blob_index += 1

        x[0].extend(new_descriptors)
        x[1].extend(new_blobs)
        self.cumulative_time += time.time() - start
        return x
