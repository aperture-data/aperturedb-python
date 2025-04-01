import hashlib
from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
from .clip import generate_embedding, descriptor_set


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

        # Let's sample some data to figure out the descriptorset we need.
        if len(self._add_image_index) > 0:
            sample = generate_embedding(self.data[0][1][0])
            utils = self.get_utils()
            utils.add_descriptorset(
                self.search_set_name, dim=len(sample) // 4, metric=["CS"])

    def getitem(self, subscript):
        x = self.data[subscript]

        for ic in self._add_image_index:
            serialized = generate_embedding(x[1][ic])
            # If the image already has an image_sha256, we use it.
            image_sha256 = x[0][ic]["AddImage"].get("properties", {}).get(
                "adb_image_sha256", None)
            if not image_sha256:
                image_sha256 = hashlib.sha256(x[1][ic]).hexdigest()
            x[1].append(serialized)
            x[0].append(
                {
                    "AddDescriptor": {
                        "set": self.search_set_name,
                        "properties": {
                            "image_sha256": image_sha256,
                        },
                        "if_not_found": {
                            "image_sha256": ["==", image_sha256],
                        },
                        "connect": {
                            "ref": x[0][ic]["AddImage"]["_ref"]
                        }
                    }
                })
        return x
