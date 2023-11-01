from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
from .clip import generate_embedding, descriptor_set
from aperturedb.Utils import create_connector, Utils


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
        super().__init__(data)
        self.search_set_name = kwargs.get(
            "search_set_name", descriptor_set)

        # Let's sample some data to figure out the descriptorset we need.
        if len(self._add_image_index) > 0:
            sample = generate_embedding(self.data[0][1][0])
            utils = Utils(create_connector())
            utils.add_descriptorset(self.search_set_name, dim=len(sample) // 4)

    def getitem(self, subscript):
        x = self.data[subscript]

        for ic in self._add_image_index:
            serialized = generate_embedding(x[1][ic])
            x[1].append(serialized)
            x[0].append(
                {
                    "AddDescriptor": {
                        "set": self.search_set_name,
                        "connect": {
                            "ref": x[0][ic]["AddImage"]["_ref"]
                        }
                    }
                })
        return x
