from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
from PIL import Image
import io
import time
from .facenet import generate_embedding
from aperturedb.Utils import create_connector, Utils


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
        super().__init__(data)
        self.search_set_name = kwargs.get(
            "search_set_name", "facenet_pytorch_embeddings")

        # Let's sample some data to figure out the descriptorset we need.
        if len(self._add_image_index) > 0:
            sample = self._get_embedding_from_blob(self.data[0][1][0])
            utils = Utils(create_connector())
            utils.add_descriptorset(self.search_set_name, dim=len(sample) // 4)

    def _get_embedding_from_blob(self, image_blob: bytes):
        pil_image = Image.open(io.BytesIO(image_blob))
        embedding = generate_embedding(pil_image)
        serialized = embedding.cpu().detach().numpy().tobytes()
        return serialized

    def getitem(self, subscript):
        start = time.time()
        self.ncalls += 1
        x = self.data[subscript]

        for ic in self._add_image_index:
            serialized = self._get_embedding_from_blob(
                x[1][self._add_image_index.index(ic)])
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
        self.cumulative_time += time.time() - start
        return x
