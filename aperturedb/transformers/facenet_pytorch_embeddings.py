from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
from PIL import Image
import io
from .facenet import generate_embedding
from aperturedb.Utils import create_connector, Utils


class FacenetPytorchEmbeddings(Transformer):
    """
    Generates the embeddings for the images using the Facenet Pytorch model.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data)
        self.search_set_name = kwargs.get(
            "search_set_name", "facenet_pytorch_embeddings")

        # Let's sample some data to figure out the descriptorset we need.
        sample = self.__get_embedding_from_blob(self.data[0][1][0])
        utils = Utils(create_connector())
        utils.add_descriptorset(self.search_set_name, dim=len(sample) // 4)

    def __get_embedding_from_blob(self, image_blob: bytes):
        pil_image = Image.open(io.BytesIO(image_blob))
        embedding = generate_embedding(pil_image)
        serialized = embedding.cpu().detach().numpy().tobytes()
        return serialized

    def getitem(self, subscript):
        x = self.data[subscript]

        for ic in self._add_image_index:
            serialized = self.__get_embedding_from_blob(x[1][ic])
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
