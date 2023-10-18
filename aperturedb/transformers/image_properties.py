from aperturedb.transformers.transformer import Transformer
from aperturedb.Subscriptable import Subscriptable
from PIL import Image
import traceback
import io


class ImageProperties(Transformer):
    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data)

    def getitem(self, subscript):
        x = self.data[subscript]
        try:
            # x is a transaction that has an add_image command and a blob
            for ic in self._add_image_index:
                # Compute the dynamic properties and apply them to metadata
                x[0][ic]["AddImage"]["properties"]["adb_image_size"] = len(
                    x[1][ic])

                # Compute the image dimensions.
                pil_image = Image.open(io.BytesIO(x[1][ic]))
                x[0][ic]["AddImage"]["properties"]["adb_image_width"] = pil_image.width
                x[0][ic]["AddImage"]["properties"]["adb_image_height"] = pil_image.height

        except Exception as e:
            traceback.print_exc(limit=5)

        return x
