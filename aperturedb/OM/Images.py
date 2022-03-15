import io
from aperturedb import Connector
from aperturedb.OM.Repository import Repository
from aperturedb.OM.Image import Image
from torchvision.datasets.vision import VisionDataset
from torch.utils.data import DataLoader

class Images(Repository):
    """**Collection of Images in aperturedb**
    """
    def __init__(self, db: Connector) -> None:
        super().__init__(db)
        self._object_type = "Image"
        self._entity_types[self._object_type] = Image

    def display(self, show_bboxes: bool = False):
        """**Display a set of image**

        Could be invoked to get visual representation of a images
        Would only work in notebook environment.

        Args:
            show_bboxes (bool, optional): _description_. Defaults to False.
        """
        pass

    def dataset_import(self, dataset: VisionDataset, root: str = None) -> int:
        def image_to_byte_array(image:Image):
            imgByteArr = io.BytesIO()
            image.save(imgByteArr, format='JPEG')
            imgByteArr = imgByteArr.getvalue()
            return imgByteArr

        
        for item in dataset:
            adImage = self.create_new(
                blob = image_to_byte_array(item[0]),
                properties= {
                    "class": item[1]
                }
            )
            adImage.save()

        