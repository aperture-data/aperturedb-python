from aperturedb.OM.Repository import Repository

class Images(Repository):
    """**Collection of Images in aperturedb**
    """
    def __init__(self) -> None:
        super().__init__()
        self._object_type = "Image"

    def display(self, show_bboxes: bool = False):
        """**Display a set of image**

        Could be invoked to get visual display of a images
        Would only work in notebook environment.

        Args:
            show_bboxes (bool, optional): _description_. Defaults to False.
        """
        pass