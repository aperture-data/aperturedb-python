from typing import Dict, Optional
from aperturedb import Connector
from aperturedb.OM.Repository import Repository
from aperturedb.OM.BoundingBox import BoundingBox

class BoundingBoxes(Repository):
    """**Collection of BoundingBoxes in aperturedb**
    """
    def __init__(self, db: Connector) -> None:
        super().__init__(db)
        self._object_type = "BoundingBox"
        self._entity_types[self._object_type] = BoundingBox

    def create_new(self,
        label: str,
        properties: Optional[Dict],
        rectangle: Dict) -> BoundingBox:
        """**Create a new Bounding box**

        Args:
            label (str): Label associated with the bounding box.
            properties (Optional[Dict]): Arbitrary key value pairs.
            rectangle (Dict): Dictionary comprising of x, y, width and height attributes.

        Returns:
            BoundingBox: Python object representing a Bounding box.

        Example::

            bboxes = BoundingBoxes.BoundingBoxes(self.db)
            bbox = bboxes.create_new(
                label="Plane",
                properties={
                    "label_id": 43
                },
                rectangle={
                    "x": 0,
                    "y": 0,
                    "width": 100,
                    "height": 200
                }
            )
            bbox.save()
        """
        return BoundingBox(
            db=self._db,
            properties=properties,
            rectangle=rectangle,
            label=label,
            object_type="BoundingBox"
        )
