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
        return BoundingBox(
            db=self._db,
            properties=properties,
            rectangle=rectangle,
            label=label,
            object_type="BoundingBox"
        )
