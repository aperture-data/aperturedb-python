from typing import ByteString, Dict, List, Optional
from aperturedb import Connector
from aperturedb.OM.Entity import Entity


class BoundingBox(Entity):
    """**Bounding box is a highlighted area of an image**
    
    Bounding box is a visual annotation to specific area of an Image.
    It contains a rectangle and a label.

    
    """
    def __init__(self, 
        db: Connector, 
        properties: Optional[Dict], 
        object_type: str,
        label,
        rectangle) -> None:
        super().__init__(
            db=db, 
            properties=properties, 
            object_type=object_type, 
            label=label,
            rectangle=rectangle)