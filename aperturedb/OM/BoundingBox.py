from typing import ByteString, Dict, List, Optional
from aperturedb import Connector
from aperturedb.OM.Entity import Entity


class BoundingBox(Entity):
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