from typing import ByteString, Dict, List, Optional
from aperturedb import Connector
from aperturedb.OM.Entity import Entity

class Image(Entity):
    """**Image is rasterized form of something which can be visualized**

        Args:
            db (Connector): Underlying connector
            properties (Optional[Dict]): Arbitrary key value pairs.
            operations (List[Dict], optional): _description_. Defaults to None.
            blob (ByteString, optional): _description_. Defaults to None.
            object_type (str, optional): _description_. Defaults to None.
    """
    def __init__(self, 
        db: Connector, 
        properties: Optional[Dict], 
        operations: List[Dict] = None, 
        blob: ByteString = None, 
        object_type: str = None) -> None:
        super().__init__(
            db = db, 
            properties = properties, 
            operations = operations, 
            blob = blob,
            object_type = object_type)

    
