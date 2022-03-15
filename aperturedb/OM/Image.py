from typing import ByteString, Dict, List, Optional
from aperturedb import Connector
from aperturedb.OM.Entity import Entity

class Image(Entity):
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

    
