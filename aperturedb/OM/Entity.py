from typing import ByteString, List, Optional, Dict
from aperturedb import Connector


class Entity:
    """**This is the base class for the various ApertureDB Objects.**

    ApertureDB objects:
        * Entity
        * Image
        * Video
        * Blob
        * DescriptorSet
        * Descriptor
        * BoundingBox
    """
    def __init__(self, 
        db: Connector, 
        properties: Optional[Dict] = None,
        operations: List[Dict] = None,
        blob: ByteString = None,
        entity_class: str = None,
        object_type: str = "Entity",
        label: str = None,
        rectangle: Dict = None) -> None:
        self._db = db
        self._properties = properties
        self._operations = operations
        self._blob = blob
        self._object_type = object_type
        self._entity_class = entity_class
        self._label = label
        self._rectangle = rectangle

        for prop in properties:
            setattr(self, prop, properties[prop])
    
    def _add_command(self):
        return f"Add{self._object_type}"

    def save(self):
        """**Persists the current python representation of of the DB object**

        This might result in an Update/Add command, depending on wether the object is being
        newly created or modified.
        """
        query = [{
            self._add_command() : {
            }
        }]
        if self._entity_class is not None:
            query[0][self._add_command()]["class"] = self._entity_class

        if self._properties is not None:
            query[0][self._add_command()]["properties"] = self._properties

        if self._label is not None:
            query[0][self._add_command()]["label"] = self._label
        
        if self._rectangle is not None:
            query[0][self._add_command()]["rectangle"] = self._rectangle

        if self._blob is not None:
            resp, _ = self._db.query(query, [self._blob])
        else:
            resp, _ = self._db.query(query)

        for property, value in resp[0][self._add_command()].items():
            setattr(self, property, value)
        
        return self