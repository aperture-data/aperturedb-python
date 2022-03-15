from typing import Dict, List, Optional
from aperturedb.OM.Entity import Entity
from aperturedb import Connector


class Repository:
    """**Repository can be thought of as a store of homogenous entities**

    Notes:
        - This interface tries to match pytorch's Dataset interface.
        - Ideally, there should be a way to convert to/from pytorch dataset into Data in aperturedb.
        - This has options to extract the entities in the database in various ways, controled by filter ctriteria.
    
    Example::
    
        dataset = Repository.filter(constrints)
        rectangles, labels = pytorch.infer(dataset)
        dataset.add_BoundingBoxes(rectangles, labels)
    """

    def __init__(self, db: Connector) -> None:
        self._object_type = "Entity"
        self._db = db
        self._entity_types = {
            "Entity": Entity
        }

    def _find_command(self):
        return f"Find{self._object_type}"

    def get(self, id) -> Entity:
        """**Get an Entity from a repository**

        Get a unique Object from the database by it's uniqueid property.

        Args:
            id (_type_): The id to uniquely identify an entity.

        Returns:
            Entity: The Entity managed by this reposotory.

        Retrieve an Image::

            from aperturedb import Connector, Images
            db_connector = Connector.Connector(user="admin", password="admin")
            images = Images.Images(db_connector)
            image = image.get(id=92)

        """
        query = {
            self._find_command(): {
                "constraints": {
                    "_uniqueid": ["==", id]
                },
                "results": {
                    "all_properties": True,
                    "limit": 1
                }
            }    
        }
        resp, _ = self._db.query([query])
        entity = resp[0][self._find_command()]["entities"][0]
        obj = self._entity_types[self._object_type](self._db, entity, [], None, self._object_type)
        return obj

    def filter(self, 
        constraints = None, 
        operations = None, 
        format = None) -> List[Entity]:
        """**Restrict the number of entities based on ctriteria**

        A new search will throw away the results of any previous search
        Without any constraints the method acts as ``find_all``

        Args:
            constraints: The criteria for search, optional
            operations: Operations before returning the list, optional
            format: Encoding format

        Returns:
            List of Entities.

        Example::

            for image in self.images.filter():
                self.assertIsNotNone(image)
                self.assertTrue(hasattr(image, "_uniqueid"))
        """
        query = {
            self._find_command(): {
                "results": {
                    "all_properties": True
                },
                "blobs": False
            }
        }
        resp, _ = self._db.query([query])

        return map(lambda x: self._entity_types[self._object_type](
            db=self._db,
            properties=x), resp[0][self._find_command()]["entities"])

    def create_new(self, 
        properties=None, 
        operations=None, 
        blob=None, 
        entity_class=None) -> Entity:
        """**Create a new entity.**

        A Create new function discards all previous information.
        All of the arguments are optional.

        Args:
            properties (dict): A collection of arbitrary key value pairs associated with this object
            operations (List[dict]): A list of operations to be aplpied on the entity before persisting it.
            blob (Bytestring): An in-memory representation of data for the image.
            rectangle (Dict): A dcit representing a rectangle.
            entity_class (str): A class associated with generic repository.

        Example::

            entities = Repository.Repository(self.db)
            entity = entities.create_new(
                properties={
                    "name": "James Bond",
                    "age": 7,
                    "email": "james@aperturedata.io"
                },
                entity_class="Person"
            )
            entity.save()
        """
        #Create the entity, and run a db query with add directive.
        obj = self._entity_types[self._object_type](self._db, properties, operations, blob, self._object_type)
        return obj

    def find_similar(self, sample) -> List[Entity]:
        """**Get entities similar to the sample**
        
        This will return a set of entities that are similar to the input simple.

        Args:
            sample (_type_): The example input to be compared against.
        """
        pass

    def dataset_export(self, root: str) -> int:
        """**Saves the collection into the path specified**

        Args:
            root (str): path to save collection at
        
        Returns:
            int: Status to indicate the outcome of export.
        """
        pass

    def dataset_import(self, root: str ) -> int:
        """**Imports the collection that has been generated by another instance**

        Args:
            root (str): path where the exported dataset exists.

        Returns:
            int: Status to indicate the outcome of import
        """
        pass

