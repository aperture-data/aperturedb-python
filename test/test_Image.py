from datetime import datetime
import unittest
import  os
from aperturedb import Connector, Images, Repository, Status, BoundingBoxes
from utils import cleanDB

class TestImageMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.db = Connector.Connector(user="admin", password="admin")
        cls.status = Status.Status(cls.db)
        cls.images = Images.Images(cls.db)
        cleanDB(cls.db)

    def create_Bounding_Box(self):
        bboxes = BoundingBoxes.BoundingBoxes(self.db)
        bboxes.create_new(
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
        bbox = bboxes.save()
        return bbox

    def create_Image(self, path):
        image = None
        with open(path, "rb") as instream:
            img_data = instream.read()
            self.images.create_new(
                properties={
                    "description" : "A first class image",
                    "timestamp": str(datetime.now()),
                    "path": path
                }, 
                operations=[], 
                blob=img_data)
            image = self.images.save()
        return image            



    
    def test_create(self):
        for i in range(10):
            file_path = f"input/images/000{i}.jpg"
            abs_path  = os.path.join(os.path.join( os.path.dirname( __file__) , file_path))
            image = self.create_Image(abs_path)
            self.assertTrue(hasattr(image, "_uniqueid"))
            self.assertTrue(hasattr(image, "description"))
            self.assertTrue(hasattr(image, "timestamp"))

    def test_getOne(self):
        image = self.create_Image(os.path.join(os.path.join( os.path.dirname( __file__), "input/images/0000.jpg")))
        img = self.images.get(id=image._uniqueid)
        self.assertIsNotNone(img)
        self.assertEqual(img._uniqueid, image._uniqueid)

    def test_ConnectedObjects(self):
        entities = Repository.Repository(self.db)
        entities.create_new(
            properties={
                "name": "James Bond",
                "age": 7,
                "email": "james@aperturedata.io"
            },
            eclass="Person"
        )
        entity = entities.save()
        self.assertTrue(hasattr(entity, "_uniqueid"))

        re_read = entities.get(entity._uniqueid)
        self.assertIsNotNone(re_read)

    def test_CreateBbox(self):
        box = self.create_Bounding_Box()
        self.assertIsNotNone(box)
        self.assertIsNotNone(getattr(box,"_uniqueid"))


    def test_Connect(self):
        box = self.create_Bounding_Box()
        image = self.create_Image(os.path.join(os.path.join( os.path.dirname( __file__), "input/images/0000.jpg")))
        bboxes = BoundingBoxes.BoundingBoxes(self.db)
        bbox = bboxes.get(id=box._uniqueid)
        img = self.images.get(id=image._uniqueid)
        img.connect_BoundingBox(bbox)

    def test_enumerate(self):
        for image in self.images.filter():
            self.assertIsNotNone(image)
            self.assertTrue(hasattr(image, "_uniqueid"))

        

        
if __name__ == '__main__':
    unittest.main()