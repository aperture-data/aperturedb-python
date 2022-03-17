from datetime import datetime
import unittest
import os
from aperturedb import Connector, Status
from aperturedb.OM import Images, Repository, BoundingBoxes
from aperturedb import Images as nativeImages
import torchvision

#Cannot run this till cleanDB is merged.
@unittest.skip
class TestImageMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.db = Connector.Connector(user="admin", password="admin")
        cls.status = Status.Status(cls.db)
        cls.images = Images.Images(cls.db)

    def create_Bounding_Box(self):
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
        return bbox

    def create_Image(self, path):
        image = None
        with open(path, "rb") as instream:
            img_data = instream.read()
            return self.images.create_new(
                properties={
                    "description" : "A first class image",
                    "timestamp": str(datetime.now()),
                    "path": path
                }, 
                operations=[], 
                blob=img_data)
    
    def test_create(self):
        for i in range(10):
            file_path = f"input/images/000{i}.jpg"
            abs_path  = os.path.join(os.path.join( os.path.dirname( __file__) , file_path))
            image = self.create_Image(abs_path)
            self.assertTrue(hasattr(image, "description"))
            self.assertTrue(hasattr(image, "timestamp"))
            image.save()
            self.assertTrue(hasattr(image, "_uniqueid"))

    def test_getOne(self):
        image = self.create_Image(os.path.join(os.path.join( os.path.dirname( __file__), "input/images/0000.jpg")))
        image.save()
        img = self.images.get(id=image._uniqueid)
        self.assertIsNotNone(img)
        self.assertEqual(img._uniqueid, image._uniqueid)

    def test_Entity(self):
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
        self.assertTrue(hasattr(entity, "_uniqueid"))

        re_read = entities.get(entity._uniqueid)
        self.assertIsNotNone(re_read)

    def test_CreateBbox(self):
        box = self.create_Bounding_Box()
        self.assertIsNotNone(box)
        self.assertIsNotNone(getattr(box,"_uniqueid"))
    
    def test_enumerate(self):
        for image in self.images.filter():
            self.assertIsNotNone(image)
            self.assertTrue(hasattr(image, "_uniqueid"))

    def test_import(self):
        #Import a pytorch dataset into aperturedb
        mnist = torchvision.datasets.MNIST(root='.', download=True)
        self.images.dataset_import(dataset=mnist)

    def test_update(self):
        constraints = nativeImages.Constraints()
        constraints.equal('class', 3)
        images = self.images.filter(constraints=constraints)
        bounding_boxes = BoundingBoxes.BoundingBoxes(self.db).filter()
        images.associate_entities(bounding_boxes)

   

if __name__ == '__main__':
    unittest.main()