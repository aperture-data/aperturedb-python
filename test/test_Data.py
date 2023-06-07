import numpy as np
from aperturedb.Query import QueryBuilder

import logging
logger = logging.getLogger(__name__)


class TestEntityLoader():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def assertTrue(self, condition):
        if not condition:
            raise AssertionError("Condition not true")

    def test_Loader(self, utils, insert_data_from_csv):
        classes = ["_Image", "_Descriptor", "Person", "_BoundingBox"]
        self.assertTrue(utils.remove_all_objects())

        results = [utils.create_entity_index(
            e, "id") for e in classes]
        self.assertTrue(all(results))

        # Insert Person Nodes
        data, _ = insert_data_from_csv("./input/persons.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))

        # Insert Images
        data, _ = insert_data_from_csv(in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

        # Insert Connections
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/connections-persons-images.adb.csv")
        self.assertEqual(
            len(data), utils.count_connections("has_image"))

        # Insert BBoxes
        data, _ = insert_data_from_csv(in_csv_file="./input/bboxes.adb.csv")
        self.assertEqual(len(data), utils.count_bboxes())

    def test_LoaderDescriptorsImages(self, utils, insert_data_from_csv):
        self.assertTrue(utils.remove_all_objects())

        # Insert Images, images may be there already, and that case should
        # be handled correctly by contraints
        data, _ = insert_data_from_csv(in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

        # Insert DescriptorsSet
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/descriptorset.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_DescriptorSet"))

        # Insert Descriptors

        sets = ["setA", "setB"]

        total_descriptors = 0
        for setname in sets:
            data, _ = insert_data_from_csv(
                in_csv_file = "./input/" + setname + ".adb.csv")

            total_descriptors += len(data)
            self.assertEqual(total_descriptors,
                             utils.count_entities("_Descriptor"))

    def test_BlobLoader(self, db, utils, insert_data_from_csv):
        # Assert that we have a clean slate to begin with.
        self.assertTrue(utils.remove_all_objects())

        data, _ = insert_data_from_csv(in_csv_file = "./input/blobs.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Blob"))

        query = [{
            "FindBlob": {
                "blobs": True,
                "results": {
                    "list": ["license"],
                    "limit": 1
                }
            }
        }]

        res, blob = db.query(query)
        self.assertEqual(len(blob), 1)

        arr = np.frombuffer(blob[0])
        self.assertEqual(arr[2], 3.3)

    def test_conditional_add(self, utils, insert_data_from_csv):
        logger.debug(f"Cleaning existing data")
        self.assertTrue(utils.remove_all_objects())
        # insert one of the images from image csv, for bboxes to refer to.
        images, _ = insert_data_from_csv(
            in_csv_file="./input/images.adb.csv", rec_count=1)
        self.assertEqual(len(images), 1)

        # Insert BBoxes with repeated entries.
        # There is just one unique entry in the input csv.
        logger.debug(f"Inserting bounding box data")
        boxes, _ = insert_data_from_csv(
            in_csv_file="./input/bboxes-constraints.adb.csv")
        self.assertEqual(3, len(boxes))
        self.assertEqual(1, utils.count_bboxes())

class TestEntityDelete():
    def test_delete(self, utils, insert_data_from_csv, modify_data_from_csv):
        # Assert that we have a clean slate to begin with.
        self.assertTrue(utils.remove_all_objects())

        # Verify entire file is loaded
        data = insert_data_from_csv(in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Image"))

        deleted = insert_data_from_csv(in_csv_file = "./input/images_delete.adb.csv")
        self.assertEqual(len(data)-len(deleted), utils.count_entities("_Image"))

class TestEntityUpdate():
    def test_partial_update(self, utils, insert_data_from_csv, xx ):
        # Assert that we have a clean slate to begin with.
        self.assertTrue(utils.remove_all_objects())

        # Verify entire file is loaded
        data = insert_data_from_csv(in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Image"))
        update = insert_data_from_csv(in_csv_file = "./input/images_partial_update.adb.csv")
        self.assertEqual(len(data)-len(deleted), utils.count_entities("_Image"))

class TestEntityAddOrUpdate():
    def test_just_add(self,utils,modify_data_from_csv ):
        data = modify_data_from_csv(in_csv_file = "./input/entity_update_just_add.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))

class TestImageAddOrUpdate():
    def test_images(self,utils,modify_data_from_csv ):
        data = modify_data_from_csv(in_csv_file= "./input/images_update_and_add.adb.csv")
        self.assertEqual(True,True) # how to verify.

class TestImageAlwaysNewest():
    def test_images(self,utils,modify_data_from_csv ):
        data = modify_data_from_csv(in_csv_file= "./input/images_newest_blobs.adb.csv")
        self.assertEqual(True,True) # how to verify.

class TestURILoader():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def test_S3Loader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/s3_images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

    def test_HttpImageLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/http_images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

    def test_GSImageLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/gs_images.adb.csv")
        self.assertEqual(len(data), utils.count_images())
