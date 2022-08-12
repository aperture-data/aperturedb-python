import numpy as np

from .test_Base import TestBase
import dbinfo

from aperturedb import DescriptorDataCSV, Utils
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV
from aperturedb.DescriptorDataCSV import DescriptorDataCSV

from aperturedb.BBoxDataCSV import BBoxDataCSV

import logging
import pytest
logger = logging.getLogger(__name__)


def insert_data_from_csv(self: TestBase, in_csv_file, rec_count=-1):
    file_data_pair = {
        "./input/persons.adb.csv": EntityDataCSV,
        "./input/images.adb.csv": ImageDataCSV,
        "./input/connections-persons-images.adb.csv": ConnectionDataCSV,
        "./input/bboxes.adb.csv": BBoxDataCSV,
        "./input/blobs.adb.csv": BlobDataCSV,
        "./input/descriptorset.adb.csv": DescriptorSetDataCSV,
        "./input/setA.adb.csv": DescriptorDataCSV,
        "./input/setB.adb.csv": DescriptorDataCSV,
        "./input/s3_images.adb.csv": ImageDataCSV,
        "./input/http_images.adb.csv": ImageDataCSV,
        './input/bboxes-constraints.adb.csv': BBoxDataCSV
    }

    data = file_data_pair[in_csv_file](in_csv_file)
    if rec_count != -1:
        data = data[:rec_count]

    if self.stats:
        print("\n")

    loader = ParallelLoader(self.db)
    loader.ingest(data, batchsize=self.batchsize,
                  numthreads=self.numthreads,
                  stats=self.stats)
    assert loader.error_counter == 0
    return data


class TestEntityLoader(TestBase):

    def test_Loader(self):

        self.db = self.create_connection()
        dbutils = Utils.Utils(self.db)
        classes = ["_Image", "_Descriptor", "Person", "_BoundingBox"]
        for c in classes:
            # Assert that we have a clean slate to begin with.
            self.assertEqual(dbutils.remove_entities(c), True)
            self.assertEqual(dbutils.count_entities(c), 0)

        results = [dbutils.create_entity_index(
            e, "id", "integer") for e in classes]
        self.assertTrue(all(results))

        # Insert Person Nodes
        data = insert_data_from_csv(self, "./input/persons.adb.csv")
        self.assertEqual(len(data), dbutils.count_entities("Person"))

        # Insert Images
        data = insert_data_from_csv(
            self, in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), dbutils.count_images())

        # Insert Connections
        data = insert_data_from_csv(
            self, in_csv_file = "./input/connections-persons-images.adb.csv")
        self.assertEqual(
            len(data), dbutils.count_connections("has_image"))

        # Insert BBoxes
        data = insert_data_from_csv(self, in_csv_file="./input/bboxes.adb.csv")
        self.assertEqual(len(data), dbutils.count_bboxes())

    def test_LoaderDescriptorsImages(self):

        self.db = self.create_connection()
        dbutils = Utils.Utils(self.db)
        # Insert Images, images may be there already, and that case should
        # be handled correctly by contraints
        data = insert_data_from_csv(
            self, in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), dbutils.count_images())

        # Insert DescriptorsSet
        data = insert_data_from_csv(
            self, in_csv_file = "./input/descriptorset.adb.csv")
        self.assertEqual(
            len(data), dbutils.count_entities("_DescriptorSet"))

        # Insert Descriptors

        sets = ["setA", "setB"]

        total_descriptors = 0
        for setname in sets:
            data = insert_data_from_csv(self,
                                        in_csv_file = "./input/" + setname + ".adb.csv")

            total_descriptors += len(data)
            self.assertEqual(total_descriptors,
                             dbutils.count_entities("_Descriptor"))

    def test_BlobLoader(self):
        self.db = self.create_connection()
        dbutils = Utils.Utils(self.db)

        # Assert that we have a clean slate to begin with.
        self.assertEqual(dbutils.remove_entities("_Blob"), True)
        self.assertEqual(dbutils.count_entities("_Blob"), 0)

        data = insert_data_from_csv(
            self, in_csv_file = "./input/blobs.adb.csv")
        self.assertEqual(len(data), dbutils.count_entities("_Blob"))

        query = [{
            "FindBlob": {
                "blobs": True,
                "results": {
                    "list": ["license"],
                    "limit": 1
                }
            }
        }]

        res, blob = self.db.query(query)
        self.assertEqual(len(blob), 1)

        arr = np.frombuffer(blob[0])
        self.assertEqual(arr[2], 3.3)

    def test_conditional_add(self):
        self.db = self.create_connection()
        dbutils = Utils.Utils(self.db)
        logger.debug(f"Cleaning existing data")

        self.assertTrue(dbutils.remove_entities("_BoundingBox"))
        self.assertTrue(dbutils.remove_entities("_Image"))
        # insert one of the images from image csv, for bboxes to refer to.
        images = insert_data_from_csv(
            self, in_csv_file="./input/images.adb.csv", rec_count=1)
        self.assertEqual(len(images), 1)

        # Insert BBoxes with repeated entries.
        # There is just one unique entry in the input csv.
        logger.debug(f"Inserting bounding box data")
        boxes = insert_data_from_csv(
            self, in_csv_file="./input/bboxes-constraints.adb.csv")
        self.assertEqual(3, len(boxes))
        self.assertEqual(1, dbutils.count_bboxes())


class TestURILoader(TestBase):

    def test_S3Loader(self):

        # Insert Images
        self.db = self.create_connection()

        dbutils = Utils.Utils(self.db)

        count_before = dbutils.count_images()
        data = insert_data_from_csv(
            self, in_csv_file = "./input/s3_images.adb.csv")
        self.assertEqual(len(data), dbutils.count_images() - count_before)

    def test_HttpImageLoader(self):
        self.db = self.create_connection()

        dbutils = Utils.Utils(self.db)

        count_before = dbutils.count_images()

        data = insert_data_from_csv(
            self, in_csv_file = "./input/http_images.adb.csv")
        self.assertEqual(len(data), dbutils.count_images() - count_before)
