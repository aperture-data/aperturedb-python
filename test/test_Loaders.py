import argparse
import time
import unittest

import dbinfo
import numpy  as np

from aperturedb import Connector, Status
from aperturedb import EntityLoader, ConnectionLoader
from aperturedb import ImageLoader, BBoxLoader, BlobLoader
from aperturedb import DescriptorSetLoader, DescriptorLoader

class TestLoader(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ApertureDB Server Info
        self.db_host  = dbinfo.DB_HOST
        self.db_port  = dbinfo.DB_PORT
        self.user     = dbinfo.DB_USER
        self.password = dbinfo.DB_PASSWORD

        # Config params
        self.batchsize  = 99
        self.numthreads = 31
        self.stats      = False

        db_up = False
        attempts = 0
        while(not db_up):
            try:
                db = Connector.Connector(self.db_host, self.db_port, self.user, self.password)
                db_up = True
                if (attempts > 0):
                    print("Connection to ApertureDB successful.")
            except:
                print("Attempt", attempts,
                      "to connect to ApertureDB failed, retrying...")
                attempts += 1
                time.sleep(1) # sleeps 1 second

            if attempts > 10:
                print("Failed to connect to ApertureDB after 10 attempts")
                exit()

    def create_connection(self):
        return Connector.Connector(self.db_host, self.db_port, self.user, self.password)

class TestEntityLoader(TestLoader):

    def test_Loader(self):

        # Insert Person Nodes
        db = self.create_connection()

        in_csv_file = "./input/persons.adb.csv"

        generator = EntityLoader.EntityGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = EntityLoader.EntityLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_entities("Person"))


        # Insert Images
        in_csv_file = "./input/images.adb.csv"

        generator = ImageLoader.ImageGeneratorCSV(in_csv_file, check_image=False)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_images())


        # Insert Connections

        in_csv_file = "./input/connections-persons-images.adb.csv"

        generator = ConnectionLoader.ConnectionGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = ConnectionLoader.ConnectionLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_connections("has_image"))

        # Insert BBoxes

        in_csv_file = "./input/bboxes.adb.csv"

        generator = BBoxLoader.BBoxGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = BBoxLoader.BBoxLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_bboxes())

    def test_LoaderDescriptorsImages(self):

        # Insert Images, images may be there already, and that case should
        # be handled correctly by contraints
        db = self.create_connection()

        in_csv_file = "./input/images.adb.csv"

        generator = ImageLoader.ImageGeneratorCSV(in_csv_file, check_image=False)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_images())


        # Insert DescriptorsSet
        in_csv_file = "./input/descriptorset.adb.csv"

        generator = DescriptorSetLoader.DescriptorSetGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = DescriptorSetLoader.DescriptorSetLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_entities("_DescriptorSet"))

        # Insert Descriptors

        sets = ["setA", "setB"]

        total_descriptors = 0
        for setname in sets:

            in_csv_file = "./input/" + setname + ".adb.csv"

            generator = DescriptorLoader.DescriptorGeneratorCSV(in_csv_file)

            if self.stats:
                print("\n")

            loader = DescriptorLoader.DescriptorLoader(db)
            loader.ingest(generator, batchsize=self.batchsize,
                                     numthreads=self.numthreads,
                                     stats=self.stats)

            dbstatus = Status.Status(db)
            total_descriptors += len(generator)
            self.assertEqual(total_descriptors, dbstatus.count_entities("_Descriptor"))

    def test_BlobLoader(self):

        # Insert Person Nodes
        db = self.create_connection()

        in_csv_file = "./input/blobs.adb.csv"

        generator = BlobLoader.BlobGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = BlobLoader.BlobLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_entities("_Blob"))

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


class TestURILoader(TestLoader):

    def test_S3Loader(self):

        # Insert Images
        db = self.create_connection()
        dbstatus = Status.Status(db)
        count_before = dbstatus.count_images()

        in_csv_file = "./input/s3_images.adb.csv"
        generator = ImageLoader.ImageGeneratorCSV(in_csv_file, check_image=True)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_images() - count_before)

    def test_HttpImageLoader(self):

        # Insert Images
        db = self.create_connection()
        dbstatus = Status.Status(db)
        count_before = dbstatus.count_images()

        in_csv_file = "./input/http_images.adb.csv"
        generator = ImageLoader.ImageGeneratorCSV(in_csv_file, check_image=True)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_images() - count_before)

