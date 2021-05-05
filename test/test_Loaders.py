import argparse
import time
import unittest

import dbinfo

from aperturedb import Connector, Status
from aperturedb import EntityLoader, ConnectionLoader
from aperturedb import ImageLoader, BBoxLoader
from aperturedb import DescriptorSetLoader, DescriptorLoader

class TestLoader(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ApertureDB Server Info
        self.db_host = dbinfo.DB_HOST
        self.db_port = dbinfo.DB_PORT

        # Config params
        self.batchsize  = 99
        self.numthreads = 31
        self.stats      = True

        db_up = False
        attempts = 0
        while(not db_up):
            try:
                db = Connector.Connector(self.db_host, self.db_port)
                db_up = True
                if (attempts > 0):
                    print("Connection to ApertureDB successful.")
            except:
                print("Attempt", attempts,
                      "to connect to ApertureDB failed, retying...")
                attempts += 1
                time.sleep(1) # sleeps 1 second

            if attempts > 10:
                print("Failed to connect to ApertureDB after 10 attempts")
                exit()

class TestEntityLoader(TestLoader):

    def test_Loader(self):

        # Insert Person Nodes
        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/persons.adb.csv"

        generator = EntityLoader.EntityGeneratorCSV(in_csv_file)

        loader = EntityLoader.EntityLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_entities("Person"))


        # Insert Images
        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/images.adb.csv"

        generator = ImageLoader.ImageGeneratorCSV(in_csv_file, check_image=False)

        loader = ImageLoader.ImageLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_images())


        # Insert Connections
        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/connections-persons-images.adb.csv"

        generator = ConnectionLoader.ConnectionGeneratorCSV(in_csv_file)

        loader = ConnectionLoader.ConnectionLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_connections("has_image"))

        # Insert BBoxes
        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/bboxes.adb.csv"

        generator = BBoxLoader.BBoxGeneratorCSV(in_csv_file)

        loader = BBoxLoader.BBoxLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_bboxes())

    def test_LoaderDescriptorsImages(self):

        # Insert Images, images may be there already, and that case should
        # be handled correctly by contraints
        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/images.adb.csv"

        generator = ImageLoader.ImageGeneratorCSV(in_csv_file, check_image=False)

        loader = ImageLoader.ImageLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_images())


        # Insert DescriptorsSet
        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/descriptorset.adb.csv"

        generator = DescriptorSetLoader.DescriptorSetGeneratorCSV(in_csv_file)

        loader = DescriptorSetLoader.DescriptorSetLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=self.stats)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_entities("VD:DESCSET"))

        # Insert Descriptors

        sets = ["setA", "setB"]

        total_descriptors = 0
        for setname in sets:
            db = Connector.Connector(self.db_host, self.db_port)

            in_csv_file = "./input/" + setname + ".adb.csv"

            generator = DescriptorLoader.DescriptorGeneratorCSV(in_csv_file)

            loader = DescriptorLoader.DescriptorLoader(db)
            print("\n")
            loader.ingest(generator, batchsize=self.batchsize,
                                     numthreads=self.numthreads,
                                     stats=self.stats)

            dbstatus = Status.Status(db)
            total_descriptors += len(generator)
            self.assertEqual(total_descriptors, dbstatus.count_entities("VD:DESC"))
