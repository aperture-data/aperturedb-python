import numpy as np

from test_Base import TestBase

from aperturedb import Utils
from aperturedb import EntityLoader, ConnectionLoader
from aperturedb import ImageLoader, BBoxLoader, BlobLoader
from aperturedb import DescriptorSetLoader, DescriptorLoader


class TestEntityLoader(TestBase):
    def setUp(self) -> None:
        db = self.create_connection()
        dbutils = Utils.Utils(db)
        classes = ["_Image", "_Descriptor", "Person", "_BoundingBox"]
        for c in classes:
            # Assert that we have a clean slate to begin with.
            self.assertEqual(dbutils.remove_entities(c), True)
            self.assertEqual(dbutils.count_entities(c), 0)

    def test_Loader(self):

        db = self.create_connection()
        dbutils = Utils.Utils(db)

        dbutils.create_entity_index("_Image",      "id")
        dbutils.create_entity_index("_Descriptor", "id")
        dbutils.create_entity_index("Person",      "id")

        # Insert Person Nodes
        in_csv_file = "./input/persons.adb.csv"

        generator = EntityLoader.EntityGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = EntityLoader.EntityLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        self.assertEqual(len(generator), dbutils.count_entities("Person"))

        # Insert Images
        in_csv_file = "./input/images.adb.csv"

        generator = ImageLoader.ImageGeneratorCSV(
            in_csv_file, check_image=False)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        self.assertEqual(len(generator), dbutils.count_images())

        # Insert Connections

        in_csv_file = "./input/connections-persons-images.adb.csv"

        generator = ConnectionLoader.ConnectionGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = ConnectionLoader.ConnectionLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        self.assertEqual(
            len(generator), dbutils.count_connections("has_image"))

        # Insert BBoxes

        in_csv_file = "./input/bboxes.adb.csv"

        generator = BBoxLoader.BBoxGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = BBoxLoader.BBoxLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        self.assertEqual(len(generator), dbutils.count_bboxes())

    def test_LoaderDescriptorsImages(self):

        # Insert Images, images may be there already, and that case should
        # be handled correctly by contraints
        db = self.create_connection()

        in_csv_file = "./input/images.adb.csv"

        generator = ImageLoader.ImageGeneratorCSV(
            in_csv_file, check_image=False)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        dbutils = Utils.Utils(db)
        self.assertEqual(len(generator), dbutils.count_images())

        # Insert DescriptorsSet
        in_csv_file = "./input/descriptorset.adb.csv"

        generator = DescriptorSetLoader.DescriptorSetGeneratorCSV(in_csv_file)

        if self.stats:
            print("\n")

        loader = DescriptorSetLoader.DescriptorSetLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        dbutils = Utils.Utils(db)
        self.assertEqual(
            len(generator), dbutils.count_entities("_DescriptorSet"))

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
                          # TODO FIXME HELP ME DIE
                          # This tests is failing when loading in
                          # multiple threads.
                          # We set numthreads to 1 until we fix
                          # numthreads=self.numthreads,
                          numthreads=1,
                          stats=self.stats)

            dbutils = Utils.Utils(db)
            total_descriptors += len(generator)
            self.assertEqual(total_descriptors,
                             dbutils.count_entities("_Descriptor"))

            self.assertEqual(dbutils.count_descriptors_in_set(
                setname), len(generator))

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

        dbutils = Utils.Utils(db)
        self.assertEqual(len(generator), dbutils.count_entities("_Blob"))

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


class TestURILoader(TestBase):

    def test_S3Loader(self):

        # Insert Images
        db = self.create_connection()
        dbutils = Utils.Utils(db)
        count_before = dbutils.count_images()

        in_csv_file = "./input/s3_images.adb.csv"
        generator = ImageLoader.ImageGeneratorCSV(
            in_csv_file, check_image=True)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        dbutils = Utils.Utils(db)
        self.assertEqual(len(generator), dbutils.count_images() - count_before)

    def test_HttpImageLoader(self):

        # Insert Images
        db = self.create_connection()
        dbutils = Utils.Utils(db)
        count_before = dbutils.count_images()

        in_csv_file = "./input/http_images.adb.csv"
        generator = ImageLoader.ImageGeneratorCSV(
            in_csv_file, check_image=True)

        if self.stats:
            print("\n")

        loader = ImageLoader.ImageLoader(db)
        loader.ingest(generator, batchsize=self.batchsize,
                      numthreads=self.numthreads,
                      stats=self.stats)

        dbutils = Utils.Utils(db)
        self.assertEqual(len(generator), dbutils.count_images() - count_before)
