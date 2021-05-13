import argparse
import time
import unittest

import dbinfo

from aperturedb import Connector, Status
from aperturedb import Images
from aperturedb import PyTorchDataset

class TestTorch(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ApertureDB Server Info
        self.db_host = dbinfo.DB_HOST
        self.db_port = dbinfo.DB_PORT

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

class TestTorchDatasets(TestTorch):

    '''
        These tests need to be run after the Loaders, because it uses
        data inserted by the loaders.
    '''

    def test_omConstraints(self):

        db = Connector.Connector(self.db_host, self.db_port)

        const = Images.Constraints()
        const.greaterequal("age", 0)
        dataset = PyTorchDataset.ApertureDBDatasetConstraints(db, constraints=const)

        dbstatus = Status.Status(db)
        self.assertEqual(len(dataset), dbstatus.count_images())

        start = time.time()

        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("\n")
        print("Throughput (imgs/s):", len(dataset) / (time.time() - start))

    def test_nativeContraints(self):

        db = Connector.Connector(self.db_host, self.db_port)

        query = [ {
            "FindImage": {
                "constraints": {
                    "age": [">=", 0]
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": 224,
                        "height": 224
                    }
                ],
                "results": {
                    "list": ["license"]
                }
            }
        }]

        dataset = PyTorchDataset.ApertureDBDataset(db, query, label_prop="license")

        dbstatus = Status.Status(db)
        self.assertEqual(len(dataset), dbstatus.count_images())

        start = time.time()

        # Iterate over dataset.
        for img in dataset:
            if len(img[0]) < 0:
                print("Empty image?")
                self.assertEqual(True, False)

        print("\n")
        print("Throughput (imgs/s):", len(dataset) / (time.time() - start))
