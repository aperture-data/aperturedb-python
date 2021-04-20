import argparse
import time
import unittest

import dbinfo

from aperturedb import Connector, EntityLoader
from aperturedb import Status

class TestLoader(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ApertureDB Server Info
        self.db_host = dbinfo.DB_HOST
        self.db_port = dbinfo.DB_PORT

        # Config params
        self.batchsize  = 99
        self.numthreads = 31

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

    def test_entityLoader(self):

        db = Connector.Connector(self.db_host, self.db_port)

        in_csv_file = "./input/persons.adb.csv"

        # print("Creating Generator from CSV...")
        generator = EntityLoader.EntityGeneratorCSV(in_csv_file)
        # print("Generator done.")

        loader = EntityLoader.EntityLoader(db)
        print("\n")
        loader.ingest(generator, batchsize=self.batchsize,
                                 numthreads=self.numthreads,
                                 stats=True)

        dbstatus = Status.Status(db)
        self.assertEqual(len(generator), dbstatus.count_entities("Person"))
