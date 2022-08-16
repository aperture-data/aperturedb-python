import time
import dbinfo
import unittest
from aperturedb import Connector


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # import pdb;pdb.set_trace()
        # ApertureDB Server Info
        cls.db_host  = dbinfo.DB_HOST
        cls.db_port  = dbinfo.DB_PORT
        cls.user     = dbinfo.DB_USER
        cls.password = dbinfo.DB_PASSWORD

        # Config params
        cls.batchsize  = 99
        cls.numthreads = 31
        cls.stats      = False

        db_up = False
        attempts = 0
        while(not db_up):
            try:
                db = Connector.Connector(cls.db_host, cls.db_port,
                                         user=cls.user, password=cls.password)
                db_up = True
                if (attempts > 0):
                    print("Connection to ApertureDB successful.")
            except:
                print("Attempt", attempts,
                      "to connect to ApertureDB failed, retrying...")
                # db.print_last_response()

                attempts += 1
                time.sleep(1)  # sleeps 1 second

            if attempts > 10:
                print("Failed to connect to ApertureDB after 10 attempts")
                exit()

    def create_connection(self):
        return Connector.Connector(self.db_host, self.db_port, self.user, self.password)
