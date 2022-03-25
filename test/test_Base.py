import time
import unittest

import dbinfo

from aperturedb import Connector


class TestBase(unittest.TestCase):

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
                db = Connector.Connector(self.db_host, self.db_port,
                                         user=self.user, password=self.password)
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
