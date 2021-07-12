import os
import sys
import time

import vdms

BACKOFF_TIME = 0.2 # seconds
BACKOFF_MULTIPLIER = 1.2

class Connector(object):

    def __init__(self, ip="localhost", port=55555):

        self.ip   = ip
        self.port = port

        self.connector = vdms.vdms()
        self.connector.connect(ip, port)

        self.last_query_time = 0

    def __del__(self):

        self.connector.disconnect()

    def create_new_connection(self):

        return Connector(self.ip, self.port)

    def query(self, q, blobs=[], n_retries=0):

        if n_retries == 0:
            start = time.time()
            self.response, self.blobs = self.connector.query(q, blobs)
            self.last_query_time = time.time() - start
            return self.response, self.blobs

        status  = -1
        retries = 0
        wait_time = BACKOFF_TIME
        while (status < 0) :

            if retries > 0:
                time.sleep(wait_time)
                wait_time *= BACKOFF_MULTIPLIER

            if retries > n_retries:
                error_msg  = "Warning: Query failed after " + str(n_retries)
                error_msg += " retries\n"
                sys.stderr.write(error_msg)
                sys.stderr.write("Response: \n")
                sys.stderr.write(self.connector.get_last_response_str())
                sys.stderr.write("\n")
                sys.stderr.write("Query: \n")
                sys.stderr.write(str(q))
                sys.stderr.write("\n")
                break

            start = time.time()
            self.response, self.blobs = self.connector.query(q, blobs)
            self.last_query_time = time.time() - start
            status = self.check_status(self.response)

            retries += 1

        return self.response, self.blobs

    def get_last_response_str(self):

        return self.connector.get_last_response_str()

    def get_last_query_time(self):

        return self.last_query_time

    def get_response(self):

        return self.response

    def get_blobs(self):

        return self.blobs

    # Return a flag on whether the last query succeeded:
    # i.e., all commands return status >= 0
    def last_query_ok(self):

        return self.check_status(self.response) >= 0

    def check_status(self, json_res):

        status = 0
        if (isinstance(self.response, dict)):
            if ("status" not in self.response):
                status = self.check_status(json_res[list(json_res.keys())[0]])
            else:
                status = json_res["status"]
        elif (isinstance(json_res, (tuple, list))):
            if ("status" not in json_res[0]):
                status = self.check_status(json_res[0])
            else:
                status = json_res[0]["status"]

        return status
