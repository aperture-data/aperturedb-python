import os

import vdms

from . import Images

class Connector(object):

    def __init__(self, ip="localhost", port=55555):

        self.ip   = ip
        self.port = port

        self.connector = vdms.vdms()
        self.connector.connect(ip, port)

    def __del__(self):

        self.connector.disconnect()

    def query(self, query, blob_array = []):

        self.response, self.blobs = self.connector.query(query, blob_array)

        return self.response, self.blobs

    def get_last_response_str(self):

        return self.connector.get_last_response_str()

    def get_response(self):

        return self.response

    def get_blobs(self):

        return self.blobs

    def get_image_handler(self):

        db = vdms.vdms()
        db.connect(self.ip, self.port)

        return Images.Images(db)
