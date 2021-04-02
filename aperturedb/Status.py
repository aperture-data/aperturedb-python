import os
import sys
import time

class Status(object):

    def __init__(self, connector):

        self.connector = connector

    def status(self):

        q = [{ "GetStatus": {} }]

        try:
            res, blobs = self.connector.query(q)
        except:
            print(self.connector.get_last_response_str())
            raise

        return self.connector.get_last_response_str()

    def count_images(self, constraints={}):

        q = [{
            "FindImage": {
                "results": {
                    "blob": False,
                    "count": "",
                }
            }
        }]

        if constraints:
            q[0]["FindImage"]["constraints"] = constraints

        try:
            res, blobs = self.connector.query(q)
            total_images = res[0]["FindImage"]["count"]
        except:
            total_images = 0
            print(self.connector.get_last_response_str())
            raise

        return total_images

    def count_entities(self, entity_class, constraints={}):

        q = [{
            "FindEntity": {
                "class": entity_class,
                "results": {
                    "count": "",
                }
            }
        }]

        if constraints:
            q[0]["FindEntity"]["constraints"] = constraints

        try:
            res, blobs = self.connector.query(q)
            total_entities = res[0]["FindEntity"]["count"]
        except:
            total_entities = 0
            print(self.connector.get_last_response_str())
            raise

        return total_entities

    def count_connections(self, connections_class, constraints={}):

        q = [{
            "FindConnection": {
                "class": connections_class,
                "results": {
                    "count": "",
                }
            }
        }]

        if constraints:
            q[0]["FindConnection"]["constraints"] = constraints

        try:
            res, blobs = self.connector.query(q)
            total_connections = res[0]["FindConnection"]["count"]
        except:
            total_connections = 0
            print(self.connector.get_last_response_str())
            raise

        return total_connections
