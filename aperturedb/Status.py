import os
import sys
import time

class Status(object):

    def __init__(self, connector):

        self.connector = connector.create_new_connection()

    def __repr__(self):

        return self.status()

    def status(self):

        q = [{ "GetStatus": {} }]

        try:
            res, blobs = self.connector.query(q)
        except:
            self.connector.print_last_response()

        return self.connector.get_last_response_str()

    def print_schema(self, refresh=False):

        self.get_schema(refresh=refresh)
        self.connector.print_last_response()

    def get_schema(self, refresh=False):

        query = [ {
            "GetSchema": {
                "refresh": refresh,
            }
        }]

        res, blobs = self.connector.query(query)

        schema = {}

        try:
            schema = res[0]["GetSchema"]
        except:
            print("Cannot retrieve schema")
            self.connector.print_last_response()

        return schema

    def count_images(self, constraints={}):

        q = [{
            "FindImage": {
                "blobs": False,
                "results": {
                    "count": True,
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
            self.connector.print_last_response()

        return total_images

    def count_bboxes(self, constraints={}):

        q = [{
            "FindBoundingBox": {
                "blobs": False,
                "results": {
                    "count": True,
                }
            }
        }]

        if constraints:
            q[0]["FindBoundingBox"]["constraints"] = constraints

        try:
            res, blobs = self.connector.query(q)
            total_connections = res[0]["FindBoundingBox"]["count"]
        except:
            total_connections = 0
            self.connector.print_last_response()

        return total_connections

    def count_entities(self, entity_class, constraints={}):

        q = [{
            "FindEntity": {
                "with_class": entity_class,
                "results": {
                    "count": True,
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
            self.connector.print_last_response()

        return total_entities

    def count_connections(self, connections_class, constraints={}):

        q = [{
            "FindConnection": {
                "with_class": connections_class,
                "results": {
                    "count": True,
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
            self.connector.print_last_response()

        return total_connections

    def count_descriptorsets(self):

        q = [{
            "FindDescriptorSet": {
                "results": {
                    "count": True,
                }
            }
        }]

        try:
            res, blobs = self.connector.query(q)
            total_descriptor_sets = res[0]["FindDescriptorSet"]["count"]
        except:
            total_descriptor_sets = 0
            self.connector.print_last_response()

        return total_descriptor_sets

    def remove_descriptorset(self, set_name):

        q = [{
            "FindDescriptorSet": {
                "_ref": 1,
                "with_name": set_name,

            }
        }, {
            "DeleteDescriptorSet": {
                "ref": 1
            }
        }]

        try:
            res, _ = self.connector.query(q)
            if not self.connector.last_query_ok():
                self.connector.print_last_response()
                return False
        except:
            self.connector.print_last_response()
            return False

        return True
