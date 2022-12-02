import logging
import json

from aperturedb.Connector import Connector
from aperturedb import ProgressBar

logger = logging.getLogger(__name__)

DESCRIPTOR_CLASS = "_Descriptor"
DESCRIPTOR_CONNECTION_CLASS = "_DescriptorSetToDescriptor"

DEFAULT_METADATA_BATCH_SIZE = 100_000


class Utils(object):
    """
    **A bunch of helper methods to get information from aperturedb.**

    Args:
        object (Connector): The underlying Connector.
    """

    def __init__(self, connector: Connector, verbose=False):

        self.connector = connector.create_new_connection()
        self.verbose = verbose

    def __repr__(self):

        return self.status()

    def print(self, str):

        if self.verbose:
            print(str)

    def status(self):

        q = [{"GetStatus": {}}]

        try:
            res, blobs = self.connector.query(q)
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return self.connector.get_last_response_str()

    def print_schema(self, refresh=False):

        if refresh:
            logger.warning("get_schema: refresh no longer needed.")
            logger.warning("get_schema: refresh will be deprecated.")
            logger.warning("Please remove 'refresh' parameter.")

        self.get_schema()
        self.connector.print_last_response()

    def get_schema(self, refresh=False):

        if refresh:
            logger.warning("get_schema: refresh no longer needed.")
            logger.warning("get_schema: refresh will be deprecated.")
            logger.warning("Please remove 'refresh' parameter.")

        query = [{
            "GetSchema": {
            }
        }]

        res, _ = self.connector.query(query)

        schema = {}

        try:
            schema = res[0]["GetSchema"]
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return schema

    def _object_summary(self, name, object):

        total_elements = object["matched"]

        print(f"{name.ljust(20)}")
        if "src" in object:
            print(f"  {object['src']} ====> {object['dst']}")

        print(f"  Total elements: {total_elements}")

        p = object["properties"]

        if p is None:
            return

        max = 0
        for k in p:
            max = len(k) if max < len(k) else max
        max += 1

        for k in p:
            i = w = " "
            i = "I" if p[k][1] else i

            # Warning if there are some properties not present in all elements
            w = "!" if p[k][0] != total_elements else w
            # Warning if there is a property with "id" in the name not indexes
            w = "!" if "id" in k and not p[k][1] else w
            print(f"{i} {w} {p[k][2].ljust(8)} |"
                  f" {k.ljust(max)} | {p[k][0]} "
                  f"({int(p[k][0]/total_elements*100.0)}%)")

    def summary(self):

        r = self.get_schema()
        s = json.loads(self.status())[0]["GetStatus"]
        version = s["version"]
        status  = s["status"]
        info    = s["info"]

        total_entities    = r["entities"]["returned"]
        total_connections = r["entities"]["returned"]

        entities_classes = [c for c in r["entities"]["classes"]]
        connections_classes = [c for c in r["connections"]["classes"]]

        total_images = self.count_images()

        print(f"================== Summary ==================")
        print(f"Database: {self.connector.host}")
        print(f"Version: {version}")
        print(f"Status:  {status}")
        print(f"Info:    {info}")
        print(f"Total entities types:    {total_entities}")
        print(f"Total connections types: {total_connections}")
        print(f"Total images:            {total_images}")
        print(f"------------------ Entities -----------------")
        for c in entities_classes:
            self._object_summary(c, r["entities"]["classes"][c])

        print(f"---------------- Connections ----------------")
        for c in connections_classes:
            self._object_summary(c, r["connections"]["classes"][c])

        print(f"=============================================")

    def _create_index(self, index_type, class_name, property_key):

        q = [{
            "CreateIndex": {
                "index_type":    index_type,
                "class":         class_name,
                "property_key":  property_key,
            }
        }]

        try:
            res, blobs = self.connector.query(q)
            if not self.connector.last_query_ok():
                logger.error(self.connector.get_last_response_str())
                return False
        except:
            logger.error(self.connector.get_last_response_str())
            return False

        return True

    def _remove_index(self, index_type, class_name, property_key):

        q = [{
            "RemoveIndex": {
                "index_type":   index_type,
                "class":        class_name,
                "property_key": property_key,
            }
        }]

        try:
            res, blobs = self.connector.query(q)
            if not self.connector.last_query_ok():
                logger.error(self.connector.get_last_response_str())
                return False
        except:
            logger.error(self.connector.get_last_response_str())
            return False

        return True

    def create_entity_index(self, class_name, property_key, property_type=None):

        if property_type is not None:
            logger.warning(f"create_entity_index ignores 'property_type'")
            logger.warning(f"'property_type' will be deprecated in the future")
            logger.warning(f"Please remove 'property_type' parameter")

        return self._create_index("entity", class_name, property_key)

    def create_connection_index(self, class_name, property_key, property_type=None):

        if property_type is not None:
            logger.warning(f"create_connection_index ignores 'property_type'")
            logger.warning(f"'property_type' will be deprecated in the future")
            logger.warning(f"Please remove 'property_type' parameter")

        return self._create_index("connection", class_name, property_key)

    def remove_entity_index(self, class_name, property_key):

        return self._remove_index("entity", class_name, property_key)

    def remove_connection_index(self, class_name, property_key):

        return self._remove_index("connection", class_name, property_key)

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
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return total_images

    def get_uniqueids(self, object_type, constraints={}):

        q = [{
            "FindEntity": {
                "with_class": object_type,
                "batch": {},
                "results": {
                    "list": ["_uniqueid"],
                }
            }
        }]

        if constraints:
            q[0]["FindEntity"]["constraints"] = constraints

        ids = []

        try:
            res, blobs = self.connector.query(q)
            total_elements = res[0]["FindEntity"]["batch"]["total_elements"]
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        batch_size = DEFAULT_METADATA_BATCH_SIZE
        iterations = total_elements // batch_size
        reminder   = total_elements % batch_size

        if iterations == 0 and reminder > 0:
            iterations = 1

        pb = ProgressBar.ProgressBar()

        for i in range(iterations):

            batch = {
                "batch_size": batch_size,
                "batch_id": i
            }

            q[0]["FindEntity"]["batch"] = batch

            try:
                res, blobs = self.connector.query(q)
                ids += [element["_uniqueid"]
                        for element in res[0]["FindEntity"]["entities"]]
            except BaseException as e:
                logger.error(self.connector.get_last_response_str())
                raise e

            if self.verbose:
                pb.update(i / iterations)

        if self.verbose:
            pb.update(1)  # For the end of line

        return ids

    def get_images_uniqueids(self, constraints={}):

        return self.get_uniqueids("_Image", constraints)

    def count_bboxes(self, constraints=None):
        # The default params in python functions should not be
        # mutable objects.
        # It can lead to https://docs.python-guide.org/writing/gotchas/#mutable-default-arguments
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
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return total_connections

    def count_entities(self, entity_class, constraints=None):

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
            fe = res[0]["FindEntity"]

            if fe["status"] == 1:
                # TODO: Here we return 0 entities because the query failed.
                # This is because Find* Command will return status: 1
                # and no count if no object is found.
                # We should change the Find* Command to return status: 0
                # and count: 0 if no object is found.
                total_entities = 0
            else:
                total_entities = fe["count"]

        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return total_entities

    def count_connections(self, connections_class, constraints=None):

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
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return total_connections

    def add_descriptorset(self, name, dim, metric="L2", engine="FaissFlat"):

        query = [{
            "AddDescriptorSet": {
                "name":       name,
                "dimensions": dim,
                "metric":     metric,
                "engine":     engine
            }
        }]

        response, arr = self.connector.query(query)

        expected = [{
            "AddDescriptorSet": {
                "status": 0,
            }
        }]

        if response != expected:
            logger.error(self.connector.get_last_response_str())
            return False

        return True

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
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return total_descriptor_sets

    def get_descriptorset_list(self):

        q = [{
            "FindDescriptorSet": {
                "results": {
                    "list": ["_name"],
                }

            }
        }]

        sets = []
        try:
            res, _ = self.connector.query(q)

            sets = [ent["_name"]
                    for ent in res[0]["FindDescriptorSet"]["entities"]]
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return sets

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
                logger.error(self.connector.get_last_response_str())
                return False
        except:
            logger.error(self.connector.get_last_response_str())
            return False

        return True

    def _remove_objects(self, type, class_name, batch_size):

        if type == "entities":
            cmd = "Entity"
            count = self.count_entities(class_name)
        elif type == "connections":
            cmd = "Connection"
            count = self.count_connections(class_name)
        else:
            raise ValueError("Type must be either 'entities' or 'connections'")

        total = count

        pb = ProgressBar.ProgressBar()

        find = "Find" + cmd
        dele = "Delete" + cmd
        while count > 0:

            q = [{
                find: {
                    "_ref": 1,
                    "with_class": class_name,
                    "results": {
                        "limit": batch_size
                    }
                }
            }, {
                dele: {
                    "ref": 1
                }
            }]

            res, _ = self.connector.query(q)

            if not self.connector.last_query_ok():
                logger.error(self.connector.get_last_response_str())
                return False

            self.print(
                f"Delete transaction of {batch_size} elements took: {self.connector.get_last_query_time()}")

            count -= batch_size

            if self.verbose:
                pb.update(abs((count - total) / total))

        return True

    def remove_entities(self, class_name, batched=False, batch_size=10000):

        # We used to batch because removing a large number of object did not work ok.
        # Now it seems to work ok, so we don't batch anymore.
        # We keep the option in case we need to batch, but not default.
        if batched:
            return self._remove_objects("entities", class_name, batch_size)

        query = [{
            "DeleteEntity": {
                "with_class": class_name
            }
        }]

        r, _ = self.connector.query(query)

        if not self.connector.last_query_ok():
            logger.error(self.connector.get_last_response_str())
            return False

        try:
            count = r[0]["DeleteEntity"]["count"]
            self.print(f"Deleted {count} entities")
            self.print(
                f"Delete transaction took: {self.connector.get_last_query_time()}")
        except:
            logger.error("Could not get count of deleted entities")
            return False

        return True

    def remove_connections(self, class_name, batched=False, batch_size=10000):

        # We used to batch because removing a large number of object did not work ok.
        # Now it seems to work ok, so we don't batch anymore.
        # We keep the option in case we need to batch, but not default.
        if batched:
            return self._remove_objects("connections", class_name, batch_size)

        query = [{
            "DeleteConnection": {
                "with_class": class_name
            }
        }]

        r, _ = self.connector.query(query)

        if not self.connector.last_query_ok():
            logger.error(self.connector.get_last_response_str())
            return False

        try:
            count = r[0]["DeleteConnection"]["count"]
            self.print(f"Deleted {count} connections")
            self.print(
                f"Delete transaction took: {self.connector.get_last_query_time()}")
        except:
            logger.error("Could not get count of deleted connections")
            return False

        return True

    def remove_all_descriptorsets(self):

        self.print("Removing indexes...")

        idx_props = self.get_indexed_props(DESCRIPTOR_CLASS)

        for idx in idx_props:
            self.remove_entity_index(DESCRIPTOR_CLASS, idx)

        self.print("Done removing indexes.")

        self.print("Removing connections...")
        if not self.remove_connections(DESCRIPTOR_CONNECTION_CLASS):
            print("Error removing connections.")
            return False
        self.print("Done removing connections.")

        self.print("Removing descriptors...")
        if not self.remove_entities(DESCRIPTOR_CLASS):
            print("Error removing descriptors.")
            return False
        self.print("Done removing descriptors.")

        sets = self.get_descriptorset_list()

        self.print("Removing sets...")
        for s in sets:
            self.print("Removing {}...".format(s))
            self.remove_descriptorset(s)

        self.print("Done removing sets.")

    def get_indexed_props(self, class_name, type="entities"):
        """
        Returns all the indexed properties for a given class.
        """

        if type not in ["entities", "connections"]:
            raise ValueError("Type must be either 'entities' or 'connections'")

        schema = self.get_schema()

        try:
            indexed_props = schema[type]["classes"][class_name]["properties"]
            indexed_props = [
                k for k in indexed_props.keys() if indexed_props[k][1]]
        except:
            indexed_props = []

        return indexed_props

    def count_descriptors_in_set(self, set_name):

        total = -1

        q = [{
            "FindDescriptorSet": {
                "_ref": 1,
                "with_name": set_name,
            }
        }, {
            "FindDescriptor": {
                "set": set_name,
                "is_connected_to": {
                    "ref": 1,
                    "connection_class": "_DescriptorSetToDescriptor"
                },
                "results": {
                    "count": True
                }
            }
        }]

        try:
            res, _ = self.connector.query(q)
            if not self.connector.last_query_ok():
                logger.error(self.connector.get_last_response_str())
            else:
                total = res[1]["FindDescriptor"]["count"]
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return total

    def remove_all_objects(self):

        cmd = {"constraints": {"_uniqueid": ["!=", "0.0.0"]}}

        # There is no DeleteDescriptor, but when the sets are removed
        # all the descriptors are also removed.
        queries = [
            [{"DeleteDescriptorSet": cmd}],
            # [{"DeleteDescriptor": cmd}],
            [{"DeleteBoundingBox": cmd}],
            [{"DeleteVideo": cmd}],
            [{"DeleteImage": cmd}],
            [{"DeleteBlob": cmd}],
            [{"DeletePolygon": cmd}],
            [{"DeleteEntity": cmd}],
        ]

        try:
            for q in queries:
                response, _ = self.connector.query(q)
                if not self.connector.last_query_ok():
                    logger.error(self.connector.get_last_response_str())
                    return False

            response, _ = self.connector.query(
                [{"GetSchema": {"refresh": True}}])

            if not self.connector.last_query_ok():
                logger.error(self.connector.get_last_response_str())
                return False

            entities    = response[0]["GetSchema"]["entities"]
            connections = response[0]["GetSchema"]["connections"]

            if entities is not None:
                logger.error("Entities not removed completely")
                logger.error(self.connector.get_last_response_str())
                return False

            if connections is not None:
                logger.error("Connections not removed completely")
                logger.error(self.connector.get_last_response_str())
                return False

        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        return True
