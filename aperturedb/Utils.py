import logging
import json
import os
import importlib
import sys

from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb import ProgressBar
from aperturedb.ParallelQuery import execute_batch
from aperturedb.Configuration import Configuration
from aperturedb.cli.configure import ls


logger = logging.getLogger(__name__)

DESCRIPTOR_CLASS = "_Descriptor"
DESCRIPTOR_CONNECTION_CLASS = "_DescriptorSetToDescriptor"

DEFAULT_METADATA_BATCH_SIZE = 100_000


def import_module_by_path(filepath):
    """
    This function imports a module given a path to a python file.
    """
    module_name = os.path.basename(filepath)[:-3]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def __create_connector(configuration: Configuration):
    if configuration.use_rest:
        connector = ConnectorRest(
            host=configuration.host,
            port=configuration.port,
            user=configuration.username,
            password=configuration.password,
            config=configuration)
    else:
        connector = Connector(
            host=configuration.host,
            port=configuration.port,
            user=configuration.username,
            password=configuration.password,
            config=configuration)
    logger.info(f"Connected Using: {configuration}")
    return connector


def create_connector():
    """
    **Create a connector to the database.**

    Args:
        None

    Returns:
        Connector: The connector to the database.
    """
    all_configs = ls(log_to_console=False)

    ac = all_configs["active"]
    config = None
    if ac is not None:
        # check if the active config is in the global or local
        # Local should be the final choice
        if "global" in all_configs and ac in all_configs["global"]:
            config = all_configs["global"][ac]
        if "local" in all_configs and ac in all_configs["local"]:
            config = all_configs["local"][ac]
    assert config is not None, "No active configuration found."

    env_config = os.environ.get("APERTUREDB_CONFIG")
    if env_config is not None:
        # TODO test me.
        config = all_configs["global"][env_config] if env_config in all_configs["global"] else all_configs["local"][env_config]
        return __create_connector(config)
    # Then check if the local config has active
    else:
        return __create_connector(config)


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

    def execute(self, query, blobs=[], success_statuses=[0]):
        try:
            rc, r, b = execute_batch(
                query, blobs, self.connector, success_statuses=success_statuses)
        except BaseException as e:
            logger.error(self.connector.get_last_response_str())
            raise e

        if rc != 0:
            raise Exception(f"query failed with result {rc}")

        return r, b

    def status(self):

        q = [{"GetStatus": {}}]

        self.execute(q)

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
            return total_elements

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
                  f" {k.ljust(max)} | {str(p[k][0]).rjust(9)} "
                  f"({int(p[k][0]/total_elements*100.0)}%)")

        return total_elements

    def summary(self):

        r = self.get_schema()
        s = json.loads(self.status())[0]["GetStatus"]
        version = s["version"]
        status  = s["status"]
        info    = s["info"]

        if r["entities"] == None:
            total_entities = 0
            entities_classes = []
        else:
            total_entities   = r["entities"]["returned"]
            entities_classes = [c for c in r["entities"]["classes"]]

        if r["connections"] == None:
            total_connections = 0
            connections_classes = []
        else:
            total_connections   = r["connections"]["returned"]
            connections_classes = [c for c in r["connections"]["classes"]]

        print(f"================== Summary ==================")
        print(f"Database: {self.connector.host}")
        print(f"Version: {version}")
        print(f"Status:  {status}")
        print(f"Info:    {info}")
        print(f"------------------ Entities -----------------")
        print(f"Total entities types:    {total_entities}")
        total_nodes = 0
        for c in entities_classes:
            total_nodes += self._object_summary(c, r["entities"]["classes"][c])

        print(f"---------------- Connections ----------------")
        print(f"Total connections types: {total_connections}")
        total_edges = 0
        for c in connections_classes:
            total_edges += self._object_summary(c,
                                                r["connections"]["classes"][c])

        print(f"------------------ Totals -------------------")
        print(f"Total nodes: {total_nodes}")
        print(f"Total edges: {total_edges}")
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
            self.execute(q, success_statuses=[0, 2])
        except:
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
            self.execute(q)
        except:
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

        res, _ = self.execute(q)
        total_images = res[0]["FindImage"]["count"]

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

        res, _ = self.execute(q)
        total_elements = res[0]["FindEntity"]["batch"]["total_elements"]

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

            res, _ = self.execute(q)
            ids += [element["_uniqueid"]
                    for element in res[0]["FindEntity"]["entities"]]

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

        res, _ = self.execute(q)
        total_connections = res[0]["FindBoundingBox"]["count"]

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

        res, _ = self.execute(q)
        total_entities = res[0]["FindEntity"]["count"]

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

        res, _ = self.execute(q)
        total_connections = res[0]["FindConnection"]["count"]

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

        try:
            self.execute(query)
        except:
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

        res, _ = self.execute(q)
        total_descriptor_sets = res[0]["FindDescriptorSet"]["count"]

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
        res, _ = self.execute(q)
        if res[0]["FindDescriptorSet"]["returned"] > 0:
            sets = [ent["_name"]
                    for ent in res[0]["FindDescriptorSet"]["entities"]]

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
            self.execute(q)
        except:
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

            try:
                self.execute(q)
            except:
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

        try:
            r, _ = self.execute(query)
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

        try:
            r, _ = self.execute(query)
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

        res, _ = self.execute(q)
        total = res[1]["FindDescriptor"]["count"]

        return total

    def remove_all_indexes(self):
        """Remove all indexes from the database.

        This may improve the performance of remove_all_objects.
        It may improve or degrade the performance of other operations.

        Note that this only removes populated indexes.
        """
        def find_indexes(schema):
            typemap = dict(entities="entity", connections="connection")
            for typ, data in schema['GetSchema'].items():
                if typ in typemap and type(data) == dict and 'classes' in data:
                    for clas, classdata in data['classes'].items():
                        if 'properties' in classdata and classdata['properties']:
                            for property_key, (_, indexed, _) in classdata['properties'].items():
                                if indexed:
                                    yield {"index_type": typemap[typ], "class": clas, "property_key": property_key}

        try:
            r, _  = self.execute([{"GetSchema": {}}])
            schema = r[0]
            indexes = list(find_indexes(schema))
            query = [{"RemoveIndex": index} for index in indexes]
            query.append({"GetSchema": {}})
            r2, _ = self.execute(query)
            schema2 = r2[-1]
            indexes2 = list(find_indexes(schema2))
            if indexes2:
                logger.error(f"Failed to remove all indexes: {indexes2}")
                return False
        except BaseException as e:
            logger.exception(e)
            return False

        return True

    def remove_all_objects(self):

        cmd = {"constraints": {"_uniqueid": ["!=", "0.0.0"]}}

        transaction = [
            {"DeleteImage": cmd},
            {"DeleteVideo": cmd},
            {"DeleteBlob": cmd},
            {"DeleteClip": cmd},
            # DeleteDescriptorSet also deletes the descriptors
            {"DeleteDescriptorSet": cmd},

            # All the following should be deleted automatically once the
            # related objects are deleted.
            # We keep them here until ApertureDB fully implements this.
            {"DeleteBoundingBox": cmd},
            {"DeletePolygon": cmd},
            {"DeleteFrame": cmd},
            {"DeleteEntity": cmd},
            {"GetSchema": {"refresh": True}}
        ]

        try:
            response, _ = self.execute(transaction)
            schema = response[-1]["GetSchema"]
            if schema["status"] != 0:
                logger.error(f"status is non-zero: {response}")
            elif schema["connections"] is not None:
                logger.error(f"connections is not None: {response}")
            elif schema["entities"] is not None:
                logger.error(f"entities is not None: {response}")
            else:
                return True
        except BaseException as e:
            logger.exception(e)

        return False
