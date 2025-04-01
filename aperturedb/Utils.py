"""
Miscellaneous utility functions for ApertureDB.
This class contains a collection of helper functions to interact with the database.
"""
from aperturedb.Query import QueryBuilder
from aperturedb.CommonLibrary import execute_query
from tqdm import tqdm
from aperturedb.Connector import Connector
import logging
import json
from typing import List, Optional, Dict

HAS_GRAPHVIZ = True
try:
    from graphviz import Source, Digraph
except:
    HAS_GRAPHVIZ = False

    class Source:
        pass

    class Digraph:
        pass


logger = logging.getLogger(__name__)

DESCRIPTOR_CLASS = "_Descriptor"
DESCRIPTOR_CONNECTION_CLASS = "_DescriptorSetToDescriptor"
DEFAULT_METADATA_BATCH_SIZE = 100_000


class Utils(object):
    """
    **Helper methods to get information from aperturedb, or affect it's state.**

    Args:
        object (Connector): The underlying Connector.
    """

    def __init__(self, client: Connector, verbose=False):
        self.client: Connector = client.clone()
        self.verbose = verbose

    def __repr__(self):
        return f"{id(self)}"

    def print(self, str):
        if self.verbose:
            print(str)

    def execute(self, query, blobs=[], success_statuses=[0]):
        """
        Execute a query.

        Args:
            query (list): The query to execute.
            blobs (list, optional): The blobs to send with the query.
            success_statuses (list, optional): The list of success statuses.

        Returns:
            result: The result of the query.
            blobs: The blobs returned by the query.
        """
        try:
            rc, r, b = execute_query(self.client,
                                     query, blobs, success_statuses=success_statuses)
        except BaseException as e:
            logger.error(self.client.get_last_response_str())
            raise e

        if rc != 0:
            raise Exception(f"query failed with result {rc}")

        return r, b

    def status(self):
        """
        Executes a `GetStatus` query.
        See :ref:`GetStatus <GetStatus>`_ in the ApertureDB documentation for more information.
        """

        q = [{"GetStatus": {}}]

        self.execute(q)

        return self.client.get_last_response_str()

    def print_schema(self, refresh=False):

        if refresh:
            logger.warning("get_schema: refresh no longer needed.")
            logger.warning("get_schema: refresh will be deprecated.")
            logger.warning("Please remove 'refresh' parameter.")

        self.get_schema()
        self.client.print_last_response()

    def get_schema(self, refresh=False):
        """
        Get the schema of the database.
        See :ref:`GetSchema <GetSchema>`_ in the ApertureDB documentation for more information.
        """

        if refresh:
            logger.warning("get_schema: refresh no longer needed.")
            logger.warning("get_schema: refresh will be deprecated.")
            logger.warning("Please remove 'refresh' parameter.")

        query = [{
            "GetSchema": {
            }
        }]

        res, _ = self.execute(query)

        schema = {}

        try:
            schema = res[0]["GetSchema"]
        except BaseException as e:
            logger.error(self.client.get_last_response_str())
            raise e

        return schema

    def visualize_schema(self, filename: str = None, format: str = "png") -> Source:
        """
        Visualize the schema of the database.  Optionally save the visualization to a file in the specified format.

        The returned object can be rendered to a file as follows:
        ```python
        s = utils.visualize_schema()
        s.render("schema", format="png")
        ```

        It can also be displayed inline in a Jupyter notebook:
        ```python
        from IPython.display import display
        s = utils.visualize_schema()
        display(s)
        ```

        Relies on graphviz to be installed.

        Args:
            filename (str, optional): The filename to save the visualization to. Default is None.
            format (str, optional): The format to save the visualization to. Default is "png".

        Returns:
            source: The visualization of the schema.
        """
        if not HAS_GRAPHVIZ:
            raise Exception("graphviz not installed.")
        r = self.get_schema()

        colors = dict(
            edge="#3A3B9C",
            entity_background="#2A2E78",
            entity_foreground="#E2E0F1",
            property_background="#337EC0",
            property_foreground="#E2E0F1",
            connection_background="#5956F1",
            connection_foreground="#E2E0F1",
            connection_property_background="#33E1FF",
            connection_property_foreground="#2A2E78"
        )

        dot = Digraph(comment='ApertureDB Schema Diagram', node_attr={
                      'shape': 'none'}, graph_attr={'rankdir': 'LR'}, edge_attr={'color': colors['edge']})

        # Add entities as nodes and connections as edges
        entities = r['entities']['classes']
        connections = r['connections']['classes']

        for entity, data in entities.items():
            matched = data["matched"]
            # dictionary from name to (matched, indexed, type)
            properties = data["properties"]
            table = f'''<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
            <TR><TD BGCOLOR="{colors["entity_background"]}" COLSPAN="3"><FONT COLOR="{colors["entity_foreground"]}"><B>{entity}</B> ({matched:,})</FONT></TD></TR>
            '''
            for prop, (matched, indexed, typ) in properties.items():
                table += f'<TR><TD BGCOLOR="{colors["property_background"]}"><FONT COLOR="{colors["property_foreground"]}"><B>{prop.strip()}</B></FONT></TD> <TD BGCOLOR="{colors["property_background"]}"><FONT COLOR="{colors["property_foreground"]}">{matched:,}</FONT></TD> <TD BGCOLOR="{colors["property_background"]}"><FONT COLOR="{colors["property_foreground"]}">{"Indexed" if indexed else "Unindexed"}, {typ}</FONT></TD></TR>'
            for connection, data in connections.items():
                if data['src'] == entity:
                    matched = data["matched"]
                    # dictionary from name to (matched, indexed, type)
                    properties = data["properties"]
                    table += f'<TR><TD BGCOLOR="{colors["connection_background"]}" COLSPAN="3" PORT="{connection}"><FONT COLOR="{colors["connection_foreground"]}"><B>{connection}</B> ({matched:,})</FONT></TD></TR>'
                    if properties:
                        for prop, (matched, indexed, typ) in properties.items():
                            table += f'<TR><TD BGCOLOR="{colors["connection_property_background"]}"><FONT COLOR="{colors["connection_property_foreground"]}"><B>{prop.strip()}</B></FONT></TD> <TD BGCOLOR="{colors["connection_property_background"]}"><FONT COLOR="{colors["connection_property_foreground"]}">{matched:,}</FONT></TD> <TD BGCOLOR="{colors["connection_property_background"]}"><FONT COLOR="{colors["connection_property_foreground"]}">{"Indexed" if indexed else "Unindexed"}, {typ}</FONT></TD></TR>'
            table += '</TABLE>>'
            dot.node(entity, label=table)

        for connection, data in connections.items():
            dot.edge(f'{data["src"]}:{connection}',
                     f'{data["dst"]}')

        # Render the diagram inline
        s = Source(dot.source, filename="schema_diagram.gv", format="png")

        if filename is not None:
            s.render(filename, format=format)

        return s

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
        """
        Print a summary of the database.

        This is essentially a call to :ref:`GetSchema <GetSchema>`_, with the results formatted in a more human-readable way.
        """
        r = self.get_schema()
        s = json.loads(self.status())[0]["GetStatus"]
        version = s["version"]
        status = s["status"]
        info = s["info"]

        if r["entities"] == None:
            total_entities = 0
            entities_classes = []
        else:
            total_entities = r["entities"]["returned"]
            entities_classes = [c for c in r["entities"]["classes"]]

        if r["connections"] == None:
            total_connections = 0
            connections_classes = []
        else:
            total_connections = r["connections"]["returned"]
            connections_classes = [c for c in r["connections"]["classes"]]

        print(f"================== Summary ==================")
        print(f"Database: {self.client.host}")
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
        """
        Create an index for an entity class.
        See :ref:`CreateIndex <CreateIndex>`_ in the ApertureDB documentation for more information.
        """

        if property_type is not None:
            logger.warning(f"create_entity_index ignores 'property_type'")
            logger.warning(f"'property_type' will be deprecated in the future")
            logger.warning(f"Please remove 'property_type' parameter")

        return self._create_index("entity", class_name, property_key)

    def create_connection_index(self, class_name, property_key, property_type=None):
        """
        Create an index for a connection class.
        See :ref:`CreateIndex <CreateIndex>`_ in the ApertureDB documentation for more information.
        """

        if property_type is not None:
            logger.warning(f"create_connection_index ignores 'property_type'")
            logger.warning(f"'property_type' will be deprecated in the future")
            logger.warning(f"Please remove 'property_type' parameter")

        return self._create_index("connection", class_name, property_key)

    def remove_entity_index(self, class_name, property_key):
        """
        Remove an index for an entity class.
        See :ref:`RemoveIndex <RemoveIndex>`_ in the ApertureDB documentation for more information.
        """

        return self._remove_index("entity", class_name, property_key)

    def remove_connection_index(self, class_name, property_key):
        """
        Remove an index for a connection class.
        See :ref:`RemoveIndex <RemoveIndex>`_ in the ApertureDB documentation for more information.
        """
        return self._remove_index("connection", class_name, property_key)

    def count_images(self, constraints={}) -> int:
        """
        Count the number of images in the database.

        Args:
            constraints (dict, optional): The constraints to apply to the query.
                See the [`Constraints`](../parameter_wrappers/Constraints) wrapper class for more information.

        Returns:
            count: The number of images in the database.
        """
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

    def count_bboxes(self, constraints=None) -> int:
        """
        Count the number of bounding boxes in the database.

        Args:
            constraints (dict, optional): The constraints to apply to the query.
                See the [`Constraints`](../parameter_wrappers/Constraints) wrapper class for more information.

        Returns:
            count: The number of bounding boxes in the database.
        """
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

    def count_entities(self, entity_class, constraints=None) -> int:
        """
        Count the number of entities in the database.

        Args:
            constraints (dict, optional): The constraints to apply to the query.
                See the [`Constraints`](../parameter_wrappers/Constraints) wrapper class for more information.

        Returns:
            count: The number of entities in the database.
        """
        params = {"results": {"count": True}}
        if constraints:
            params["constraints"] = constraints
        q = QueryBuilder.find_command(entity_class, params=params)
        res, _ = self.execute(query=[q])
        total_entities = res[0][list(q.keys())[0]]["count"]

        return total_entities

    def count_connections(self, connections_class, constraints=None) -> int:
        """
        Count the number of connections in the database.

        Args:
            constraints (dict, optional): The constraints to apply to the query.
                See the [`Constraints`](../parameter_wrappers/Constraints) wrapper class for more information.

        Returns:
            count: The number of connections in the database.
        """

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

    def add_descriptorset(self, name: str, dim: int, metric=["CS"],
                          engine=["HNSW"],
                          properties: Optional[Dict] = None) -> bool:
        """
        Add a descriptor set to the database.

        Args:
            name (str): The name of the descriptor set.
            dim (int): The dimension of the descriptors.
            metric (str, optional): The metric to use for the descriptors.
            engine (str, optional): The engine to use for the descriptors.
            properties (dict, optional): The properties of the descriptor set.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
        """
        query = [{
            "AddDescriptorSet": {
                "name":       name,
                "dimensions": dim,
                "metric":     metric,
                "engine":     engine
            }
        }]

        if properties is not None:
            query[0]["AddDescriptorSet"]["properties"] = properties

        try:
            self.execute(query)
        except:
            return False

        return True

    def count_descriptorsets(self) -> int:
        """
        Count the number of descriptor sets in the database.

        Returns:
            count: The number of descriptor sets in the database.
        """
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

    def get_descriptorset_list(self) -> List[str]:
        """
        Get the list of descriptor sets in the database.

        Returns:
            sets (list of str): The list of descriptor sets in the database.
        """
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

    def remove_descriptorset(self, set_name: str) -> bool:
        """
        Remove a descriptor set from the database.

        See :ref:`DeleteDescriptorSet <DeleteDescriptorSet>`_ in the ApertureDB documentation for more information.

        Args:
            set_name (str): The name of the descriptor set.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
        """
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

        pb = tqdm(total=total, desc=f"Removing {type} {class_name}",
                  unit="elements", unit_scale=True, dynamic_ncols=True)

        find = "Find" + cmd
        dele = "Delete" + cmd
        try:
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
                    f"Delete transaction of {batch_size} elements took: {self.client.get_last_query_time()}")

                count -= batch_size

                if self.verbose:
                    pb.update(batch_size)
        finally:
            pb.close()

        return True

    def remove_entities(self, class_name: str, batched: bool = False, batch_size: int = 10000) -> bool:
        """
        Remove all entities of a given class from the database.

        See :ref:`DeleteEntity <DeleteEntity>`_ in the ApertureDB documentation for more information.

        Args:
            class_name (str): The class of the entities to remove.
            batched (bool, optional): Whether to batch the operation. Default is False.
            batch_size (int, optional): The batch size to use. Default is 10000.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
        """
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
                f"Delete transaction took: {self.client.get_last_query_time()}")
        except:
            logger.error("Could not get count of deleted entities")
            return False

        return True

    def remove_connections(self, class_name, batched=False, batch_size=10000):
        """
        Remove all connections of a given class from the database.

        See :ref:`DeleteConnection <DeleteConnection>`_ in the ApertureDB documentation for more information.

        Args:
            class_name (str): The class of the connections to remove.
            batched (bool, optional): Whether to batch the operation. Default is False.
            batch_size (int, optional): The batch size to use. Default is 10000.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
        """
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
                f"Delete transaction took: {self.client.get_last_query_time()}")
        except:
            logger.error("Could not get count of deleted connections")
            return False

        return True

    def remove_all_descriptorsets(self) -> bool:
        """
        Remove all descriptor sets from the database, together with descriptors, indexes and connections.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
        """
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

    def get_indexed_props(self, class_name: str, type="entities") -> List[str]:
        """
        Returns all the indexed properties for a given class.

        Args:
            class_name (str): The class name.
            type (str, optional): The type of the class. Default is "entities".

        Returns:
            indexed_props (list of str): The list of indexed properties.
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

    def count_descriptors_in_set(self, set_name: str) -> int:
        """
        Count the number of descriptors in a descriptor set.

        Args:
            set_name (str): The name of the descriptor set.

        Returns:
            total (int): The number of descriptors in the set.
        """
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

    def remove_all_indexes(self) -> bool:
        """Remove all indexes from the database.

        This may improve the performance of remove_all_objects.
        It may improve or degrade the performance of other operations.

        Note that this only removes populated indexes.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
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
            r, _ = self.execute([{"GetSchema": {}}])
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

    def remove_all_objects(self) -> bool:
        """
        Remove all objects from the database.

        This includes images, videos, blobs, clips, descriptor sets, descriptors, bounding boxes, polygons, frames, entities, connections, indexes and connections.

        Returns:
            success (bool): True if the operation was successful, False otherwise.
        """
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

    def user_log_message(self, message: str, level: str = "INFO") -> None:
        """
        Log a message to the user log.

        This is useful because it can later be seen in Grafana, not only as log entries in the ApertureDB
        Logging dashboard, but also as event markers in the ApertureDB Status dashboard.

        Args:
            message (str): The message to log.
            level (str): The level of the message. Default is "INFO".
        """
        assert level in ["INFO", "WARNING",
                         "ERROR"], f"Invalid log level: {level}"
        q = [{"UserLogMessage": {"text": message, "type": level}}]
        self.execute(q)


def create_connector(name=None, key=None):
    from aperturedb.CommonLibrary import create_connector, issue_deprecation_warning
    issue_deprecation_warning("Utils.create_connector",
                              "CommonLibrary.create_connector")
    return create_connector(name=name, key=key)
