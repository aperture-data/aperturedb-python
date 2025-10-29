from aperturedb import ParallelQuery
from aperturedb.Connector import Connector
from aperturedb.Utils import Utils
from aperturedb.Subscriptable import Subscriptable

import numpy as np
import logging

# For each property of each Entity or Connection,
# the following information is returned as an array of 3 elements of the form [matched, has_index_flag, type]:
# - matched: Number of objects that match the search.
# - has_index_flag: Indicates whether the property is indexed or not.
# - type: Type for the property. See supported types.
# https://docs.aperturedata.io/query_language/Reference/db_commands/GetSchema
PROPERTIES_SCHEMA_INDEX_FLAG = 1

logger = logging.getLogger(__name__)


class ParallelLoader(ParallelQuery.ParallelQuery):
    """
    **Parallel and Batch Loader for ApertureDB**

    This takes a dataset (which is a collection of homogeneous objects)
    or a derived class, and optimally inserts them into database by splitting them
    into batches, and passing the batches to multiple workers.
    """

    def __init__(self, client: Connector, dry_run: bool = False):
        super().__init__(client, dry_run=dry_run)
        self.utils = Utils(self.client)
        self.type = "element"

    def get_entity_indexes(self, schema: dict) -> dict:
        """
        Returns a dictionary of indexes for entities' properties.

        Args:
            schema (dict): The schema dictionary to get indexes from.

        Returns:
            dict: A dictionary of entity indexes.
        """

        indexes = {}

        entities = schema.get("entities") or {}

        for cls_name, cls_schema in (entities.get("classes") or {}).items():
            for prop_name, prop_schema in (cls_schema.get("properties") or {}).items():
                if prop_schema[PROPERTIES_SCHEMA_INDEX_FLAG]:
                    indexes.setdefault("entity", {}).setdefault(
                        cls_name, set()).add(prop_name)

        return indexes

    def get_connection_indexes(self, schema: dict) -> dict:
        """
        Returns a dictionary of indexes for connections' properties.

        Args:
            schema (dict): The schema dictionary to get indexes from.

        Returns:
            dict: A dictionary of connection indexes.
        """

        indexes = {}

        connections = schema.get("connections") or {}
        cls_names = connections.get("classes") or {}

        for cls_name, cls_schema in cls_names.items():

            # check if cls_schema is a dict or a list
            if isinstance(cls_schema, dict):
                for prop_name, prop_schema in (cls_schema.get("properties") or {}).items():
                    if prop_schema[PROPERTIES_SCHEMA_INDEX_FLAG]:
                        indexes.setdefault("connection", {}).setdefault(
                            cls_name, set()).add(prop_name)
            elif isinstance(cls_schema, list):
                # If cls_schema is a list, this occurs when the schema defines multiple connection variants
                # for the same class. Each element in the list is expected to be a dictionary representing
                # a connection variant, with a "properties" key containing the property schemas.
                # Example schema format:
                # "connections": {
                #     "classes": {
                #         "SomeConnectionClass": [
                #             {"properties": {"prop1": [...], "prop2": [...]}},
                #             {"properties": {"prop3": [...], "prop4": [...]}},
                #         ]
                #     }
                # }
                for connection in cls_schema:
                    for prop_name, prop_schema in (connection.get("properties") or {}).items():
                        if prop_schema[PROPERTIES_SCHEMA_INDEX_FLAG]:
                            indexes.setdefault("connection", {}).setdefault(
                                cls_name, set()).add(prop_name)
            else:
                exception_msg = "Unexpected schema format for connection class "
                exception_msg += f"'{cls_name}': {cls_schema}"
                logger.error(exception_msg)
                raise ValueError(exception_msg)

        return indexes

    def get_existing_indices(self):

        indexes = {}
        schema = self.utils.get_schema()

        if schema:
            entity_indexes     = self.get_entity_indexes(schema)
            connection_indexes = self.get_connection_indexes(schema)

            # Combine both entity and connection indexes
            indexes = {**entity_indexes, **connection_indexes}

        return indexes

    def query_setup(self, generator: Subscriptable) -> None:
        """
        Runs the setup for the loader, which includes creating indices.
        Currently, it only creates indices for the properties that are used for constraint.

        Will only run when the argument generator has a get_indices method that returns
        a dictionary of the form:

        ``` python
        {
            "entity": {
                "class_name": ["property_name"]
            },
        }

        or

        {
            "connection": {
                "class_name": ["property_name"]
            },
        }
        ```

        Args:
            generator (Subscriptable): The Subscriptable object that is being ingested
        """
        if hasattr(generator, "get_indices"):
            schema = self.utils.get_schema()

            indices_needed = generator.get_indices()

            # Create indexes for entities
            for entity_class in indices_needed.get("entity", {}):
                for property_name in indices_needed["entity"].get(entity_class, []):
                    if not self.utils.create_entity_index(entity_class, property_name):
                        logger.warning(
                            f"Failed to create index for {entity_class}.{property_name}")

            # Create indexes for connections
            for connection_class in indices_needed.get("connection", {}):
                for property_name in indices_needed["connection"].get(connection_class, []):
                    if not self.utils.create_connection_index(connection_class, property_name):
                        logger.warning(
                            f"Failed to create index for {connection_class}.{property_name}")

    def ingest(self, generator: Subscriptable, batchsize: int = 1, numthreads: int = 4, stats: bool = False) -> None:
        """
        **Method to ingest data into the database**

        Args:
            generator (Subscriptable): The list of data, or a class derived from [Subscriptable](/python_sdk/helpers/Subscriptable) to be ingested.
            batchsize (int, optional): The size of batch to be used. Defaults to 1.
            numthreads (int, optional): Number of workers to create. Defaults to 4.
            stats (bool, optional): If stats need to be presented, realtime. Defaults to False.
        """
        logger.info(
            f"Starting ingestion with batchsize={batchsize}, numthreads={numthreads}")
        self.query(generator, batchsize, numthreads, stats)

    def print_stats(self) -> None:

        times = np.array(self.get_times())
        total_queries_exec = len(times)

        print("============ ApertureDB Loader Stats ============")
        print(f"Total time (s): {self.total_actions_time}")
        print(f"Total queries executed: {total_queries_exec}")

        succeeded_queries = sum([stat["succeeded_queries"]
                                for stat in self.actual_stats])
        succeeded_commands = sum([stat["succeeded_commands"]
                                  for stat in self.actual_stats])

        if succeeded_queries == 0:
            print("All queries failed!")

        else:
            mean = np.mean(times)
            std = np.std(times)
            tp = 0 if mean == 0 else 1 / mean * self.numthreads

            print(f"Avg Query time (s): {mean}")
            print(f"Query time std: {std}")
            print(f"Avg Query Throughput (q/s): {tp}")

            i_tp = self.total_actions / self.total_actions_time
            print(
                f"Overall insertion throughput ({self.type}/s): {i_tp if self.error_counter == 0 else 'NaN'}")

            if self.error_counter > 0:
                err_perc = 100 * self.error_counter / total_queries_exec
                print(f"Total errors encountered: {self.error_counter}")
                print(f"Errors (%): {err_perc}")

            # TODO this does not take into account that the last
            # batch may be smaller than batchsize
            print(f"Total inserted elements: {succeeded_queries}")
            print(f"Total successful commands: {succeeded_commands}")
        print("=================================================")
