
from aperturedb import ParallelQuery
from aperturedb.Connector import Connector
from aperturedb.Utils import Utils
from aperturedb.Subscriptable import Subscriptable

import numpy as np
import logging
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

    def get_existing_indices(self):
        schema = self.utils.get_schema()
        existing_indices = {}
        if schema:
            for index_type in (("entity", "entities"), ("connection", "connections")):
                foo = schema.get(index_type[1]) or {}
                bar = foo.get("classes") or {}
                for cls_name, cls_schema in bar.items():
                    props = cls_schema.get("properties") or {}
                    for prop_name, prop_schema in props.items():
                        if prop_schema[1]:  # indicates property has an index
                            existing_indices.setdefault(index_type[0], {}).setdefault(
                                cls_name, set()).add(prop_name)
        return existing_indices

    def query_setup(self, generator: Subscriptable) -> None:
        """
        Runs the setup for the loader, which includes creating indices
        Currently, it only creates indices for the properties that are
        also used for constraint.

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
            for schema_type, schema_type_plural in [("entity", "entities"), ("connection", "connections")]:
                for entity_class in indices_needed.get(schema_type, {}):
                    for property_name in indices_needed[schema_type][entity_class]:
                        schema_type = schema.get(schema_type_plural, {}) or {}
                        if property_name not in schema_type.get('classes', {}).get(entity_class, {}).get('properties', {}):
                            if not self.utils.create_entity_index(entity_class, property_name):
                                logger.warning(
                                    f"Failed to create index for {entity_class}.{property_name}")
                        else:
                            logger.info(
                                f"Index for {entity_class}.{property_name} already exists")

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
