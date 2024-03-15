
from aperturedb import ParallelQuery
from aperturedb.Connector import Connector

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

    def __init__(self, db: Connector, dry_run: bool = False):
        super().__init__(db, dry_run=dry_run)
        self.type = "element"

    def get_existing_indices(self):
        schema_result, _ = self.db.query([{"GetSchema": {}}])
        schema = schema_result[0]["GetSchema"]
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

    def create_indices(self, indices) -> None:
        if len(indices) == 0:
            return

        existing_indices = self.get_existing_indices()
        new_indices = []
        for tp, classes in indices.items():
            ex_classes = existing_indices.get(tp, {})
            for cls, props in classes.items():
                ex_props = ex_classes.get(cls, set())
                for prop in props - ex_props:
                    new_indices.append({
                        "index_type": tp,
                        "class": cls,
                        "property_key": prop
                    })

        if len(new_indices) == 0:
            return

        logger.info(
            f"Creating {len(new_indices)} indices: {new_indices}.")

        create_indices = [{"CreateIndex": idx} for idx in new_indices]

        res, _ = self.db.query(
            create_indices)

        if self.db.check_status(res) != 0:
            logger.warning(
                "Failed to create indices; ingestion will be slow.")
            logger.warning(res)

    def query_setup(self, generator) -> None:
        if hasattr(generator, "get_indices"):
            self.create_indices(generator.get_indices())

    def ingest(self, generator, batchsize: int = 1, numthreads: int = 4, stats: bool = False) -> None:
        """
        **Method to ingest data into the database**

        Args:
            generator (_type_): The list of data, or a class derived from [Subscriptable](/python_sdk/helpers/Subscriptable) to be ingested.
            batchsize (int, optional): The size of batch to be ussed. Defaults to 1.
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
