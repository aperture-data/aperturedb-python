
from aperturedb import ParallelQuery

import numpy as np
import logging
logger = logging.getLogger(__name__)


class ParallelLoader(ParallelQuery.ParallelQuery):
    """
    **Parallel and Batch Loader for ApertureDB**

    """

    def __init__(self, db, dry_run=False):
        super().__init__(db, dry_run=dry_run)
        self.type = "element"

    def get_existing_indices(self):
        schema_result, _ = self.db.query([{"GetSchema": {}}])
        schema = schema_result[0]["GetSchema"]
        existing_indices = set()
        for index_type in [["entity", "entities"], ["connection", "connections"]]:
            if (index_type[1] in schema) and ("classes" in schema[index_type[1]]):
                for cls_name, cls_schema in enumerate(schema[index_type[1]]["classes"]):
                    if "properties" in cls_schema:
                        for prop_name, prop_schema in enumerate(cls_schema):
                            if prop_schema[1]:  # indicates property has an index
                                existing_indices.add({
                                    "index_type": index_type[0],
                                    "class": cls_name,
                                    "property": prop_name
                                })

    def create_indices(self, indices):
        if len(indices) == 0:
            return

        existing_indices = self.get_existing_indices()
        new_indices = indices - existing_indices
        if len(new_indices) == 0:
            return

        logger.info(
            f"Creating {len(new_indices)} indices: {new_indices}.")

        create_indices = [{"CreateIndex": idx} for idx in new_indices]

        res, _ = self.db.query(
            create_indices)

        if self.db.check_status(res) != 0:
            logger.warn(
                "Failed to create indices; ingestion will be slow.")

    def ingest(self, csv_data, batchsize=1, numthreads=4, stats=False):
        self.create_indices(csv_data.get_indices())
        logger.info(
            f"Starting ingestion with batchsize={batchsize}, numthreads={numthreads}")
        self.query(csv_data, batchsize, numthreads, stats)

    def print_stats(self):

        times = np.array(self.get_times())
        total_queries_exec = len(times)

        print("============ ApertureDB Loader Stats ============")
        print(f"Total time (s): {self.total_actions_time}")
        print(f"Total queries executed: {total_queries_exec}")

        suceeded_queries = sum([stat["suceeded_queries"]
                                for stat in self.actual_stats])
        suceeded_commands = sum([stat["suceeded_commands"]
                                for stat in self.actual_stats])

        if suceeded_queries == 0:
            print("All queries failed!")

        else:
            mean = np.mean(times)
            std  = np.std(times)
            tp = 1 / mean * self.numthreads

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
            print(f"Total inserted elements: {suceeded_queries}")
            print(f"Total successful commands: {suceeded_commands}")
        print("=================================================")
