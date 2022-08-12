import math
import time
from threading import Thread

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

    def ingest(self, generator, batchsize=1, numthreads=4, stats=False):
        logger.info(
            f"Starting ingestion with batchsize={batchsize}, numthreads={numthreads}")
        self.query(generator, batchsize, numthreads, stats)

    def print_stats(self):

        times = np.array(self.times_arr)
        total_queries_exec = len(times)
        inserted_elements  = self.total_actions

        print("============ ApertureDB Loader Stats ============")
        print("Total time (s):", self.total_actions_time)
        print("Total queries executed:", total_queries_exec)

        if total_queries_exec == 0:
            print("All queries failed!")

        else:
            print("Avg Query time (s):", np.mean(times))
            print("Query time std:", np.std(times))
            print("Avg Query Throughput (q/s)):",
                  1 / np.mean(times) * self.numthreads)

            msg = "(" + self.type + "/s):"
            print("Overall insertion throughput", msg,
                  self.total_actions / self.total_actions_time)

            if self.error_counter > 0:
                print("Total errors encountered:", self.error_counter)
                inserted_elements -= self.error_counter * self.batchsize
                print("Errors (%):", 100 *
                      self.error_counter / total_queries_exec)

            print("Total inserted elements:", inserted_elements)
        print("=================================================")
