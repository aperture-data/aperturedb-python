import math
import time
from threading import Thread

from aperturedb import ProgressBar

import numpy as np

class ParallelLoader:

    """
        Parallel and Batch Loader for ApertureDB
    """

    def __init__(self, db, dry_run=False):

        self.db = db
        self.dry_run = dry_run

        self.type = "element"

        # Default Values
        self.batchsize  = 1
        self.numthreads = 1
        self.n_retries  = 1

        self.total_elements = 0
        self.times_arr = []
        self.ingestion_time = 0
        self.error_counter  = 0

        self.loader_filename = "loader_progress.log"

    def do_batch(self, db, data):

        q, blobs = self.generate_batch(data)

        if not self.dry_run:
            r,b = db.query(q, blobs, n_retries=self.n_retries)
            if not db.last_query_ok():
                self.error_counter += 1
            query_time = db.get_last_query_time()
        else:
            query_time = 1

        # append is thread-safe
        self.times_arr.append(query_time)

    def worker(self, thid, generator, start, end):

        db = self.db.create_new_connection()

        if thid == 0 and self.stats:
            pb = ProgressBar.ProgressBar(self.loader_filename)

        total_batches = (end - start) // self.batchsize

        if (end - start) % self.batchsize > 0:
            total_batches += 1

        for i in range(total_batches):

            batch_start = start + i * self.batchsize
            batch_end   = min(batch_start + self.batchsize, end)

            try:
                # This can be done with slices instead of list comprehension.
                # but required support from generator.
                data_for_query = [ generator[idx]
                                   for idx in range(batch_start, batch_end) ]
                self.do_batch(db, data_for_query)
            except:
                self.error_counter += 1

            if thid == 0 and self.stats:
                pb.update((i + 1) / total_batches)

    def ingest(self, generator, batchsize=1, numthreads=1, stats=False):

        self.times_arr  = []
        self.batchsize  = batchsize
        self.numthreads = numthreads
        self.stats      = stats
        self.total_elements = len(generator)

        start_time = time.time()

        elements_per_thread = math.ceil(self.total_elements / self.numthreads)

        thread_arr = []
        for i in range(self.numthreads):
            idx_start = i * elements_per_thread
            idx_end   = min(idx_start + elements_per_thread, self.total_elements)

            thread_add = Thread(target=self.worker,
                                args=(i, generator, idx_start, idx_end))
            thread_arr.append(thread_add)


        a = [ th.start() for th in thread_arr]
        a = [ th.join()  for th in thread_arr]

        self.ingestion_time = time.time() - start_time

        if self.stats:
            self.print_stats()

    def print_stats(self):

        times = np.array(self.times_arr)
        total_queries_exec = len(times)
        inserted_elements  = self.total_elements

        print("============ ApertureDB Loader Stats ============")
        print("Total time (s):", self.ingestion_time)
        print("Total queries executed:", total_queries_exec)
        print("Avg Query time (s):", np.mean(times))
        print("Query time std:", np.std (times))
        print("Avg Query Throughput (q/s)):",
                1 / np.mean(times) * self.numthreads)

        msg = "(" + self.type + "/s):"
        print("Overall insertion throughput", msg,
                self.total_elements / self.ingestion_time)

        if self.error_counter > 0:
            print("Total errors encountered:", self.error_counter)
            inserted_elements -= self.error_counter * self.batchsize
            print("Errors (%):", 100 * self.error_counter / total_queries_exec)

        print("Total inserted elements:", inserted_elements)
        print("=================================================")
