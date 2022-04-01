import math
import time
from threading import Thread

from aperturedb import ProgressBar

import numpy as np

class ParallelQuery:

    """
        Parallel and Batch Querier for ApertureDB
    """

    def __init__(self, db, dry_run=False):

        # db can me None in the case of Downloaders
        # or other abstractions that want to use the parallel/batching
        # structures without running queries
        if db:
            self.db = db.create_new_connection()

        self.dry_run = dry_run

        self.type = "query"

        # Default Values
        self.batchsize  = 1
        self.numthreads = 1

        self.total_queries = 0
        self.times_arr = []
        self.queries_time = 0
        self.error_counter  = 0

        self.loader_filename = "progress.log"

    def  get_times(self):

        return self.times_arr

    def generate_batch(self, data):

        """
            Here we flatten the individual queries to run them as
            a single query in a batch
        """
        q     = [cmd  for query in data for cmd  in query[0]]
        blobs = [blob for query in data for blob in query[1]]

        return q, blobs

    def do_batch(self, db, data):

        q, blobs = self.generate_batch(data)

        if not self.dry_run:
            r,b = db.query(q, blobs)
            if not db.last_query_ok():
                self.error_counter += 1
            query_time = db.get_last_query_time()
        else:
            query_time = 1

        # append is thread-safe
        self.times_arr.append(query_time)

    def worker(self, thid, generator, start, end):

        # A new connection will be created for each thread
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
            except Exception as e:
                print(e)
                self.error_counter += 1

            if thid == 0 and self.stats:
                pb.update((i + 1) / total_batches)

    def query(self, generator, batchsize=1, numthreads=4, stats=False):

        self.times_arr  = []
        self.batchsize  = batchsize
        self.numthreads = numthreads
        self.stats      = stats
        self.total_queries = len(generator)

        start_time = time.time()

        if self.total_queries < batchsize:
            elements_per_thread = self.total_queries
            self.numthreads = 1
        else:
            elements_per_thread = math.ceil(self.total_queries / self.numthreads)

        thread_arr = []
        for i in range(self.numthreads):
            idx_start = i * elements_per_thread
            idx_end   = min(idx_start + elements_per_thread, self.total_queries)

            thread_add = Thread(target=self.worker,
                                args=(i, generator, idx_start, idx_end))
            thread_arr.append(thread_add)


        a = [ th.start() for th in thread_arr]
        a = [ th.join()  for th in thread_arr]

        self.queries_time = time.time() - start_time

        if self.stats:
            self.print_stats()

    def print_stats(self):

        times = np.array(self.times_arr)
        total_queries_exec = len(times)

        print("============ ApertureDB Parallel Query Stats ============")
        print("Total time (s):", self.queries_time)
        print("Total queries executed:", total_queries_exec)

        if total_queries_exec == 0:
            print("All queries failed!")

        else:
            print("Avg Query time (s):", np.mean(times))
            print("Query time std:", np.std (times))
            print("Avg Query Throughput (q/s)):",
                    1 / np.mean(times) * self.numthreads)

            msg = "(" + self.type + "/s):"
            print("Overall query throughput", msg,
                    self.total_queries / self.queries_time)

            if self.error_counter > 0:
                print("Total errors encountered:", self.error_counter)
                print("Errors (%):", 100 * self.error_counter / total_queries_exec)

        print("=========================================================")
