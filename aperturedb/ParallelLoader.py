import math
import time
from threading import Thread

import numpy as np

class ParallelLoader:

    """
        Parallel and Batch Loader for ApertureDB
    """

    def __init__(self, db, dry_run=False):

        self.db = db
        self.dry_run = dry_run

        # Default Values
        self.batchsize  = 1
        self.numthreads = 1
        self.n_retries = 10

        self.total_elements = 0
        self.times_arr = []
        self.ingestion_time = 0
        self.error_counter  = 0

    def insert_batch(self, db, data):

        q, blobs = self.generate_batch(data)

        if not self.dry_run:
            r,b = db.query(q, blobs, n_retries=self.n_retries)
            if not db.last_query_ok():
                self.error_counter +=1
            query_time = db.get_last_query_time()
        else:
            query_time = 1

        # append is thread-safe
        self.times_arr.append(query_time)

    def insert_worker(self, generator, start, end):

        db = self.db.create_new_connection()

        data_for_query = []

        for i in range(start, end):

            data_for_query.append(generator[i])

            if len(data_for_query) >= self.batchsize:
                self.insert_batch(db, data_for_query)
                data_for_query = []

        if len(data_for_query) > 0:
            self.insert_batch(db, data_for_query)
            data_for_query = []

    def ingest(self, generator, batchsize=1, numthreads=1, stats=False):

        self.times_arr  = []
        self.batchsize  = batchsize
        self.numthreads = numthreads
        self.total_elements = len(generator)

        elements_per_thread = math.ceil(self.total_elements / self.numthreads)

        thread_arr = []
        for i in range(self.numthreads):
            idx_start = i * elements_per_thread
            idx_end   = min(idx_start + elements_per_thread, self.total_elements)

            thread_add = Thread(target=self.insert_worker,
                                args=(generator, idx_start, idx_end))
            thread_arr.append(thread_add)

        start_time = time.time()
        for thread in thread_arr:
            thread.start()

        for thread in thread_arr:
            thread.join()

        self.ingestion_time = time.time() - start_time

        if stats:
            self.print_stats()

    def print_stats(self):

        print("====== ApertureDB Loader Stats ======")
        times = np.array(self.times_arr)
        print("Avg Query time(s):", np.mean(times))
        print("Query time std:", np.std (times))
        print("Avg Query Throughput (q/s)):",
            1 / np.mean(times) * self.numthreads)

        print("Total time(s):", self.ingestion_time)
        print("Overall insertion throughput (elements/s):",
            self.total_elements / self.ingestion_time)
        print("Total errors encountered(s):", self.error_counter)
        print("===========================================")
