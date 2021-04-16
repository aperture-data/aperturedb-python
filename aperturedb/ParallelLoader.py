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
        self.batchsize  = 1
        self.numthreads = 1

        self.times_arr = []
        self.dry_run = dry_run
        self.ingestion_time = 0

    def insert_batch(self, db, data):

        q, blobs = self.generate_batch(data)

        if not self.dry_run:
            r,b = db.query(q, blobs, n_retries=10)
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

        elements_per_thread = math.ceil(len(generator) / self.numthreads)

        thread_arr = []
        for i in range(self.numthreads):
            idx_start = i * elements_per_thread
            idx_end   = min(idx_start + elements_per_thread, len(generator))

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
            len(generator) / self.ingestion_time)
        print("===========================================")
