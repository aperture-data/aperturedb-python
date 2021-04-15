import math
import time
from threading import Thread

import numpy as np

from aperturedb import Status

# TODO: This can be a general parallel/batch loader.
# Right now it is for images, but it can be easily generalized.
class ImageLoader:

    def __init__(self, db):

        self.db = db
        self.batchsize  = 1
        self.numthreads = 1

        self.times_arr = []
        self.dry_run = False
        self.ingestion_time = 0

    def insert_batch(self, db, image_data):

        q = []
        blobs = []

        for data in image_data:

            ai = {
                "AddImage": {
                }
            }

            if "properties" in data:
                ai["AddImage"]["properties"] = data["properties"]
            if "format" in data:
                ai["AddImage"]["format"] = data["format"]
            if "constraints" in data:
                ai["AddImage"]["constraints"] = data["constraints"]
            if "operations" in data:
                ai["AddImage"]["operations"] = data["operations"]

            if len(data["img_blob"]) == 0:
                raise Exception("Image cannot be empty")

            blobs.append(data["img_blob"])
            q.append(ai)

        if not self.dry_run:
            r,b = db.query(q, blobs, n_retries=10)
            query_time = db.get_last_query_time()
        else:
            query_time = 1

        # append is thread-safe
        self.times_arr.append(query_time)

    def insert_worker(self, generator, start, end):

        db = self.db.create_new_connection()

        image_data = []

        for i in range(start, end):

            image_data.append(generator[i])

            if len(image_data) >= self.batchsize:
                self.insert_batch(db, image_data)
                image_data = []

        if len(image_data) > 0:
            self.insert_batch(db, image_data)
            image_data = []

    def ingest_images(self, generator, batchsize=1, numthreads=1, stats=False, dry_run=False):

        self.times_arr  = []
        self.batchsize  = batchsize
        self.numthreads = numthreads
        self.dry_run    = dry_run

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

        status = Status.Status(self.db)
        print("====== ApertureDB Image Loader Stats ======")
        print("Images in the db:", status.count_images())

        times = np.array(self.times_arr)
        print("Avg Query time(s):", np.mean(times))
        print("Query time std:", np.std (times))
        print("Avg Query Throughput (images/s)):",
            1 / np.mean(times) * self.batchsize * self.numthreads)

        print("Total time(s):", ingestion_time)
        print("Overall insertion throughput (img/s):",
            len(generator) / ingestion_time)
        print("===========================================")
