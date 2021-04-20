import math
import time
from threading import Thread

import numpy as np

from aperturedb import Status
from aperturedb import ParallelLoader

class ImageLoader(ParallelLoader.ParallelLoader):

    '''
        ApertureDB Image Loader.

        This class is to be used in combination with a "generator".
        The generator must be an iterable object that generated "image_data"
        elements:
            image_data = {
                "properties":  properties,
                "constraints": constraints,
                "operations":  operations,
                "format":      format ("jpg", "png", etc),
                "img_blob":    (bytes),
            }
    '''

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

    def generate_batch(self, image_data):

        q = []
        blobs = []

        for data in image_data:

            ai = {
                "AddImage": {
                }
            }

            if "properties" in data:
                ai["AddImage"]["properties"] = data["properties"]
            if "constraints" in data:
                ai["AddImage"]["constraints"] = data["constraints"]
            if "operations" in data:
                ai["AddImage"]["operations"] = data["operations"]
            if "format" in data:
                ai["AddImage"]["format"] = data["format"]

            if len(data["img_blob"]) == 0:
                raise Exception("Image cannot be empty")

            blobs.append(data["img_blob"])
            q.append(ai)

        return q, blobs

    def print_stats(self):

        status = Status.Status(self.db)
        print("====== ApertureDB Image Loader Stats ======")
        print("Images in the db:", status.count_images())

        times = np.array(self.times_arr)
        print("Avg Query time(s):", np.mean(times))
        print("Query time std:", np.std (times))
        print("Avg Query Throughput (images/s)):",
            1 / np.mean(times) * self.batchsize * self.numthreads)

        print("Total time(s):", self.ingestion_time)
        print("Overall insertion throughput (img/s):",
            len(generator) / self.ingestion_time)
        print("===========================================")
