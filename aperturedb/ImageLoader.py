import math
import time
from threading import Thread

import numpy as np
import cv2

from aperturedb import Status
from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_PATH = "filename"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"

class ImageGeneratorCSV(CSVParser.CSVParser):

    '''
        ApertureDB Image Data loader.
        Expects a csv file with the following columns:

            filename,PROP_NAME_1, ... PROP_NAME_N,constraint_PROP1

        Example csv file:
        filename,id,label,constaint_id
        /home/user/file1.jpg,321423532,dog,321423532
        /home/user/file2.jpg,42342522,cat,42342522
        ...
    '''

    def __init__(self, filename, check_image=True):

        super().__init__(filename)

        self.check_image = check_image

        self.props_keys       = [x for x in self.header[1:] if not x.startswith(CSVParser.CONTRAINTS_PREFIX) ]
        self.constraints_keys = [x for x in self.header[1:] if x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

    def __getitem__(self, idx):

        filename = self.df.loc[idx, HEADER_PATH]
        img_ok, img = self.load_image(filename)

        if not img_ok:
            Exception("Error loading image: " + filename )

        data = {
            "img_blob": img,
            "format": "jpg"
        }

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

    def load_image(self, filename):

        if self.check_image:
            try:
                a = cv2.imread(filename)
                if a.size <= 0:
                    print("IMAGE SIZE ERROR:", filename)
            except:
                print("IMAGE ERROR:", filename)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except:
            print("IMAGE ERROR:", filename)

        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != HEADER_PATH:
            raise Exception("Error with CSV file field: filename. Must be first field")

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

        self.type = "image"

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

            if "img_blob" not in data or len(data["img_blob"]) == 0:
                print("WARNING: Skipping empty image.")
                continue

            blobs.append(data["img_blob"])
            q.append(ai)

        return q, blobs

    def print_stats(self):

        # This is to make sure that the connect has not timed-out
        # after the loaded completed.
        self.db = self.db.create_new_connection()

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
            self.total_elements / self.ingestion_time)
        print("Total errors encountered:", self.error_counter)
        print("Elements added:",
            self.total_elements - self.error_counter * self.batchsize)
        print("===========================================")
