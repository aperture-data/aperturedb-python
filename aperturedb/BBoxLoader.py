import math
import time
from threading import Thread

import numpy  as np
import pandas as pd

from aperturedb import Status
from aperturedb import ParallelLoader

HEADER_X_POS  = "x_pos"
HEADER_Y_POS  = "y_pos"
HEADER_WIDTH  = "width"
HEADER_HEIGHT = "height"
IMG_KEY_PROP  = "img_key_prop"
IMG_KEY_VAL   = "img_key_value"
BBOX_PROPERTIES  = "properties"

class BBoxGeneratorCSV():

    '''
        ApertureDB BBox Data loader.
        Expects a csv file with the following columns:

            IMG_KEY,x_pos,y_pos,width,height,BBOX_PROP_NAME_1, ... BBOX_PROP_NAME_N

        IMG_KEY column has the property name of the image property that
        the bounding box will be connected to, and each row has the value
        that will be used for finding the image.

        x_pos,y_pos,width,height are the coordinates of the bounding boxes,
        as integers (unit is in pixels)

        BBOX_PROP_NAME_N is an arbitrary name of the property of the bounding
        box, and each row has the value for that property.

        Example csv file:
        img_unique_id,x_pos,y_pos,width,height,type
        d5b25253-9c1e,257,154,84,125,manual
        d5b25253-9c1e,7,537,522,282,manual
        ...
    '''

    def __init__(self, filename):

        self.df = pd.read_csv(filename)
        print(self.df)

        self.validate()

        self.bbox_props_keys = [x for x in self.header[5:] if not x.startswith("connect_") ]

        self.img_key = self.header[0]

    def __len__(self):

        return len(self.df.index)

    def __getitem__(self, idx):

        data = {}

        data["x"] = int(self.df.loc[idx, HEADER_X_POS])
        data["y"] = int(self.df.loc[idx, HEADER_Y_POS])
        data["w"] = int(self.df.loc[idx, HEADER_WIDTH])
        data["h"] = int(self.df.loc[idx, HEADER_HEIGHT])

        val = self.df.loc[idx, self.img_key]

        if val != "":
            data[IMG_KEY_PROP] = self.img_key
            data[IMG_KEY_VAL]  = val

        if len(self.bbox_props_keys) > 0:
            properties = {}
            for key in self.bbox_props_keys:
                properties[key] = self.df.loc[idx, key]
            data[BBOX_PROPERTIES] = properties

        # TODO: Implement connection to arbitraty objects

        return data

    def validate(self):

        self.header = list(self.df.columns.values)
        print(self.header)

        if self.header[1] != HEADER_X_POS:
            print(self.header[1])
            raise Exception("Error with CSV file field: " + HEADER_X_POS)
        if self.header[2] != HEADER_Y_POS:
            raise Exception("Error with CSV file field: " + HEADER_Y_POS)
        if self.header[3] != HEADER_WIDTH:
            raise Exception("Error with CSV file field: " + HEADER_WIDTH)
        if self.header[4] != HEADER_HEIGHT:
            raise Exception("Error with CSV file field: " + HEADER_HEIGHT)

    def load(self, batchsize=1, numthreads=1, stats=False):

        super().ingest(self, batchsize, numthreads, stats)

class BBoxLoader(ParallelLoader.ParallelLoader):

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

    def generate_batch(self, bbox_data):

        q = []

        for data in bbox_data:

            fi = {
                "FindImage": {
                }
            }

            if IMG_KEY_PROP in data:
                key = data[IMG_KEY_PROP]
                val = data[IMG_KEY_VAL]
                constraints = {}
                constraints[key] = ["==", val]
                fi["FindImage"]["constraints"] = constraints

            q.append(fi)

            ai = {
                "AddBoundingBox": {
                    "rectangle": {
                        "x": data["x"],
                        "y": data["y"],
                        "w": data["w"],
                        "h": data["h"],
                    }
                }
            }

            if "properties" in data:
                ai["AddBoundingBox"]["properties"] = data[BBOX_PROPERTIES]

            # TODO Add connections to arbitrary objects.

            q.append(ai)

            if self.dry_run:
                print(q)

        return q, []

    def print_stats(self):

        print("====== ApertureDB BoundingBox Loader Stats ======")

        times = np.array(self.times_arr)
        print("Avg Query time(s):", np.mean(times))
        print("Query time std:", np.std (times))
        print("Avg Query Throughput (bbox/s)):",
            1 / np.mean(times) * self.batchsize * self.numthreads)

        print("Total time(s):", self.ingestion_time)
        print("===========================================")
