import math
import time
from threading import Thread

import numpy  as np
import pandas as pd

from aperturedb import Status
from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_X_POS  = "x_pos"
HEADER_Y_POS  = "y_pos"
HEADER_WIDTH  = "width"
HEADER_HEIGHT = "height"
IMG_KEY_PROP  = "img_key_prop"
IMG_KEY_VAL   = "img_key_value"

class BBoxGeneratorCSV(CSVParser.CSVParser):

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

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[5:] if not x.startswith(CSVParser.CONTRAINTS_PREFIX) ]
        self.constraints_keys = [x for x in self.header[5:] if x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

        self.img_key = self.header[0]

    def __getitem__(self, idx):

        data = {
            "x":      int(self.df.loc[idx, HEADER_X_POS]),
            "y":      int(self.df.loc[idx, HEADER_Y_POS]),
            "width":  int(self.df.loc[idx, HEADER_WIDTH]),
            "height": int(self.df.loc[idx, HEADER_HEIGHT]),
        }

        val = self.df.loc[idx, self.img_key]

        if val != "":
            data[IMG_KEY_PROP] = self.img_key
            data[IMG_KEY_VAL]  = val

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[CSVParser.PROPERTIES] = properties

        if constraints:
            data[CSVParser.CONSTRAINTS] = constraints

        return data

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[1] != HEADER_X_POS:
            raise Exception("Error with CSV file field: " + HEADER_X_POS)
        if self.header[2] != HEADER_Y_POS:
            raise Exception("Error with CSV file field: " + HEADER_Y_POS)
        if self.header[3] != HEADER_WIDTH:
            raise Exception("Error with CSV file field: " + HEADER_WIDTH)
        if self.header[4] != HEADER_HEIGHT:
            raise Exception("Error with CSV file field: " + HEADER_HEIGHT)

class BBoxLoader(ParallelLoader.ParallelLoader):

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "bbox"

    def generate_batch(self, bbox_data):

        q = []

        ref_counter = 1
        for data in bbox_data:

            # TODO we could reuse image references within the batch
            # instead of creating a new find for every image.
            img_ref = ref_counter
            ref_counter += 1
            fi = {
                "FindImage": {
                    "_ref": img_ref,
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
                    "image": img_ref,
                    "rectangle": {
                        "x":      data["x"],
                        "y":      data["y"],
                        "width":  data["width"],
                        "height": data["height"],
                    },
                }
            }

            if "properties" in data:
                ai["AddBoundingBox"]["properties"] = data[CSVParser.PROPERTIES]

            q.append(ai)

        if self.dry_run:
            print(q)

        return q, []
