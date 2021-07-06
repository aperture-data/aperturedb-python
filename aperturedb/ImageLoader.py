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
IMG_FORMAT  = "format"

class ImageGeneratorCSV(CSVParser.CSVParser):

    '''
        ApertureDB Image Data loader.
        Expects a csv file with the following columns (format optional):

            filename,PROP_NAME_1, ... PROP_NAME_N,constraint_PROP1,format

        Example csv file:
        filename,id,label,constaint_id,format
        /home/user/file1.jpg,321423532,dog,321423532,jpg
        /home/user/file2.jpg,42342522,cat,42342522,png
        ...
    '''

    def __init__(self, filename, check_image=True):

        super().__init__(filename)

        self.check_image = check_image

        self.format_given     = IMG_FORMAT in self.header
        self.props_keys       = [x for x in self.header[1:] if not x.startswith(CSVParser.CONTRAINTS_PREFIX)]
        self.props_keys       = [x for x in self.props_keys if x != IMG_FORMAT]
        self.constraints_keys = [x for x in self.header[1:] if x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

    def __getitem__(self, idx):

        filename   = self.df.loc[idx, HEADER_PATH]
        data = {}

        img_ok, img = self.load_image(filename)
        if not img_ok:
            Exception("Error loading image: " + filename )

        data["img_blob"] = img
        if self.format_given:
            data["format"] = self.df.loc[idx, IMG_FORMAT]

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
                ai["AddImage"]["if_not_found"] = data["constraints"]
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
