import math
import time
from threading import Thread
import json
import sys
import os

import numpy  as np
import pandas as pd

from aperturedb import Status
from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_POLYGONS = "polygons"
IMG_KEY_PROP    = "img_key_prop"
IMG_KEY_VAL     = "img_key_value"
INTERNAL_PROPS  = {
    "_label": "label",
}

class PolygonGeneratorCSV(CSVParser.CSVParser):

    '''
        ApertureDB Polygon Data loader.
        Expects a csv file with the following columns:

            IMG_KEY,POLYGON_PROP_NAME_1, ... POLYGON_PROP_NAME_N,polygons

        IMG_KEY column has the property name of the image property that
        the bounding box will be connected to, and each row has the value
        that will be used for finding the image.

        POLYGON_PROP_NAME_I is an arbitrary name of the property of the polygon,
        and each row has a value for that property.

        The "polygons" field contains an array of polygon regions. Each polygon region is an array of (x,y) vertices that describe the boundary of a single contiguous polygon. See also https://docs.aperturedata.io/parameters/polygons.html.

        Example csv file:
        image_id,id,category_id,_label,polygons
        397133,82445,44,bottle,"[[(224.24, 297.18), (228.29, 297.18), ...]]"
        397133,119568,67,dining table,"[[(292.37, 425.1), (340.6, 373.86), ...]]"
        ...
    '''

    def __init__(self, filename):

        super().__init__(filename)

        polygon_prop_keys = self.header[1:-1]
        self.props_keys       = [x for x in polygon_prop_keys
            if not x.startswith(CSVParser.CONTRAINTS_PREFIX)
                and x not in INTERNAL_PROPS.keys()]
        self.constraints_keys = [x for x in polygon_prop_keys
            if x.startswith(CSVParser.CONTRAINTS_PREFIX)
                and x not in INTERNAL_PROPS.keys()]
        self.internal_keys    = [x for x in polygon_prop_keys
            if x in INTERNAL_PROPS.keys()]

        self.img_key = self.header[0]

    def __getitem__(self, idx):

        data = {
            "polygons": json.loads(self.df.loc[idx, HEADER_POLYGONS])
        }

        data[IMG_KEY_PROP] = self.img_key
        data[IMG_KEY_VAL]  = self.df.loc[idx, self.img_key]

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)
        internal    = self.parse_internal(idx)

        if properties:
            data[CSVParser.PROPERTIES] = properties

        if constraints:
            data[CSVParser.CONSTRAINTS] = constraints

        if internal:
            data["internal"] = internal

        return data

    def parse_internal(self, idx):
        internal = {}
        if len(self.internal_keys) > 0:
            for key in self.internal_keys:
                internal[INTERNAL_PROPS[key]] = self.df.loc[idx, key]

        return internal

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[-1] != HEADER_POLYGONS:
            raise Exception("Error with CSV file field: " + HEADER_POLYGONS)

class PolygonLoader(ParallelLoader.ParallelLoader):

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "polygon"

    def generate_batch(self, polygon_data):

        q = []

        ref_counter = 1
        img_map = {}
        for data in polygon_data:

            img_id = data[IMG_KEY_VAL]
            if img_id not in img_map:
                ref_counter += 1
                img_map[img_id] = ref_counter
                fi = {
                    "FindImage": {
                        "_ref": ref_counter,
                    }
                }

                key = data[IMG_KEY_PROP]
                val = data[IMG_KEY_VAL]
                constraints = {}
                constraints[key] = ["==", val]
                fi["FindImage"]["constraints"] = constraints

                q.append(fi)

            ap = {
                "AddPolygon": {
                    "image_ref": img_map[img_id],
                    "polygons": data["polygons"],
                }
            }

            if CSVParser.PROPERTIES in data:
                ap["AddPolygon"]["properties"] = data[CSVParser.PROPERTIES]

            if "internal" in data:
                ap["AddPolygon"].update(data["internal"])

            q.append(ap)

        if self.dry_run:
            print(q)

        return q, []
