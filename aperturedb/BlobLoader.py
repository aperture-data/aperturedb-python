import math
import time
from threading import Thread

import numpy  as np
import pandas as pd

from aperturedb import Status
from aperturedb import ParallelLoader
from aperturedb import CSVParser

PROPERTIES  = "properties"
CONSTRAINTS = "constraints"
BLOB_PATH   = "filename"

class BlobGeneratorCSV(CSVParser.CSVParser):

    '''
        ApertureDB Blob Data loader.
        Expects a csv file with the following columns:

            filename,PROP_NAME_1, ... PROP_NAME_N,constraint_PROP1

        Example csv file:
        filename,name,lastname,age,id,constaint_id
        /mnt/blob1,John,Salchi,69,321423532,321423532
        /mnt/blob2,Johna,Salchi,63,42342522,42342522
        ...
    '''

    def __init__(self, filename):

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[1:] if not x.startswith(CSVParser.CONTRAINTS_PREFIX)]
        self.props_keys       = [x for x in self.props_keys if x != BLOB_PATH]
        self.constraints_keys = [x for x in self.header[1:] if x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

    def __getitem__(self, idx):

        data = {}

        filename      = self.df.loc[idx, BLOB_PATH]
        blob_ok, blob = self.load_blob(filename)
        if not blob_ok:
            Exception("Error loading blob: " + filename )
        data["blob"] = blob

        properties  = self.parse_properties (self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

    def load_blob(self, filename):

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except:
            print("BLOB ERROR:", filename)

        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != BLOB_PATH:
            raise Exception("Error with CSV file field: " + BLOB_PATH)

class BlobLoader(ParallelLoader.ParallelLoader):

    '''
        ApertureDB Blob Loader.

        This class is to be used in combination with a "generator".
        The generator must be an iterable object that generated "Blob_data"
        elements:
            Blob_data = {
                "properties":  properties,
                "constraints": constraints,
            }
    '''

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "blob"

    def generate_batch(self, Blob_data):

        q = []
        blobs = []

        for data in Blob_data:

            ae = {
                "AddBlob": {
                }
            }

            if PROPERTIES in data:
                ae["AddBlob"][PROPERTIES] = data[PROPERTIES]

            if CONSTRAINTS in data:
                ae["AddBlob"]["if_not_found"] = data[CONSTRAINTS]

            q.append(ae)

            if len(data["blob"]) == 0:
                print("WARNING: Skipping empty blob.")
                continue
            blobs.append(data["blob"])

        if self.dry_run:
            print(q)

        return q, blobs
