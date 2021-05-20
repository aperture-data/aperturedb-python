import math
import time
from threading import Thread

import numpy  as np
import pandas as pd

from aperturedb import Status
from aperturedb import ParallelLoader
from aperturedb import CSVParser

ENTITY_CLASS = "EntityClass"
PROPERTIES   = "properties"
CONSTRAINTS  = "constraints"
BLOB_PATH    = "blob_filename"

class EntityGeneratorCSV(CSVParser.CSVParser):

    '''
        ApertureDB Entity Data loader.
        Expects a csv file with the following columns (filename optional):

            EntityClass,PROP_NAME_1, ... PROP_NAME_N,constraint_PROP1,filename

        Example csv file:
        EntityClass,name,lastname,age,id,constaint_id
        Person,John,Salchi,69,321423532,321423532
        Person,Johna,Salchi,63,42342522,42342522
        ...
    '''

    def __init__(self, filename):

        super().__init__(filename)

        self.blob_present     = BLOB_PATH in self.header
        self.props_keys       = [x for x in self.header[1:] if not x.startswith(CSVParser.CONTRAINTS_PREFIX)]
        self.props_keys       = [x for x in self.props_keys if x != BLOB_PATH]
        self.constraints_keys = [x for x in self.header[1:] if x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

    def __getitem__(self, idx):

        data = {}
        data["class"] = self.df.loc[idx, ENTITY_CLASS]
        if self.blob_present:
            filename   = self.df.loc[idx, BLOB_PATH]
            blob_ok, blob = self.load_blob(filename)
            if not blob_ok:
                Exception("Error loading blob: " + filename )
            data["blob"] = blob

        properties  = self.parse_properties(self.df, idx)
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

        if self.header[0] != ENTITY_CLASS:
            raise Exception("Error with CSV file field: " + ENTITY_CLASS)

class EntityLoader(ParallelLoader.ParallelLoader):

    '''
        ApertureDB Entity Loader.

        This class is to be used in combination with a "generator".
        The generator must be an iterable object that generated "entity_data"
        elements:
            entity_data = {
                "class":       entity_class,
                "properties":  properties,
                "constraints": constraints,
                "blob": blob , optional
            }
    '''

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "entity"

    def generate_batch(self, entity_data):

        q = []
        blobs = []

        for data in entity_data:

            ae = {
                "AddEntity": {
                    "class": data["class"]
                }
            }

            if PROPERTIES in data:
                ae["AddEntity"][PROPERTIES] = data[PROPERTIES]

            if CONSTRAINTS in data:
                ae["AddEntity"][CONSTRAINTS] = data[CONSTRAINTS]

            if "blob" in data:
                if len(data["blob"]) == 0:
                    print("WARNING: Skipping empty blob.")
                    continue
                ae["AddEntity"]["blob"] = True
                blobs.append(data["blob"])

            q.append(ae)

        if self.dry_run:
            print(q)

        return q, blobs
