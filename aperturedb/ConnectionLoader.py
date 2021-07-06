import math
import time
from threading import Thread

import numpy  as np
import pandas as pd

from aperturedb import Status
from aperturedb import ParallelLoader
from aperturedb import CSVParser

CONNECTION_CLASS = "ConnectionClass"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"

class ConnectionGeneratorCSV(CSVParser.CSVParser):

    '''
        ApertureDB Connection Data loader.
        Expects a csv file with the following columns:

            ConnectionClass,Class1@PROP_NAME, Class2@PROP_NAME ... PROP_NAME_N,constraint_PROP1

        Example csv file:
        ConnectionClass,VD:IMG@id,Person@id,confidence,id,constaint_id
        has_image,321423532,42342522,0.4354,5432543254,5432543254
        has_image,321423532,53241521,0.6432,98476542,98476542
        ...
    '''

    def __init__(self, filename):

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[3:] if not x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

        self.constraints_keys = [x for x in self.header[3:] if x.startswith(CSVParser.CONTRAINTS_PREFIX) ]

        self.ref1_class   = self.header[1].split("@")[0]
        self.ref1_key     = self.header[1].split("@")[1]
        self.ref2_class   = self.header[2].split("@")[0]
        self.ref2_key     = self.header[2].split("@")[1]

    def __getitem__(self, idx):

        data = {
            "class":      self.df.loc[idx, CONNECTION_CLASS],
            "ref1_class": self.ref1_class,
            "ref1_key":   self.ref1_key,
            "ref1_val":   self.df.loc[idx, self.header[1]],
            "ref2_class": self.ref2_class,
            "ref2_key":   self.ref2_key,
            "ref2_val":   self.df.loc[idx, self.header[2]],
        }

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != CONNECTION_CLASS:
            raise Exception("Error with CSV file field: 0 - " + CONNECTION_CLASS)
        if "@" not in self.header[1]:
            raise Exception("Error with CSV file field: 1")
        if "@" not in self.header[2]:
            raise Exception("Error with CSV file field: 2")

class ConnectionLoader(ParallelLoader.ParallelLoader):

    '''
        ApertureDB Connection Loader.

        This class is to be used in combination with a "generator".
        The generator must be an iterable object that generated "entity_data"
        elements:
            entity_data = {
                "class":       connection_class,
                "properties":  properties,
                "constraints": constraints,
            }
    '''

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "connection"

    def generate_batch(self, entity_data):

        q = []

        ref_counter = 1

        for data in entity_data:

            ref_src = ref_counter
            ref_counter += 1
            fe_a = {
                "FindEntity": {
                    "_ref": ref_src,
                    "with_class": data["ref1_class"],
                }
            }

            fe_a["FindEntity"]["constraints"] = {}
            fe_a["FindEntity"]["constraints"][data["ref1_key"]] = ["==", data["ref1_val"]]
            q.append(fe_a)

            ref_dst = ref_counter
            ref_counter += 1
            fe_b = {
                "FindEntity": {
                    "_ref": ref_dst,
                    "with_class": data["ref2_class"],
                }
            }

            fe_b["FindEntity"]["constraints"] = {}
            fe_b["FindEntity"]["constraints"][data["ref2_key"]] = ["==", data["ref2_val"]]
            q.append(fe_b)

            ae = {
                "AddConnection": {
                    "class": data["class"],
                    "src": ref_src,
                    "dst": ref_dst,
                }
            }

            if PROPERTIES in data:
                ae["AddConnection"][PROPERTIES] = data[PROPERTIES]

            if CONSTRAINTS in data:
                ae["AddConnection"]["if_not_found"] = data[CONSTRAINTS]

            q.append(ae)

        if self.dry_run:
            print(q)

        return q, []
