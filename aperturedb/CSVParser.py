import pandas as pd
import logging
from aperturedb.Subscriptable import Subscriptable
from dask import dataframe
import os
import multiprocessing as mp


logger = logging.getLogger(__name__)

ENTITY_CLASS      = "EntityClass"
CONSTRAINTS_PREFIX = "constraint_"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"

# This number is based on the partitions one wants to use per core.
PARTITIONS_PER_CORE = 10

# Use 90% os the CPU cores by default.
CORES_USED_FOR_PARALLELIZATION = 0.9


class CSVParser(Subscriptable):
    """
    **ApertureDB General CSV Parser for Loaders.**
    This operates in 2 modes:
    - **Normal Mode**: This is the default mode. It reads the CSV file into a Pandas DataFrame.
    - **Dask Mode**: This mode is used when the CSV file is too big to fit in memory, or multiprocessing is desired.
        It reads the CSV file into a Dask DataFrame.
        In Dask mode the CSV file is read in chunks, and the operations are performed on each chunk.
        The tricky bit is that the chunck size is not known till the loader is created, so the processing happens when ingest is called.
        So the Data CSV has another signature, where the df is passed explicitly.
    """

    def __init__(self, filename, df=None, use_dask=False):
        self.use_dask = use_dask
        self.filename = filename

        if not use_dask:
            if df is None:
                self.df = pd.read_csv(filename)
            else:
                self.df = df
        else:
            # It'll impact the number of partitions, and memory usage.
            # TODO: tune this for the best performance.
            cores_used = int(CORES_USED_FOR_PARALLELIZATION * mp.cpu_count())
            self.df = dataframe.read_csv(
                self.filename,
                blocksize = os.path.getsize(self.filename) // (cores_used * PARTITIONS_PER_CORE))

        if len(self.df) == 0:
            logger.error("Dataframe empty. Is the CSV file ok?")

        self.df = self.df.astype('object')
        self.header = list(self.df.columns.values)
        self.validate()

    def __len__(self):

        return len(self.df.index)

    def parse_properties(self, df, idx):

        properties = {}
        if len(self.props_keys) > 0:
            for key in self.props_keys:
                # Handle Date data type
                if key.startswith("date:"):
                    prop = key[len("date:"):]  # remove prefix
                    properties[prop] = {"_date": self.df.loc[idx, key]}
                else:
                    value = self.df.loc[idx, key]
                    if value == value:  # skips nan values
                        properties[key] = value

        return properties

    def parse_constraints(self, df, idx):

        constraints = {}
        if len(self.constraints_keys) > 0:
            for key in self.constraints_keys:
                if key.startswith("constraint_date:"):
                    prop = key[len("constraint_date:"):]  # remove prefix
                    constraints[prop] = [
                        "==", {"_date": self.df.loc[idx, key]}]
                else:
                    prop = key[len(CONSTRAINTS_PREFIX):]  # remove "prefix
                    constraints[prop] = ["==", self.df.loc[idx, key]]

        return constraints

    def _basic_command(self, idx, custom_fields: dict = None):
        if custom_fields == None:
            custom_fields = {}
        properties = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)
        query = {
            self.command: custom_fields
        }
        if properties:
            query[self.command][PROPERTIES] = properties

        if constraints:
            query[self.command]["if_not_found"] = constraints

        return query

    def validate(self):

        Exception("Validation not implemented!")
