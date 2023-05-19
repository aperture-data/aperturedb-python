import pandas as pd
import logging
from aperturedb.Subscriptable import Subscriptable
from dask import dataframe
import os
import multiprocessing as mp


logger = logging.getLogger(__name__)

ENTITY_CLASS      = "EntityClass"
CONSTRAINTS_PREFIX = "constraint_"
DATE_PREFIX = "date:"
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

        # len for dask dataframe needs a client.
        if not use_dask and len(self.df) == 0:
            logger.error("Dataframe empty. Is the CSV file ok?")

        self.df = self.df.astype('object')
        self.header = list(self.df.columns.values)
        self.validate()

    def __len__(self):
        return len(self.df.index)

    def get_indexed_properties(self):
        if self.constraints_keys:
            return {self._parse_prop(k)[0] for k in self.constraints_keys}
        return set()

    def get_indices(self):
        raise NotImplementedError

    def _parse_prop(self, key, val=None):
        if key.startswith(CONSTRAINTS_PREFIX):
            key = key[len(CONSTRAINTS_PREFIX):]
        if key.startswith(DATE_PREFIX):
            key = key[len(DATE_PREFIX):]
            val = {"_date": val}
        return key, val

    def parse_properties(self, idx):

        properties = {}
        if self.props_keys:
            for key in self.props_keys:
                prop, value = self._parse_prop(key, self.df.loc[idx, key])
                if value == value:  # skips nan values
                    properties[prop] = value

        return properties

    def parse_constraints(self, idx):

        constraints = {}
        if self.constraints_keys:
            for key in self.constraints_keys:
                prop, value = self._parse_prop(key, self.df.loc[idx, key])
                constraints[prop] = ["==", value]

        return constraints

    def _basic_command(self, idx, custom_fields: dict = None):
        if custom_fields == None:
            custom_fields = {}
        properties = self.parse_properties(idx)
        constraints = self.parse_constraints(idx)
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
