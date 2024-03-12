from typing import Set
import pandas as pd
import logging
from aperturedb.Subscriptable import Subscriptable
from dask import dataframe
import os
import multiprocessing as mp
import re


logger = logging.getLogger(__name__)

ENTITY_CLASS = "EntityClass"
CONSTRAINTS_PREFIX = "constraint_"
DATE_PREFIX = "date:"
PROPERTIES = "properties"
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
        The tricky bit is that the chunk size is not known till the loader is created, so the processing happens when ingest is called.
        So the Data CSV has another signature, where the df is passed explicitly.

    Typically, the response_handler is application specific, and loading does not break
    on errors in response_handlers, so the default behavior is to log the error and continue.
    If you want to break on errors, set strict_response_validation to True.
    """

    def __init__(self,
                 filename: str,
                 strict_response_validation: bool = False,
                 **kwargs):

        self.filename = filename
        self.strict_response_validation = strict_response_validation
        self.constraint_keyword = "if_not_found"

        # The following are extracted from the kwargs.
        self.blobs_relative_to_csv = "blobs_relative_to_csv" in kwargs and kwargs[
            "blobs_relative_to_csv"]
        self.use_dask = "use_dask" in kwargs and kwargs["use_dask"]
        df = kwargs["df"] if "df" in kwargs else None

        self.relative_path_prefix = os.path.dirname(self.filename) if self.blobs_relative_to_csv \
            else ""

        if not self.use_dask:
            if df is None:
                self.df = pd.read_csv(filename)
            else:
                self.df = df
        else:
            # It'll impact the number of partitions, and memory usage.
            # TODO: tune this for the best performance.
            cores_used = int(CORES_USED_FOR_PARALLELIZATION * mp.cpu_count())
            blocksize = os.path.getsize(
                self.filename) // (cores_used * PARTITIONS_PER_CORE)
            if blocksize == 0:
                cpus = mp.cpu_count()
                raise Exception(
                    f"CSV file too small to be read in parallel. Use normal mode. cpus: {cpus}")
            self.df = dataframe.read_csv(
                self.filename,
                blocksize=blocksize)

        # len for dask dataframe needs a client.
        if not self.use_dask and len(self.df) == 0:
            logger.error("Dataframe empty. Is the CSV file ok?")

        self.df = self.df.astype('object')
        self.header = list(self.df.columns.values)
        self.validate()

    def __len__(self):
        return len(self.df.index)

    def get_indexed_properties(self) -> Set[str]:
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
        if len(self.props_keys) > 0:
            for key in self.props_keys:
                prop, value = self._parse_prop(key, self.df.loc[idx, key])
                if value == value:  # skips nan valies
                    properties[prop] = value
        return properties

    def parse_constraints(self, idx):

        constraints = {}
        if len(self.constraints_keys) > 0:
            for key in self.constraints_keys:
                prop, value = self._parse_prop(key, self.df.loc[idx, key])
                constraints[prop] = ["==", self.df.loc[idx, key]]
        return constraints

    def parse_other_constraint(self, constraint_name, keys, idx):

        other_constraints = {}
        if len(keys) > 0:
            for key in keys:
                res = re.search(f"^{constraint_name}_date(>|<)?:", key)
                if res is not None:
                    prop = key[len(res.group(0)):]  # remove prefix
                    sort = res.group(0)[-2:][:1]  # get character before :

                    if sort != ">" and sort != "<":
                        sort = "=="
                    other_constraints[prop] = [
                        sort, {"_date": self.df.loc[idx, key]}]
                else:
                    prop = key[len(constraint_name):]  # remove "prefix
                    op = "=="
                    if prop[0] in [">", "<", "!"]:
                        op = prop[0]
                        prop = str(prop[1:])

                    value = self.df.loc[idx, key]
                    other_constraints[prop] = [op, value]

        return other_constraints

    def _parsed_command(self, idx, custom_fields: dict = None, constraints: dict = None, properties: dict = None):
        if custom_fields == None:
            custom_fields = {}
        query = {
            self.command: custom_fields
        }
        if properties:
            query[self.command][PROPERTIES] = properties

        if constraints:
            query[self.command][self.constraint_keyword] = constraints

        return query

    def _basic_command(self, idx, custom_fields: dict = None):
        properties = self.parse_properties(idx)
        constraints = self.parse_constraints(idx)
        return self._parsed_command(idx, custom_fields, constraints, properties)

    def validate(self):

        Exception("Validation not implemented!")
