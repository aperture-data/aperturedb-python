import ast
from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_NAME   = "name"
HEADER_DIM    = "dimensions"
HEADER_ENGINE = "engine"
HEADER_METRIC = "metric"
PROPERTIES    = "properties"
CONSTRAINTS   = "constraints"


class DescriptorSetDataCSV(CSVParser.CSVParser):
    """**ApertureDB DescriptorSet Data.**

    This class loads the Descriptor Set Data which is present in a csv file,
    and converts it into a series of aperturedb queries.

    .. note::
        Is backed by a csv file with the following columns:

            ``name``, ``dimensions``, ``engine``, ``metric``, ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        name,dimensions,engine,metric
        dining_chairs,2048,FaissIVFFlat,L2
        chandeliers,2048,FaissIVFFlat,L2
        console_tables,2048,FaissIVFFlat,L2
        ...

    Example code to create an instance:

    .. code-block:: python

        data = DescriptorSetDataCSV("/path/to/DescriptorSetData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)


    .. important::
        In the above example, the first row implies to create a Descriptor set called dining_chairs.
        The Descriptors in that set wouldb be expected to be an array of float64, of length 2048.
        When performing a search on this set, FaissIVFFlat engine would be used and the metric to compute
        the distance would be L2.
    """

    def __init__(self, filename, df=None, use_dask=False):

        super().__init__(filename, df=df, use_dask=use_dask)
        if not use_dask:
            self.props_keys       = [x for x in self.header[4:]
                                     if not x.startswith(CSVParser.CONTRAINTS_PREFIX)]
            self.constraints_keys = [x for x in self.header[4:]
                                     if x.startswith(CSVParser.CONTRAINTS_PREFIX)]
            self.command = "AddDescriptorSet"

    def getitem(self, idx):

        # Metrics/Engine can be of the form:
        #       "IP", or
        #       ["IP" ...]
        idx = self.df.index.start + idx
        metrics = self.df.loc[idx, HEADER_METRIC]
        metrics = metrics if "[" not in metrics else ast.literal_eval(metrics)
        engines = self.df.loc[idx, HEADER_ENGINE]
        engines = engines if "[" not in engines else ast.literal_eval(engines)

        data = {
            "name":       self.df.loc[idx, HEADER_NAME],
            "dimensions": self.df.loc[idx, HEADER_DIM],
            "engine":     engines,
            "metric":     metrics,
        }

        q = []
        ads = self._basic_command(idx, custom_fields=data)
        q.append(ads)

        return q, []

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != HEADER_NAME:
            raise Exception("Error with CSV file field: " + HEADER_NAME)
        if self.header[1] != HEADER_DIM:
            raise Exception("Error with CSV file field: " + HEADER_DIM)
        if self.header[2] != HEADER_ENGINE:
            raise Exception("Error with CSV file field: " + HEADER_ENGINE)
        if self.header[3] != HEADER_METRIC:
            raise Exception("Error with CSV file field: " + HEADER_METRIC)
