import ast
import json
from aperturedb import CSVParser
from aperturedb.DescriptorEngines import engines_object
from aperturedb.ServerCompatibility import vector_index_commands

HEADER_NAME = "name"
HEADER_DIM = "dimensions"
HEADER_ENGINE = "engine"
HEADER_ENGINES = "engines"
HEADER_METRIC = "metric"
PROPERTIES = "properties"
CONSTRAINTS = "constraints"


class DescriptorSetDataCSV(CSVParser.CSVParser):
    """**ApertureDB DescriptorSet Data.**

    Each row defines vector indexes on a descriptor class. Additional property
    columns become index metadata. Index definitions cannot be graph endpoints.

    !!! note
        Is backed by a CSV file with the following columns:

        ``name``, ``dimensions``, ``engine``, ``metric``, ``PROP_NAME_N``, ``constraint_PROP1``

    Example CSV file::

        name,dimensions,engine,metric
        dining_chairs,2048,IVF,L2
        chandeliers,2048,IVF,L2
        console_tables,2048,IVF,L2
        ...

    The ``engine`` column holds an engine name or a list of names. The loader creates
    one index per selected engine using its default settings.
    To configure engine settings, the third column can instead be ``engines``,
    holding that JSON object, for example ``"{""hnsw"": {""M"": 16}}"``.

    Example code to create an instance:

    ``` python

        data = DescriptorSetDataCSV("/path/to/DescriptorSetData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```


    !!! info
        In the above example, the first row implies to create a Descriptor set called dining_chairs.
        The Descriptors in that set would be expected to be an array of float32, of length 2048.
        When performing a search on this set, IVF engine would be used and the metric to compute
        the distance would be L2.
    """

    def __init__(self, filename: str, **kwargs):

        super().__init__(filename, **kwargs)

        self.props_keys = [x for x in self.header[4:]
                           if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[4:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.command = "AddDescriptorSet"

    def get_indices(self):
        if self.hello_version >= 2:
            return {}
        return {
            "entity": {
                "_DescriptorSet": self.get_indexed_properties()
            }
        }

    def getitem(self, idx):

        # Metrics/Engine can be of the form:
        #       "IP", or
        #       ["IP" ...]
        idx = self.df.index.start + idx
        metrics = self.df.loc[idx, HEADER_METRIC]
        metrics = metrics if "[" not in metrics else ast.literal_eval(metrics)
        data = {
            "name":       self.df.loc[idx, HEADER_NAME],
            "dimensions": self.df.loc[idx, HEADER_DIM],
            "metric":     metrics,
        }
        if self.header[2] == HEADER_ENGINES:
            data["engines"] = json.loads(self.df.loc[idx, HEADER_ENGINES])
        else:
            engines = self.df.loc[idx, HEADER_ENGINE]
            data["engines"] = engines_object(
                engines if "[" not in engines else ast.literal_eval(engines))

        if self.hello_version >= 2:
            if self.constraints_keys or any(self._csv_options.get(key) for key in ("_ref", "ref", "connect", "if_not_found")):
                raise ValueError(
                    "Descriptor set constraints cannot be applied to a server 1.0 vector index; use native index metadata/lifecycle commands")
            return vector_index_commands(self.hello_version, data["name"], data["dimensions"],
                                         data["metric"], data["engines"], self.parse_properties(idx)), []

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
        if self.header[2] not in (HEADER_ENGINE, HEADER_ENGINES):
            raise Exception("Error with CSV file field: " + HEADER_ENGINE)
        if self.header[3] != HEADER_METRIC:
            raise Exception("Error with CSV file field: " + HEADER_METRIC)
