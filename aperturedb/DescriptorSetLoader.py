import ast
from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_NAME   = "name"
HEADER_DIM    = "dimensions"
HEADER_ENGINE = "engine"
HEADER_METRIC = "metric"
PROPERTIES    = "properties"
CONSTRAINTS   = "constraints"


class DescriptorSetGeneratorCSV(CSVParser.CSVParser):
    """**ApertureDB DescriptorSet Data loader.**

    .. warning::
        Deprecated. Use :class:`~aperturedb.DescriptorSetDataCSV.DescriptorSetDataCSV` instead.

    .. note::
        Is backed by a csv file with the following columns:

            ``name``, ``dimensions``, ``engine``, ``metric``, ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        name,dimensions,engine,metric
        dining_chairs,2048,FaissIVFFlat,L2
        chandeliers,2048,FaissIVFFlat,L2
        console_tables,2048,FaissIVFFlat,L2
        ...
    """

    def __init__(self, filename):

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[4:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[4:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

    def getitem(self, idx):

        # Metrics/Engine can be of the form:
        #       "IP", or
        #       ["IP" ...]

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

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

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


class DescriptorSetLoader(ParallelLoader.ParallelLoader):
    """**ApertureDB DescriptorSet Loader.**

    This class is to be used in combination with a **generator** object,
    for example :class:`~aperturedb.DescriptorSetLoader.DescriptorSetGeneratorCSV`,
    which is a class that implements iterable inteface and generates "descriptor set data" elements.

    Example::

            descriptor_set_data = {
                "name": "dining_chairs",
                "dimensions": 2048,
                "engine": "FaissIVFFlat",
                "metric": "L2"
            }
    """

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "descriptorSet"

    def generate_batch(self, entity_data):

        q = []

        for data in entity_data:

            ae = {
                "AddDescriptorSet": data
            }

            if PROPERTIES in data:
                ae["AddDescriptorSet"][PROPERTIES] = data[PROPERTIES]

            if CONSTRAINTS in data:
                ae["AddDescriptorSet"]["if_not_found"] = data[CONSTRAINTS]

            q.append(ae)

        if self.dry_run:
            print(q)

        return q, []
