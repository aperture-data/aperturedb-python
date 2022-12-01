from aperturedb import ParallelLoader
from aperturedb import CSVParser

ENTITY_CLASS = "EntityClass"
PROPERTIES   = "properties"
CONSTRAINTS  = "constraints"


class EntityGeneratorCSV(CSVParser.CSVParser):
    """**ApertureDB Entity Data loader.**

    .. warning::
        Deprecated. Use :class:`~aperturedb.EntityDataCSV.EntityDataCSV` instead.

    .. note::
        Expects a csv file with the following columns:

            ``EntityClass``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        EntityClass,name,lastname,age,id,constaint_id
        Person,John,Salchi,69,321423532,321423532
        Person,Johna,Salchi,63,42342522,42342522
        ...
    """

    def __init__(self, filename):

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[1:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

    def getitem(self, idx):

        data = {}
        data["class"] = self.df.loc[idx, ENTITY_CLASS]

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != ENTITY_CLASS:
            raise Exception("Error with CSV file field: " + ENTITY_CLASS)


class EntityLoader(ParallelLoader.ParallelLoader):
    """**ApertureDB Entity Loader.**

        This class is to be used in combination with a "generator".
        The generator must be an iterable object that generated "entity_data"
        elements.

    Example::

            entity_data = {
                "class":       entity_class,
                "properties":  properties,
                "constraints": constraints,
            }
    """

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
                ae["AddEntity"]["if_not_found"] = data[CONSTRAINTS]

            q.append(ae)

        if self.dry_run:
            print(q)

        return q, blobs
