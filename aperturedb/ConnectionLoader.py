import sys
from aperturedb import ParallelLoader
from aperturedb import CSVParser

CONNECTION_CLASS = "ConnectionClass"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"


class ConnectionGeneratorCSV(CSVParser.CSVParser):
    """**ApertureDB Connection Data generator.**

    .. warning::
        Deprecated. Use :class:`~aperturedb.ConnectionDataCSV.ConnectionDataCSV` instead.

    .. note::
        Is backed by a csv file with the following columns:

            ``ConnectionClass``, ``Class1@PROP_NAME``, ``Class2@PROP_NAME`` ... ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        ConnectionClass,_Image@id,_Descriptor@UUID,confidence,id,constaint_id
        has_image,321423532,AID-0X3E,0.4354,5432543254,5432543254
        has_image,42342522,BXY-AB1Z,0.6432,98476542,98476542
        ...

    **ConnectionClass**: Arbitrary class name for the entity this would be saved as.

    **<ClassName>@<PropertyName>** This is a special combination of Class Name and
    Property Name that can uniquely identify an entity.
    '@' is a delimeter, so should not be used in a property name.

    **PROP_NAME_1 .. PROP_NAME_N** Arbitraty property names.

    **constraint_PROP_NAME_1** A equality check against a unique property to ensure duplicates are not inserted.
    """

    def __init__(self, filename):

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[3:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

        self.constraints_keys = [x for x in self.header[3:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

        self.src_class   = self.header[1].split("@")[0]
        self.src_key     = self.header[1].split("@")[1]
        self.dst_class   = self.header[2].split("@")[0]
        self.dst_key     = self.header[2].split("@")[1]

    def getitem(self, idx):

        data = {
            "class":      self.df.loc[idx, CONNECTION_CLASS],
            "src_class": self.src_class,
            "src_key":   self.src_key,
            "src_val":   self.df.loc[idx, self.header[1]],
            "dst_class": self.dst_class,
            "dst_key":   self.dst_key,
            "dst_val":   self.df.loc[idx, self.header[2]],
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
            raise Exception(
                "Error with CSV file field: 0 - " + CONNECTION_CLASS)
        if "@" not in self.header[1]:
            raise Exception("Error with CSV file field: 1")
        if "@" not in self.header[2]:
            raise Exception("Error with CSV file field: 2")


class ConnectionLoader(ParallelLoader.ParallelLoader):
    """**ApertureDB Connection Loader.**

    This executes in conjunction with a **generator** object,
    for example :class:`~aperturedb.ConnectionLoader.ConnectionGeneratorCSV`,
    which is a class that implements iterable inteface and generates "connection data" elements.

    Example::

            connection_data = {
                "class":       connection_class,
                "src_class": "_Image",
                "src_key":   "id",
                "src_val":   "321423532",
                "dst_class": "_Descriptor",
                "dst_key":   "UUID",
                "dst_val":   "AID-0X3E",
                "properties":  properties,
                "constraints": constraints,
            }
    """

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "connection"

    def generate_batch(self, entity_data):

        q = []

        ref_counter = 1

        for data in entity_data:

            try:

                ref_src = ref_counter
                ref_counter += 1
                fe_a = {
                    "FindEntity": {
                        "_ref": ref_src,
                        "with_class": data["src_class"],
                    }
                }

                fe_a["FindEntity"]["constraints"] = {}
                fe_a["FindEntity"]["constraints"][data["src_key"]] = [
                    "==", data["src_val"]]
                q.append(fe_a)

                ref_dst = ref_counter
                ref_counter += 1
                fe_b = {
                    "FindEntity": {
                        "_ref": ref_dst,
                        "with_class": data["dst_class"],
                    }
                }

                fe_b["FindEntity"]["constraints"] = {}
                fe_b["FindEntity"]["constraints"][data["dst_key"]] = [
                    "==", data["dst_val"]]
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
            except KeyError as ex:
                error_str = "ERROR: ConnectionLoader::generate_batch():" + \
                    " expected key='" + ex.args[0] + "' in '" + str(data) + \
                    "'. Ignored."
                print(error_str)
                sys.stdout.write(error_str + "\n")

        if self.dry_run:
            print(q)

        return q, []
