import logging
from aperturedb import CSVParser

logger = logging.getLogger(__name__)

CONNECTION_CLASS = "ConnectionClass"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"


class ConnectionDataCSV(CSVParser.CSVParser):
    """**ApertureDB Connection Data.**

    This class loads the Connection Data which is present in a csv file,
    and converts it into a series of aperturedb queries.

    .. note::
        Is backed by a csv file with the following columns:

            ``ConnectionClass``, ``Class1@PROP_NAME``, ``Class2@PROP_NAME`` ... ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        ConnectionClass,_Image@id,_Descriptor@UUID,confidence,id,constraint_id
        has_image,321423532,AID-0X3E,0.4354,5432543254,5432543254
        has_image,42342522,BXY-AB1Z,0.6432,98476542,98476542
        ...

    **ConnectionClass**: Arbitrary class name for the entity this would be saved as.

    **<ClassName>@<PropertyName>**: This is a special combination of Class Name and Property Name that can uniquely identify an entity. ‘@’ is a delimeter, so should not be used in a property name.

    **PROP_NAME_1 .. PROP_NAME_N**: Arbitraty property names.

    **constraint_PROP_NAME_1**: A equality check against a unique property to ensure duplicates are not inserted.

    Example usage:

    .. code-block:: python

        data = ConnectionDataCSV("/path/to/ConnectionData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)



    .. important::
        This example csv's first row creates connection between an Image(id=321423532) and a descriptor(UUID=AID-0X3E)
        It also connects the images and descriptors in the subsequent rows.

        In the above example, the constraint_id ensures that a connection with the specified
        id would be only inserted if it does not already exist in the database.

    """

    def __init__(self, filename):

        super().__init__(filename)

        self.props_keys       = [x for x in self.header[3:]
                                 if not x.startswith(CSVParser.CONTRAINTS_PREFIX)]

        self.constraints_keys = [x for x in self.header[3:]
                                 if x.startswith(CSVParser.CONTRAINTS_PREFIX)]

        self.src_class   = self.header[1].split("@")[0]
        self.src_key     = self.header[1].split("@")[1]
        self.dst_class   = self.header[2].split("@")[0]
        self.dst_key     = self.header[2].split("@")[1]
        self.command     = "AddConnection"

    def getitem(self, idx):
        src_value = self.df.loc[idx, self.header[1]]
        dst_value = self.df.loc[idx, self.header[2]]
        connection_class = self.df.loc[idx, CONNECTION_CLASS]
        q = []

        try:

            ref_src = (2 * idx) % 10000 + 1
            fe_a = {
                "FindEntity": {
                    "_ref": ref_src,
                    "with_class": self.src_class,
                }
            }

            fe_a["FindEntity"]["constraints"] = {}
            fe_a["FindEntity"]["constraints"][self.src_key] = [
                "==", src_value]
            q.append(fe_a)

            ref_dst = ref_src + 1
            fe_b = {
                "FindEntity": {
                    "_ref": ref_dst,
                    "with_class": self.dst_class,
                }
            }

            fe_b["FindEntity"]["constraints"] = {}
            fe_b["FindEntity"]["constraints"][self.dst_key] = [
                "==", dst_value]
            q.append(fe_b)

            ae = self._basic_command(idx,
                                     custom_fields={
                                         "class": connection_class,
                                         "src": ref_src,
                                         "dst": ref_dst,
                                     })

            q.append(ae)
        except KeyError as ex:
            error_str = " expected key='" + ex.args[0] + "' in '" + self.df.loc[idx] + \
                "'. Ignored."
            logger.error(error_str)

        return q, []

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != CONNECTION_CLASS:
            raise Exception(
                "Error with CSV file field: 0 - " + CONNECTION_CLASS)
        if "@" not in self.header[1]:
            raise Exception("Error with CSV file field: 1")
        if "@" not in self.header[2]:
            raise Exception("Error with CSV file field: 2")
