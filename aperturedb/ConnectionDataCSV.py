import logging
from aperturedb.CSVParser import CSVParser, CONSTRAINTS_PREFIX
from aperturedb.Query import QueryBuilder

logger = logging.getLogger(__name__)

CONNECTION_CLASS = "ConnectionClass"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"


class ConnectionDataCSV(CSVParser):
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

    def __init__(self, filename, df=None, use_dask=False):
        super().__init__(filename, df=df, use_dask=use_dask)
        if not use_dask:
            self.props_keys       = [x for x in self.header[3:]
                                     if not x.startswith(CONSTRAINTS_PREFIX)]

            self.constraints_keys = [x for x in self.header[3:]
                                     if x.startswith(CONSTRAINTS_PREFIX)]

            self.src_class   = self.header[1].split("@")[0]
            self.src_key     = self.header[1].split("@")[1]
            self.dst_class   = self.header[2].split("@")[0]
            # Pandas appends a .n to the column name if there is a duplicate
            self.dst_key     = self.header[2].split("@")[1].split(".")[0]
            self.command     = "AddConnection"

    def getitem(self, idx):
        idx = self.df.index.start + idx
        src_value = self.df.loc[idx, self.header[1]]
        dst_value = self.df.loc[idx, self.header[2]]
        connection_class = self.df.loc[idx, CONNECTION_CLASS]
        q = []

        try:
            ref_src = (2 * idx) % 100000 + 1
            cmd_params = {
                "_ref": ref_src,
                "unique": True,
                "constraints": {
                    self.src_key: ["==", src_value]
                }
            }
            q.append(QueryBuilder.find_command(self.src_class, cmd_params))

            ref_dst = ref_src + 1
            cmd_params = {
                "_ref": ref_dst,
                "unique": True,
                "constraints": {
                    self.dst_key: ["==", dst_value]
                }
            }
            q.append(QueryBuilder.find_command(self.dst_class, cmd_params))

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
