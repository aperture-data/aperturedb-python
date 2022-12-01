import logging
from aperturedb import CSVParser

PROPERTIES  = "properties"
CONSTRAINTS = "constraints"
BLOB_PATH   = "filename"

logger = logging.getLogger(__name__)


class BlobDataCSV(CSVParser.CSVParser):
    """**ApertureDB Blob Data.**

    This class loads the Blob Data which is present in a csv file,
    and converts it into a series of aperturedb queries.

    .. note::
        Is backed by a csv file with the following columns:

            ``FILENAME``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP_NAME_1``

    **FILENAME**: The path of the blob object on the file system.

    **PROP_NAME_1 ... PROP_NAME_N**: Arbitrary property names associated with this blob.

    **constraint_PROP_NAME_1**: Constraints against specific property, used for conditionally adding a Blob.

    Example csv file::

        filename,name,lastname,age,id,constraint_id
        /mnt/blob1,John,Salchi,69,321423532,321423532
        /mnt/blob2,Johna,Salchi,63,42342522,42342522
        ...

    Example usage:

    .. code-block:: python

        data = BlobDataCSV("/path/to/BlobData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)



    .. important::
        In the above example, the constraint_id ensures that a blob with the specified
        id would be only inserted if it does not already exist in the database.
    """

    def __init__(self, filename, df=None, use_dask=False):

        super().__init__(filename, df=df, use_dask=use_dask)
        if not use_dask:
            self.props_keys       = [x for x in self.header[1:]
                                     if not x.startswith(CSVParser.CONSTRAINTS_PREFIX) and x != BLOB_PATH]
            self.constraints_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.command = "AddBlob"

    def getitem(self, idx):
        idx = self.df.index.start + idx
        filename = self.df.loc[idx, BLOB_PATH]
        blob_ok, blob = self.load_blob(filename)
        if not blob_ok:
            logger.error("Error loading blob: " + filename)
            raise Exception("Error loading blob: " + filename)

        q = []
        ab = self._basic_command(idx)
        q.append(ab)

        return q, [blob]

    def load_blob(self, filename):

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except Exception as e:
            logger.exception(e)

        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != BLOB_PATH:
            raise Exception("Error with CSV file field: " + BLOB_PATH)
