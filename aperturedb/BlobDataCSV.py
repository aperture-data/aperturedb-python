import logging
import os
from aperturedb import CSVParser

PROPERTIES = "properties"
CONSTRAINTS = "constraints"
BLOB_PATH = "filename"

logger = logging.getLogger(__name__)


class BlobDataCSV(CSVParser.CSVParser):
    """**ApertureDB Blob Data.**

    This class loads the Blob Data which is present in a CSV file,
    and converts it into a series of ApertureDB queries.

    :::note Is backed by a CSV file with the following columns:
    ``FILENAME``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP_NAME_1``
    :::

    **FILENAME**: The path of the blob object on the file system.

    **PROP_NAME_1 ... PROP_NAME_N**: Arbitrary property names associated with this blob.

    **constraint_PROP_NAME_1**: Constraints against specific property, used for conditionally adding a Blob.

    Example CSV file::

        filename,name,lastname,age,id,constraint_id
        /mnt/blob1,John,Salchi,69,321423532,321423532
        /mnt/blob2,Johna,Salchi,63,42342522,42342522
        ...

    Example usage:

    ``` python

        data = BlobDataCSV("/path/to/BlobData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```


    :::info
    In the above example, the constraint_id ensures that a blob with the specified
    id would be only inserted if it does not already exist in the database.
    :::
    """

    def __init__(self, filename: str, **kwargs):

        super().__init__(filename, **kwargs)

        self.props_keys = [x for x in self.header[1:]
                           if not x.startswith(CSVParser.CONSTRAINTS_PREFIX) and x != BLOB_PATH]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.command = "AddBlob"

    def get_indices(self):
        return {
            "entity": {
                "_Blob": self.get_indexed_properties()
            }
        }

    def getitem(self, idx):
        filename = os.path.join(self.relative_path_prefix,
                                self.df.loc[idx, BLOB_PATH])
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
