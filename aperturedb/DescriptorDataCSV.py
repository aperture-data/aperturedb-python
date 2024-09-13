import numpy as np
from aperturedb import CSVParser
import logging
import os

logger = logging.getLogger(__name__)

HEADER_PATH = "filename"
HEADER_INDEX = "index"
HEADER_SET = "set"
HEADER_LABEL = "label"
PROPERTIES = "properties"
CONSTRAINTS = "constraints"


class DescriptorDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB Descriptor Data.**

    This class loads the Descriptor Data which is present in a CSV file,
    and converts it into a series of ApertureDB queries.

    :::note Is backed by a CSV file with the following columns, and a NumPy array file "npz" for the descriptors:
    ``filename``, ``index``, ``set``, ``label``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP_NAME_N``
    :::

    **filename**: Path to a npz file which comprises of np arrays.

    **index**: The 0 based index of a np array in the npz file.

    **set**: The search space to restrict the knn search queries to.

    **label**: Arbitrary name given to the label associated with this descriptor.

    **PROP_NAME_1 .. PROP_NAME_N**: Arbitrarily assigned properties to this descriptor.

    **constraint_PROP_NAME_1**: A constraint to ensure uniqueness when inserting this descriptor.

    Example CSV file::

        filename,index,set,label,isTable,UUID,constraint_UUID
        /mnt/data/embeddings/kitchen.npz,0,kitchen,kitchen_table,True,AID-0X3E,AID-0X3E
        /mnt/data/embeddings/kitchen.npz,1,kitchen,kitchen_table,True,BXY-AB1Z,BXY-AB1Z
        /mnt/data/embeddings/dining_chairs.npz,1,dining_chairs,special_chair,False,COO-SE1R,COO-SE1R
        ...

    Example usage:

    ``` python

        data = DescriptorDataCSV("/path/to/DescriptorData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```



    :::info
    In the above example, the index uniquely identifies the actual np array from the many arrays in the npz file
    which is same for line 1 and line 2. The UUID and constraint_UUID ensure that a Descriptor is inserted only once in the DB.

    Association of an entity to a Descriptor can be specified by first ingesting other Objects, then Descriptors and finally by
    using [ConnectionDataCSV](/python_sdk/data_loaders/csv_wrappers/ConnectionDataCSV)

    In the above example, the constraint_UUID ensures that a connection with the specified
    UUID would be only inserted if it does not already exist in the database.
    :::

    """

    def __init__(self, filename: str, **kwargs):

        super().__init__(filename, **kwargs)
        self.npy_arrays = {}
        self.has_label = False

        self.props_keys = [x for x in self.header[3:]
                           if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[3:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.command = "AddDescriptor"

    def get_indices(self):
        return {
            "entity": {
                "_Descriptor": self.get_indexed_properties()
            }
        }

    def getitem(self, idx):
        idx = self.df.index.start + idx
        filename = os.path.join(self.relative_path_prefix,
                                self.df.loc[idx, HEADER_PATH])
        index = self.df.loc[idx, HEADER_INDEX]
        desc_set = self.df.loc[idx, HEADER_SET]

        descriptor, desc_ok = self.load_descriptor(filename, index)

        if not desc_ok:
            logger.error("Error loading descriptor: " + filename + ":" + index)
            raise Exception("Error loading descriptor: " +
                            filename + ":" + index)

        q = []
        blobs = []
        # for data in image_data:
        custom_fields = {"set": desc_set}
        if self.has_label:
            custom_fields["label"] = self.df.loc[idx, HEADER_LABEL]

        ad = self._basic_command(idx, custom_fields)

        blobs.append(descriptor)
        q.append(ad)

        return q, blobs

    # This function can be re-defined by user when they have
    # app-specific structure on the npz file.
    def retrieve_by_index(self, filename, index):

        try:
            desc = self.npy_arrays[filename][index]
        except:
            err_msg = "Cannot retrieve descriptor {} from {}".format(
                str(index), filename)
            raise Exception(err_msg)

        return desc

    def load_descriptor(self, filename, index):

        if filename not in self.npy_arrays:
            self.npy_arrays[filename] = np.load(filename)

        # Can be defined by the user.
        descriptor = self.retrieve_by_index(filename, index)

        if len(descriptor) < 0:
            return [], False

        descriptor = descriptor.astype('float32').tobytes()

        return descriptor, True

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[3] == HEADER_LABEL:
            self.has_label = True

        if self.header[0] != HEADER_PATH:
            raise Exception(
                "Error with CSV file field: filename. Must be first field")
        if self.header[1] != HEADER_INDEX:
            raise Exception(
                "Error with CSV file field: index. Must be second field")
        if self.header[2] != HEADER_SET:
            raise Exception(
                "Error with CSV file field: set. Must be third field")
