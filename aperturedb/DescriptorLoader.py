import numpy as np

from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_PATH  = "filename"
HEADER_INDEX = "index"
HEADER_SET   = "set"
HEADER_LABEL = "label"
PROPERTIES   = "properties"
CONSTRAINTS  = "constraints"


class DescriptorGeneratorCSV(CSVParser.CSVParser):
    """
    **ApertureDB Descriptor Data generator.**

    .. warning::
        Deprecated. Use :class:`~aperturedb.DescriptorDataCSV.DescriptorDataCSV` instead.

    .. note::
        Is backed by a csv file with the following columns, and a numpy
        array file "npz" for the descriptors:

            ``filename``, ``index``, ``set``, ``label``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP_NAME_N``

    **filename**: Path to a npz file which comprises of np arrays.

    **index**: The 0 based index of a np array in the npz file.

    **set**: The search space to restrict the knn search queries to.

    **label**: Arbitraty name given to the label associated with this descriptor.

    **PROP_NAME_1 .. PROP_NAME_N**: Arbitrarily assigned properties to this descriptor.

    **constraint_PROP_NAME_1**: A constraint to enusre uniqueness when inserting this descriptor.

    Example csv file::

        filename,index,set,label,isTable,UUID,constraint_UUID
        /mnt/data/embeddings/kitchen.npz,0,kitchen,kitchen_table,True,AID-0X3E,AID-0X3E
        /mnt/data/embeddings/kitchen.npz,1,kitchen,kitchen_table,True,BXY-AB1Z,BXY-AB1Z
        /mnt/data/embeddings/dining_chairs.npz,1,dining_chairs,special_chair,False,COO-SE1R,COO-SE1R
        ...

    .. important::
        In the above example, the index uniqely identiifes the actual np array from the many arrays in the npz file
        which is same for line 1 and line 2. The UUID and constraint_UUID ensure that a Descriptor is inserted only once in the DB.

        Association of an entity to a Descriptor can be specified by first ingesting other Objects, then Descriptors and finally by
        using :class:`~aperturedb.ConnectionLoader.ConnectionLoader`

    """

    def __init__(self, filename):

        super().__init__(filename)

        self.npy_arrays = {}
        self.has_label = False

        self.props_keys       = [x for x in self.header[3:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[3:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

    def getitem(self, idx):

        filename = self.df.loc[idx, HEADER_PATH]
        index    = self.df.loc[idx, HEADER_INDEX]
        desc_set = self.df.loc[idx, HEADER_SET]

        descriptor, desc_ok = self.load_descriptor(filename, index)

        if not desc_ok:
            print("Error loading descriptor: " + filename + ":" + index)
            raise Exception("Error loading descriptor: " +
                            filename + ":" + index)

        data = {
            "descriptor_blob": descriptor,
            "set": desc_set
        }

        if self.has_label:
            data[HEADER_LABEL] = self.df.loc[idx, HEADER_LABEL]

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

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


class DescriptorLoader(ParallelLoader.ParallelLoader):
    """**ApertureDB Descriptor Loader.**

    This class is to be used in combination with a **generator** object,
    for example :class:`~aperturedb.DescriptorLoader.DescriptorGeneratorCSV`,
    which is a class that implements iterable inteface and generates "descriptor data" elements.

    Example::

            descriptor_data = {
                "label": label,
                "set": set_name,
                "properties": properties,
                "constraints": constraints,
                "descriptor_blob": (bytes),
            }
    """

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "descriptor"

    def generate_batch(self, image_data):

        q = []
        blobs = []

        for data in image_data:

            ai = {
                "AddDescriptor": {
                    "set": data[HEADER_SET]
                }
            }

            if "properties" in data:
                ai["AddDescriptor"]["properties"] = data["properties"]
            if "constraints" in data:
                ai["AddDescriptor"]["if_not_found"] = data["constraints"]
            if "label" in data:
                ai["AddDescriptor"]["label"] = data["label"]

            if "descriptor_blob" not in data or len(data["descriptor_blob"]) == 0:
                print("WARNING: Skipping empty descriptor.")
                continue

            blobs.append(data["descriptor_blob"])
            q.append(ai)

        if self.dry_run:
            print(q)

        return q, blobs
