from aperturedb import CSVParser
import logging
import hashlib  # for sha1
from datetime import datetime

logger = logging.getLogger(__name__)


class BlobNewestDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB General CSV Parser for Maintaining a Blob set with changing blob data.**

      Use this only when deleting entities is ok.

      Update an Entity which has an associated blob to the data in the CSV
      What this means is:
      - If it doesn't exist, add it.
      - If it exists and the blob hasn't changed, update it.
      - If it exists and the blob has changed, delete and re-add it.

       This means if these elements are part of a graph where they are linked by connections
        these connections will be need to be regenerated afterwards.

       Additionally note that this loader does not notice if a entity is removed.
       - If you wish to clean up removed entities, an increasing load id will cause
         all existing entities to be updated to the newest, and you can delete
         using the old load id.

       This class utilizes 3 conditionals
       - normal constraint_ to select the element
       - a series of updateif_ to determine if an update is necessary
       - one or more prop_ and the associated updateif blob conditionals
         to determine if a update or an delete/add is appropriate

       Generated fields
        Format is: gen_<type>_name

        type:
        - blobsha1 - the sha1 for the blob is calculated
        - blobsize - the length in bytes of the blob is calculate
        - insertdate - ISO Format of date ( this will always change! )

        the result is then used to identify if a blob has changed.

        The generated fields are to be left empty in the CSV input.

        Summary
         This requires a constraint to be able to check if an id exists, and a generated prop
          to be able to detect if the blob matches. It will ensure only one entity exists with
          the constraints and matching the managed blob constraints.

    :::note
    Is backed by a CSV file with the following columns (format optional):

        ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

        OR

        ``url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

        OR

        ``s3_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

        OR

        ``gs_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``
        ...
    :::

    Example CSV file::

        filename,id,label,constraint_id,format,dataset_ver,updateif>_dataset_ver,gen_blobsha1_sha
        /home/user/file1.jpg,321423532,dog,321423532,jpg,2,2,
        /home/user/file2.jpg,42342522,cat,42342522,png,2,2,
        ...

    Example usage:

    ``` python

        data = ImageForceNewestDataCSV("/path/to/WorkingImageDataset.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```

    :::info
    In the above example, the constraint_id ensures that an Image with the specified
    id would be only inserted if it does not already exist in the database.
    :::
    """
    UPDATE_CONSTRAINT_PREFIX = "updateif_"
    GENERATE_PROP_PREFIX = "gen_"

    def __init__(self, entity_class: str, filename: str, **kwargs):
        self.known_generators = ["blobsize", "blobsha1", "insertdate"]
        self.entity = entity_class
        self.keys_set = False
        super().__init__(filename, **kwargs)
        # these tell the query set code how many blobs and commands to expect in each query
        # query #1 = 1 blob, 1 command.
        # query #2 = 0 blobs, 1 command.
        # query #3 = 1 blob, 2 commands.

        # defintion of how many blobs and commands to expect per query, for ParallelQuerySet.
        self.blobs_per_query = [1, 0, 1]
        self.commands_per_query = [1, 1, 2]
        self._setupkeys()

        self._generated_cache = {}

    def _setupkeys(self):
        if not self.keys_set:
            self.keys_set = True
            self.props_keys = [x for x in self.header[1:]
                               if not (x.startswith(CSVParser.CONSTRAINTS_PREFIX)
                                       or x.startswith(BlobNewestDataCSV.UPDATE_CONSTRAINT_PREFIX)
                                       or x.startswith(BlobNewestDataCSV.GENERATE_PROP_PREFIX))]
            self.generated_keys = [x for x in self.header[1:]
                                   if x.startswith(BlobNewestDataCSV.GENERATE_PROP_PREFIX)]
            self.constraints_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.search_keys = [x for x in self.header[1:]
                                if x.startswith(BlobNewestDataCSV.UPDATE_CONSTRAINT_PREFIX)]

    # derived class interface for retrieving blob
    def read_blob(self, idx):
        raise Exception(
            "No Blob Defined for BlobNewestDataCSV ( requires subclass )")

    # creates generated data for an index based on supplied action
    def parse_generated(self, idx, action):
        generated = None
        if action == "blobsize":
            blob = self.read_blob(idx)
            generated = len(blob)
        elif action == "blobsha1":
            blob = self.read_blob(idx)
            generated = hashlib.sha1(blob).hexdigest()
        elif action == "insertdate":
            generated = datetime.now().isoformat()
        else:
            raise Exception(f"Unable to generate data for action {action}")
        return generated

    # filter in or out generated constraints
    def filter_generated_constraints(self, return_generated=False):
        filtered = []
        prefix_len = len(BlobNewestDataCSV.UPDATE_CONSTRAINT_PREFIX)
        for key in self.search_keys:
            key_postfix = key[prefix_len:]
            if "_" in key_postfix:
                (action, *proplist) = key_postfix.split('_')
                if action in self.known_generators:
                    if return_generated:
                        filtered.append(key)
                else:
                    # not a match - verify that this is a known property then.
                    if key_postfix[0] in [">", "<"]:
                        key_postfix = key_postfix[1:]
                    if key_postfix not in self.props_keys:
                        raise Exception(
                            f"Column {key} is a constraint, but {key_postfix} does not match a column, and {action} is not a known generated constraint type")
                    else:
                        if not return_generated:
                            filtered.append(key)
            else:
                if not return_generated:
                    filtered.append(key)
        return filtered

    # create generated constraints for specific index
    # match controls if constrain will be a positive selection (True) or negative(False)
    def create_generated_constraints(self, idx, match=True):
        constraints = {}
        generated_keys = self.filter_generated_constraints(
            return_generated=True)
        prefix_len = len(BlobNewestDataCSV.UPDATE_CONSTRAINT_PREFIX)
        for key in generated_keys:
            (action, *proplist) = key[prefix_len:].split('_')
            prop = '_'.join(proplist)
            op = "==" if match else "!="
            cache_key = f"{idx}_{action}"
            v = None
            if cache_key in self._generated_cache:
                v = self._generated_cache[cache_key]
            else:
                v = self.parse_generated(idx, action)
                # save result
                self._generated_cache[cache_key] = v
            constraints[prop] = [op, v]
        return constraints

    # create generated props for specific index
    def create_generated_props(self, idx):
        prefix_len = len(BlobNewestDataCSV.GENERATE_PROP_PREFIX)
        properties = {}
        blob = None
        for generate in self.generated_keys:
            (action, *proplist) = generate[prefix_len:].split('_')
            prop = '_'.join(proplist)
            cache_key = f"{idx}_{action}"
            v = None
            if cache_key in self._generated_cache:
                v = self._generated_cache[cache_key]
            else:
                v = self.parse_generated(idx, action)
                # save result
                self._generated_cache[cache_key] = v
            properties[prop] = v

        return properties

    def getitem(self, idx):
        idx = self.df.index.start + idx
        query_set = []

        # process is; add if not existing ( # Pt 1 )
        # if existing
        # if blob checks pass ( or there are none ) - update metadata ( Pt 2 )
        # if blob check FAILS
        # delete ( Pt 3 )
        # re-add ( Pt 4 )

        # In terms of the blobs/commands per query -
        # query #1 - add, 1 blob, 1 command.
        # query #2 - update. 0 blobs, 1 command.
        # query # - delete/re-add 1 blob( for re-add )  2 commands.

        self.constraint_keyword = "if_not_found"
        self.command = "Add" + self.entity

        # Part 1
        properties = self.parse_properties(idx)
        constraints = self.parse_constraints(idx)
        # we add generated prop, but do not test generated constraints, as they are used to differentiate between
        # an existing entity.
        gen_props = self.create_generated_props(idx)
        properties.update(gen_props)

        entity_add = self._parsed_command(idx, None, constraints, properties)

        # Part 2
        condition_add_failed = {"results": {0: {"status": ["==", 2]}}}
        self.command = "Update" + self.entity
        update_constraints = self.parse_constraints(idx)
        search_constraints = self.parse_other_constraint(BlobNewestDataCSV.UPDATE_CONSTRAINT_PREFIX,
                                                         self.filter_generated_constraints(), idx)
        # we test generated constraints here as they will stop an update from happening.
        generated_positive_constraints = self.create_generated_constraints(
            idx, match=True)
        update_constraints.update(search_constraints)
        update_constraints.update(generated_positive_constraints)
        properties = self.parse_properties(idx)
        self.constraint_keyword = "constraints"
        entity_update = self._parsed_command(
            idx, None, update_constraints, properties)

        # Part 3
        condition_add_and_update_failed = {"results": {
            0: {"status": ["==", 2]},  # exists
            1: {"count": ["==", 0]},  # but didn't update.
        }}

        self.command = "Delete" + self.entity
        entity_delete = self._parsed_command(idx, None, constraints, None)

        query_set.append(entity_add)
        query_set.append([condition_add_failed, entity_update])
        query_set.append([condition_add_and_update_failed,
                         [entity_delete, entity_add]])

        if hasattr(self, "modify_item") and callable(self.modify_item):
            query_set = self.modify_item(query_set, idx)

        blob = self.read_blob(idx)
        # blobs , 1 for add set, 0 for update set, 1 for delete/add set
        blob_set = [[blob], [], [blob]]

        return [[query_set], [blob_set]]

    def validate(self):
        self._setupkeys()
        valid = True
        if not self.use_dask:
            if len(self.constraints_keys) < 1:
                logger.error("Cannot add/update " +
                             self.entity + "; no constraint keys")
                valid = False
            if valid and len(self.search_keys) < 1:
                logger.error("Cannot update " + self.entity +
                             "; no update constraint keys")
                valid = False
            if len(self.filter_generated_constraints()) < 1:
                logger.error("Cannot differentiate update and reinsert for  " +
                             self.__class__.__name__ + ": no generated constraints for blob comparison")
                valid = False
        return valid
