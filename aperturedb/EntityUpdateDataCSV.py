from aperturedb import CSVParser
import logging

logger = logging.getLogger(__name__)
# we need to update conditionally; if_not_found only works for add.
# additionally there are two metrics in use: selection critera
# and update critera.
# find_<prop> is the column form to bind


class SingleEntityUpdateDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB General CSV Parser for Adding and Updating Properties in an Entity**


      Update an Entity to the data in the CSV
      What this means is:
      - If it doesn't exist, add it.
      - If it exists, update the properties.



       This class utilizes 2 conditionals
       - normal constraint_ to select the element
       - a series of updateif_ to determine if an update is necessary

       Conditionals:
         updateif>_prop - updates if the database value > csv value
         updateif<_prop - updates if the database value < csv value
         updateif!_prop - updates if the database value is != csv value

    :::note
    Is backed by a CSV file with the following columns (format optional):

        ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

        OR

        ``url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

        OR

        ``s3_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

        OR

        ``gs_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``
        ..
    :::

    Example CSV file::

        filename,id,label,constraint_id,format,dataset_ver,updateif>_dataset_ver,gen_blobsha1_sha
        /home/user/file1.jpg,321423532,dog,321423532,jpg,2,2,
        /home/user/file2.jpg,42342522,cat,42342522,png,2,2,
        ...

    Example usage:

    ```python
        data = ImageForceNewestDataCSV("/path/to/ImageData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```


    :::info
    In the above example, the constraint_id ensures that an Image with the specified
    id would be only inserted if it does not already exist in the database.
    :::
    """
    UPDATE_CONSTRAINT_PREFIX = "updateif_"

    def __init__(self, entity_class: str, filename: str, **kwargs):

        if entity_class in ("Image", "Video", "Blob", "BoundingBox", "Connection", "Polygon", "Descriptor", "DescriptorSet", "Frame"):
            self.entity = entity_class
            self.entity_selection = None
        else:
            self.entity = "Entity"
            self.entity_selection = entity_class
        self.keys_set = False
        super().__init__(filename, **kwargs)
        # We do not expect a blob in either query, if a subclass adds blob, the first query will need a blob.
        self.blobs_per_query = [0, 0]
        self.commands_per_query = [1, 1]
        self._setupkeys()

    def _setupkeys(self):
        if not self.keys_set:
            self.keys_set = True
            self.props_keys = [x for x in self.header[1:]
                               if not (x.startswith(CSVParser.CONSTRAINTS_PREFIX)
                                       or x.startswith(SingleEntityUpdateDataCSV.UPDATE_CONSTRAINT_PREFIX))]
            self.constraints_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.search_keys = [x for x in self.header[1:]
                                if x.startswith(SingleEntityUpdateDataCSV.UPDATE_CONSTRAINT_PREFIX)]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        query_set = []

        self.constraint_keyword = "if_not_found"
        self.command = "Add" + self.entity
        selector = {} if self.entity_selection is None else {
            "class": self.entity_selection}
        entity_add = self._basic_command(idx, custom_fields=selector)
        condition_add_failed = {"results": {0: {"status": ["==", 2]}}}
        self.command = "Update" + self.entity
        update_constraints = self.parse_constraints(idx)
        search_constraints = self.parse_other_constraint(
            SingleEntityUpdateDataCSV.UPDATE_CONSTRAINT_PREFIX, self.search_keys, idx)
        update_constraints.update(search_constraints)
        properties = self.parse_properties(idx)
        self.constraint_keyword = "constraints"
        selector = {} if self.entity_selection is None else {
            "with_class": self.entity_selection}
        entity_update = self._parsed_command(
            idx, selector, update_constraints, properties)
        query_set.append(entity_add)
        query_set.append([condition_add_failed, entity_update])

        if hasattr(self, "modify_item") and callable(self.modify_item):
            query_set = self.modify_item(query_set, idx)

#        raise Exception("State = " +str(query_set))
        return [query_set], []

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
        return valid


class EntityUpdatDataCSV(SingleEntityUpdateDataCSV):
    def __init__(self, entity_type, filename, df=None, use_dask=False):
        super().__init__("Entity", filename, df, use_dask)
        self.entity_type = entity_type
        # Add had blob and update has blob.
        self.blobs_per_query = [0, 0]

    def modify_item(self, query_set, idx):
        query_set[0]["AddEntity"]["class"] = self.entity_type
        return query_set
