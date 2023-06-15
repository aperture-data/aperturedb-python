from aperturedb import CSVParser
import logging

logger = logging.getLogger(__name__)
# we need to update conditionally; if_not_found only works for add.
# additionally there are two metrics in use: selection critera
# and update critera.
# find_<prop> is the column form to bind


class SingleEntityUpdateCSV(CSVParser.CSVParser):
    UPDATE_CONSTRAINT_PREFIX = "updateif_"

    def __init__(self, entity_class, filename, df=None, use_dask=False):
        self.entity = entity_class
        self.keys_set = False
        super().__init__(filename, df=df, use_dask=use_dask)
        self.blobs_per_query = [0, 0]
        self.commands_per_query = [1, 1]
        self._setupkeys()

    def _setupkeys(self):
        if not self.keys_set:
            if not self.use_dask:
                self.keys_set = True
                self.props_keys       = [x for x in self.header[1:]
                                         if not (x.startswith(CSVParser.CONSTRAINTS_PREFIX)
                                                 or x.startswith(SingleEntityUpdateCSV.UPDATE_CONSTRAINT_PREFIX))]
                self.constraints_keys       = [x for x in self.header[1:]
                                               if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
                self.search_keys       = [x for x in self.header[1:]
                                          if x.startswith(SingleEntityUpdateCSV.UPDATE_CONSTRAINT_PREFIX)]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        query_set = []

        self.constraint_keyword = "if_not_found"
        self.command = "Add" + self.entity
        entity_add = self._basic_command(idx)
        condition_add_failed = {"results": {0: {"status": ["==", 2]}}}
        self.command = "Update" + self.entity
        update_constraints = self.parse_constraints(self.df, idx)
        search_constraints = self.parse_other_constraint(
            SingleEntityUpdateCSV.UPDATE_CONSTRAINT_PREFIX, self.search_keys, self.df, idx)
        update_constraints.update(search_constraints)
        properties = self.parse_properties(self.df, idx)
        self.constraint_keyword = "constraints"
        entity_update = self._parsed_command(
            idx, None, update_constraints, properties)
        query_set.append(entity_add)
        query_set.append([condition_add_failed, entity_update])

        if hasattr(self, "modify_item") and callable(self.modify_item):
            query_set = self.modify_item(query_set, idx)

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
