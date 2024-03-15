from aperturedb import CSVParser
import logging

logger = logging.getLogger(__name__)
# SparseAddingDataCSV
# Check for item existance using constraints before adding
# Useful when adding larger resources where a portion already exist


class SparseAddingDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB General CSV Parser for Loading Blob data where a large amount of the blobs already exist.

    This is a blob loader where the entity is searched for first, before the blob data is passed to the server.
    This can be useful speedup if blob data is large in comparison to the amount of data actually causing loads

    This is an abstract class, ImageSparseAddDataCSV loads Images.

    """

    def __init__(self, entity_class: str, filename: str, **kwargs):
        self.entity = entity_class
        self.keys_set = False
        super().__init__(filename, **kwargs)
        self.blobs_per_query = [0, 1]
        self.commands_per_query = [1, 1]
        self._setupkeys()

    def _setupkeys(self):
        if not self.keys_set:
            self.keys_set = True
            self.props_keys = [x for x in self.header[1:]
                               if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.constraints_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        query_set = []

        hold_props_keys = self.props_keys
        self.props_keys = []
        self.command = "Find" + self.entity
        self.constraint_keyword = "constraints"
        entity_find = self._basic_command(
            idx, custom_fields={"results": {"count": True}})
        # proceed to second command if count == 0
        condition_find_failed = {"results": {0: {"count": ["==", 0]}}}
        self.props_keys = hold_props_keys
        self.command = "Add" + self.entity
        self.constraint_keyword = "if_not_found"
        entity_add = self._basic_command(idx)
        query_set.append(entity_find)
        query_set.append([condition_find_failed, entity_add])

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
        return valid
