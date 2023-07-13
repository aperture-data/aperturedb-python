from aperturedb import CSVParser
from aperturedb.ImageDataCSV import ImageDataProcessor,IMG_FORMAT
import logging

logger = logging.getLogger(__name__)
# SparseAddingCSV
# Check for item existance using constraints before adding
# Useful when adding larger resources where a portion already exist


class SparseAddingCSV(CSVParser.CSVParser):

    def __init__(self, entity_class, filename, df=None, use_dask=False):
        self.entity = entity_class
        self.keys_set = False
        super().__init__(filename, df=df, use_dask=use_dask)
        self.blobs_per_query = [0, 1]
        self.commands_per_query = [1, 1]
        self._setupkeys()

    def _setupkeys(self):
        if not self.keys_set:
            if not self.use_dask:
                self.keys_set = True
                self.props_keys       = [x for x in self.header[1:]
                                         if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
                self.constraints_keys       = [x for x in self.header[1:]
                                               if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        query_set = []

        hold_props_keys = self.props_keys
        self.props_keys = []
        self.command = "Find" + self.entity
        self.constraint_keyword = "constraints"
        entity_find = self._basic_command(idx, custom_fields = {"results":{"count":True}})
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

class ImageSparseAddCSV(SparseAddingCSV,ImageDataProcessor):
    def __init__(self, filename, check_image=True, n_download_retries=3, df=None, use_dask=False):
        ImageDataProcessor.__init__(
            self, check_image, n_download_retries)
        SparseAddingCSV.__init__(self, "Image", filename, df, use_dask)
        source_type = self.header[0]
        self.set_processor(use_dask, source_type)

        self.format_given     = IMG_FORMAT in self.header
        self.props_keys = list(filter(lambda prop: prop not in [
                               IMG_FORMAT, "filename"], self.props_keys))
        if self.source_type not in self.source_types:
            logger.error("Source not recognized: " + self.source_type)
            raise Exception("Error loading image: " + filename)
        self.source_loader    = {
            st: sl for st, sl in zip(self.source_types, self.loaders)
        }
    def getitem(self, idx):
        blob_set = []
        [query_set, empty_blobs] = super().getitem(idx)
        image_path = self.df.loc[idx, self.source_type]
        img_ok, img = self.source_loader[self.source_type](image_path)
        if not img_ok:
            logger.error("Error loading image: " + image_path)
            raise Exception("Error loading image: " + image_path)
        # element has 2 queries, only second has blob
        blob_set = [[], [img]]
        # must wrap the blob return for this item in a list
        return [query_set, [blob_set]]

