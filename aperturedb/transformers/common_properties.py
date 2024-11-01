from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
import logging


logger = logging.getLogger(__name__)


class CommonProperties(Transformer):
    """
    This applies some common properties to the data.
    """

    def __init__(self, data: Subscriptable, **kwargs) -> None:
        """
        Args:
            data: Subscriptable object
            adb_data_source: Data source for the data
            adb_timestamp: Timestamp for the data
            adb_main_object: Main object for the data
        """
        super().__init__(data, **kwargs)

        # Statically set some properties, these are not in the data
        self.adb_data_source = kwargs.get("adb_data_source", None)
        self.adb_timestamp = kwargs.get("adb_timestamp", None)
        self.adb_main_object = kwargs.get("adb_main_object", None)

    def getitem(self, subscript):
        x = self.data[subscript]
        try:
            # x is a transaction that has an add_image command and a blob
            for ic in self._add_image_index:
                src_properties = x[0][ic]["AddImage"]["properties"]
                # Set the static properties, if explicitly set
                if self.adb_data_source:
                    src_properties["adb_data_source"] = self.adb_data_source
                if self.adb_timestamp:
                    src_properties["adb_timestamp"] = self.adb_timestamp
                if self.adb_main_object:
                    src_properties["adb_main_object"] = self.adb_main_object
        except Exception as e:
            logger.exception(e.with_traceback(), stack_info=True)

        return x
