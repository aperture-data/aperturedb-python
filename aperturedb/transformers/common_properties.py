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
        if self.adb_data_source is None and self.adb_timestamp is None and self.adb_main_object is None:
            return self.data[subscript]

        x = self.data[subscript]
        for cmd_dict in x[0]:
            try:
                if not isinstance(cmd_dict, dict):
                    continue
                for cmd_name in ["AddImage", "AddVideo", "AddBoundingBox", "AddPolygon"]:
                    if cmd_name in cmd_dict:
                        src_properties = cmd_dict[cmd_name].setdefault(
                            "properties", {})
                        if self.adb_data_source is not None:
                            src_properties["adb_data_source"] = self.adb_data_source
                        if self.adb_timestamp is not None:
                            src_properties["adb_timestamp"] = self.adb_timestamp
                        if self.adb_main_object is not None:
                            src_properties["adb_main_object"] = self.adb_main_object
            except Exception:
                logger.exception(
                    "Error applying common properties", stack_info=True)

        return x
