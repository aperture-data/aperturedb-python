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

    def _apply_common_properties(self, properties: dict):
        if self.adb_data_source is not None:
            properties["adb_data_source"] = self.adb_data_source
        if self.adb_timestamp is not None:
            properties["adb_timestamp"] = self.adb_timestamp
        if self.adb_main_object is not None:
            properties["adb_main_object"] = self.adb_main_object

    def getitem(self, subscript):
        if (
            self.adb_data_source is None and
            self.adb_timestamp is None and
            self.adb_main_object is None
        ):
            return self.data[subscript]

        x = self.data[subscript]
        for cmd_dict in x[0]:
            try:
                cmd_name = None
                if isinstance(cmd_dict, dict) and len(cmd_dict) > 0:
                    cmd_name = next(iter(cmd_dict.keys()))

                if cmd_name in ["AddImage", "AddVideo", "AddBoundingBox", "AddPolygon", "AddFrame"]:
                    src_properties = cmd_dict[cmd_name].setdefault(
                        "properties", {})
                    self._apply_common_properties(src_properties)
            except Exception:
                logger.exception(
                    "Error applying common properties", stack_info=True)

        return x
