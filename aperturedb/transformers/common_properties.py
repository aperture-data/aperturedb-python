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
            commands = [
                ("AddImage", getattr(self, "_add_image_index", [])),
                ("AddVideo", getattr(self, "_add_video_index", [])),
            ]

            for cmd_name, indices in commands:
                for ic in indices:
                    src_properties = x[0][ic][cmd_name].setdefault(
                        "properties", {})
                    if self.adb_data_source:
                        src_properties["adb_data_source"] = self.adb_data_source
                    if self.adb_timestamp:
                        src_properties["adb_timestamp"] = self.adb_timestamp
                    if self.adb_main_object:
                        src_properties["adb_main_object"] = self.adb_main_object

            for cmd_dict in x[0]:
                cmd_name = list(cmd_dict.keys())[0]
                if cmd_name in ["AddBoundingBox", "AddPolygon"]:
                    src_properties = cmd_dict[cmd_name].setdefault(
                        "properties", {})
                    if self.adb_data_source:
                        src_properties["adb_data_source"] = self.adb_data_source
                    if self.adb_timestamp:
                        src_properties["adb_timestamp"] = self.adb_timestamp
                    if self.adb_main_object:
                        src_properties["adb_main_object"] = self.adb_main_object

        except Exception as e:
            logger.exception(e.with_traceback(None), stack_info=True)

        return x
