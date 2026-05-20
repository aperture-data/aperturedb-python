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
            # Apply properties to AddImage commands
            for ic in self._add_image_index:
                src_properties = x[0][ic]["AddImage"].setdefault("properties", {})
                # Set the static properties, if explicitly set
                if self.adb_data_source:
                    src_properties["adb_data_source"] = self.adb_data_source
                if self.adb_timestamp:
                    src_properties["adb_timestamp"] = self.adb_timestamp
                if self.adb_main_object:
                    src_properties["adb_main_object"] = self.adb_main_object

            # Apply properties to AddVideo commands
            for ic in getattr(self, "_add_video_index", []):
                src_properties = x[0][ic]["AddVideo"].setdefault("properties", {})
                # Set the static properties, if explicitly set
                if self.adb_data_source:
                    src_properties["adb_data_source"] = self.adb_data_source
                if self.adb_timestamp:
                    src_properties["adb_timestamp"] = self.adb_timestamp
                if self.adb_main_object:
                    src_properties["adb_main_object"] = self.adb_main_object

            # Apply properties to AddBoundingBox commands
            for ic in getattr(self, "_add_bounding_box_index", []):
                src_properties = x[0][ic]["AddBoundingBox"].setdefault("properties", {})
                # Set the static properties, if explicitly set
                if self.adb_data_source:
                    src_properties["adb_data_source"] = self.adb_data_source
                if self.adb_timestamp:
                    src_properties["adb_timestamp"] = self.adb_timestamp
                if self.adb_main_object:
                    src_properties["adb_main_object"] = self.adb_main_object

            # Apply properties to AddPolygon commands
            for ic in getattr(self, "_add_polygon_index", []):
                src_properties = x[0][ic]["AddPolygon"].setdefault("properties", {})
                # Set the static properties, if explicitly set
                if self.adb_data_source:
                    src_properties["adb_data_source"] = self.adb_data_source
                if self.adb_timestamp:
                    src_properties["adb_timestamp"] = self.adb_timestamp
                if self.adb_main_object:
                    src_properties["adb_main_object"] = self.adb_main_object
        except Exception as e:
            logger.exception(e.with_traceback(None), stack_info=True)

        return x
