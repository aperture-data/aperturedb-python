from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer
import traceback


class CommonProperties(Transformer):
    def __init__(self, data: Subscriptable, **kwargs) -> None:
        super().__init__(data)

        # Statically set some properties, these are not in the data
        self.adb_data_source = kwargs.get("adb_data_source", None)
        self.adb_timestamp = kwargs.get("adb_timestamp", None)
        self.adb_main_object = kwargs.get("adb_main_object", None)
        self.adb_annoted_by = kwargs.get("adb_annoted_by", None)
        self.adb_annoted = kwargs.get("adb_annoted_at", False)

    def getitem(self, subscript):
        x = self.data[subscript]
        try:
            # x is a transaction that has an add_image command and a blob
            for ic in self._add_image_index:
                # Set the static properties, if explicitly set
                if self.adb_data_source:
                    x[0][ic]["AddImage"]["properties"]["adb_data_source"] = self.adb_data_source
                if self.adb_timestamp:
                    x[0][ic]["AddImage"]["properties"]["adb_timestamp"] = self.adb_timestamp
                if self.adb_main_object:
                    x[0][ic]["AddImage"]["properties"]["adb_main_object"] = self.adb_main_object
                if self.adb_annoted_by:
                    x[0][ic]["AddImage"]["properties"]["adb_annotated_by"] = self.adb_annoted_by
                x[0][ic]["AddImage"]["properties"]["adb_annotated"] = self.adb_annoted

        except Exception as e:
            traceback.print_exc(limit=5)

        return x
