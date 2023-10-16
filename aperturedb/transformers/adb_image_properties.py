from aperturedb.Subscriptable import Subscriptable
from PIL import Image
import traceback
import io


class ADBImageProperties(Subscriptable):
    def __init__(self, data: Subscriptable, **kwargs) -> None:
        self.data = data

        # Statically set some properties, these are not in the data
        self.adb_data_source = kwargs.get("adb_data_source", None)
        self.adb_timestamp = kwargs.get("adb_timestamp", None)
        self.adb_main_object = kwargs.get("adb_main_object", None)
        self.adb_annoted_by = kwargs.get("adb_annoted_by", None)
        self.adb_annoted = kwargs.get("adb_annoted_at", False)

        # Inspect the first element to get the number of queries and blobs
        x = self.data[0]
        self._queries = len(x[0])
        self._blobs = len(x[1])
        self._blob_index = []
        self._add_image_index = []

        bc = 0
        for i, c in enumerate(x[0]):
            command = list(c.keys())[0]
            if command in ["AddImage", "AddDescriptor", "AddVideo", "AddBlob"]:
                self._blob_index.append(bc)
                if command == "AddImage":
                    self._add_image_index.append(bc)
                bc += 1
        print(f"ADBImageProperties: Found {bc} blobs in the data")
        print(
            f"ADBImageProperties: Found {len(self._add_image_index)} AddImage commands in the data")

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
                    x[0][ic]["AddImage"]["properties"]["adb_annoted_by"] = self.adb_annoted_by
                x[0][ic]["AddImage"]["properties"]["adb_annoted"] = self.adb_annoted

                # Compute the dynamic properties and apply them to metadata
                x[0][ic]["AddImage"]["properties"]["adb_image_size"] = len(
                    x[1][ic])

                # Compute the image dimensions.
                # pil_image = Image.open(x[1][ic])
                pil_image = Image.open(io.BytesIO(x[1][ic]))
                x[0][ic]["AddImage"]["properties"]["adb_width"] = pil_image.width
                x[0][ic]["AddImage"]["properties"]["adb_height"] = pil_image.height

        except Exception as e:
            traceback.print_exc(limit=5)

        return x

    def __len__(self):
        return len(self.data)
