from aperturedb.Subscriptable import Subscriptable
import logging

logger = logging.getLogger(__name__)


class Transformer(Subscriptable):
    def __init__(self, data: Subscriptable) -> None:
        self.data = data

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
        logger.info(f"Found {bc} blobs in the data")
        logger.info(
            f"Found {len(self._add_image_index)} AddImage commands in the data")

    def __len__(self):
        return len(self.data)
