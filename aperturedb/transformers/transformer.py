from aperturedb.Subscriptable import Subscriptable
from aperturedb.CommonLibrary import create_connector
from aperturedb.Utils import Utils
import threading
import logging

logger = logging.getLogger(__name__)


class Transformer(Subscriptable):
    """
    Transformer is an abstract class that can be used to transform
    data before ingestion into aperturedb.

    :::info
    **Some build in transformers:**
        - CommonProperties: Add common properties to the data
        - ImageProperties: Add image properties to the data
        - Facenet: Add facenet embeddings to the data
    :::


    [Example](https://github.com/aperture-data/aperturedb-python/blob/develop/examples/similarity_search/add_faces.py) of how to use transformers:
        ```python
        from CelebADataKaggle import CelebADataKaggle
        from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
        from aperturedb.transformers.common_properties import CommonProperties
        from aperturedb.transformers.image_properties import ImageProperties

        .
        .
        .

        dataset = CelebADataKaggle()

        # Here's a pipeline that adds extra properties to the celebA dataset
        dataset = CommonProperties(
            dataset,
            adb_data_source="kaggle-celebA",
            adb_main_object="Face",
            adb_annoted=True)

        # some useful properties for the images
        dataset = ImageProperties(dataset)

        # Add the embeddings generated through facenet.
        dataset = FacenetPyTorchEmbeddings(dataset)

        ```

    """

    def __init__(self, data: Subscriptable, client=None, **kwargs) -> None:
        self.data = data

        # Inspect the first element to get the number of queries and blobs
        x = self.data[0]
        self._queries = len(x[0])
        self._blobs = len(x[1])
        self._blob_index = []
        self._add_image_index = []
        self._client = client
        self._thread_local = threading.local()

        bc = 0
        for i, c in enumerate(x[0]):
            command = None
            if isinstance(c, dict) and len(c) > 0:
                command = next(iter(c.keys()))
            if command in ["AddImage", "AddDescriptor", "AddVideo", "AddBlob"]:
                self._blob_index.append(i)
                bc += 1
            # Kept for backward compatibility
            if command == "AddImage":
                self._add_image_index.append(i)

        logger.info(f"Found {bc} blobs in the data")

        self.ncalls = 0
        self.cumulative_time = 0

    def getitem(self, subscript):
        raise NotImplementedError("Needs to be subclassed")

    def __len__(self):
        return len(self.data)

    def get_client(self):
        if not hasattr(self, "_thread_local"):
            self._thread_local = threading.local()

        if not hasattr(self._thread_local, "client"):
            if self._client is not None:
                if hasattr(self._client, "clone"):
                    self._thread_local.client = self._client.clone()
                else:
                    self._thread_local.client = self._client
            else:
                self._thread_local.client = create_connector()
        return self._thread_local.client

    def get_utils(self):
        return Utils(self.get_client())

    _ALLOWED_DELEGATED_ATTRIBUTES = frozenset({
        "use_dask",
        "strict_response_validation",
        "response_handler",
        "error_handler",
        "blobs_relative_to_csv",
        "commands_per_query",
        "blobs_per_query",
        "get_indices"
    })

    def __getattr__(self, name):
        # Delegate specific attribute access to the underlying data (generator)
        # to preserve behaviors from original generators (like CSVParser).
        if name in self._ALLOWED_DELEGATED_ATTRIBUTES and "data" in self.__dict__:
            return getattr(self.data, name)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'")
