from typing import List, Tuple
from aperturedb.Subscriptable import Subscriptable
import tensorflow as tf


class TensorFlowData(Subscriptable):
    """
    **Class to wrap around a Dataset retrieved from [Tensorflow datasets](https://www.tensorflow.org/datasets)**

    The dataset in this case can be iterated over.
    So the only thing that needs to be implemented is __init__ and generate_query,
    which takes an index and returns a query.

    :::note
    This class should be subclassed with a specific (custom) implementation of generate_query(),
    and __init__ should be called with the dataset to be wrapped.
    :::

    Example subclass: [Cifar10DataTensorflow](https://github.com/aperture-data/aperturedb-python/blob/develop/examples/Cifar10DataTensorflow.py)

    """

    def __init__(self, dataset: tf.data.Dataset) -> None:
        raise Exception("To be implemented by subclass")

    def getitem(self, idx: int):
        return self.generate_query(idx)

    def __len__(self):
        raise Exception("To be implemented by subclass")

    def generate_query(self, idx: int) -> Tuple[List[dict], List[bytes]]:
        """
        **Takes information from one atomic record from the Data and converts it to Query for ApertureDB**

        Args:
            idx (int): index of the record in collection.

        Raises:
            Exception: _description_

        Returns:
            Tuple[List[dict], List[bytes]]: A pair of list of commands and optional list of blobs to go with them.
        """
        raise Exception("To be implemented by subclass")
