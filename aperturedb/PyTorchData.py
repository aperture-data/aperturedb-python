from typing import List, Tuple
from torch.utils.data import Dataset
from aperturedb.Subscriptable import Subscriptable


class PyTorchData(Subscriptable):
    """
    **Class to wrap around a Dataset retrieved from [PyTorch datasets](https://pytorch.org/vision/0.15/datasets.html)**

    The dataset in this case can be iterated over.
    So the only thing that needs to be implemented is generate_query,
    which takes an index and returns a query.

    :::note
    This class should be subclassed with a specific (custom) implementation of generate_query().
    :::

    Example subclass: [CocoDataPyTorch](https://github.com/aperture-data/aperturedb-python/blob/develop/examples/CocoDataPyTorch.py)

    """

    def __init__(self, dataset: Dataset) -> None:
        self.loaded_dataset = [t for t in dataset]

    def getitem(self, idx: int):
        return self.generate_query(idx)

    def __len__(self):
        return len(self.loaded_dataset)

    def generate_query(self, idx: int) -> Tuple[List[dict], List[bytes]]:
        """
        **Takes information from one atomic record from the Data and converts it to Query for apertureDB**

        Args:
            idx (int): index of the record in collection.

        Raises:
            Exception: _description_

        Returns:
            Tuple[List[dict], List[bytes]]: A pair of list of commands and optional list of blobs to go with them.
        """
        raise Exception("To be implemented by subclass")
