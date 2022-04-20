from typing import Dict, List, Optional
from aperturedb import Connector, Constraints, Operations
from aperturedb.Collection import Collection
from torch.utils.data import Dataset


class Repository:
    """**Repository is a store of homogenous entities**

    Notes:
        - There should be a way to convert to/from pytorch dataset into Data in aperturedb.
        - This has options to extract the objects in the database in various ways, controled by filter ctriteria.

    Example::

        dataset = Repository.filter(constraints)
        rectangles, labels = pytorch.infer(dataset)
        dataset.add_BoundingBoxes(rectangles, labels)
    """

    def __init__(self, db: Connector) -> None:
        raise Exception({
            "message": "Should not be instantiated. Construct an object from deriving subclass."
        })

    def _find_command(self):
        return f"Find{self._object_type}"

    def _find_query(self, constraints: Constraints.Constraints):
        query = {
            self._find_command(): {
                "results": {
                    "all_properties": True
                },
                "blobs": False
            }
        }
        if constraints is not None:
            query[self._find_command()]["constraints"] = constraints.query()
        return query

    def _exchange(self, queries):
        resp, _ = self._db.query(queries)
        resp_for_class = resp[0][self._find_command()]
        if resp_for_class["status"] == 0:
            return resp_for_class["entities"]
        else:
            raise Exception(resp)

    def retrieve(self,
                 constraints: Constraints.Constraints,
                 operations: Operations.Operations,
                 limit: int) -> Collection:
        """**Retrieve the objects from the Server based on ctriteria**

        A new search will throw away the results of any previous search
        Without any constraints the method acts as ``find_all``

        Args:
            constraints: The criteria for search, optional
            operations: Operations before returning the list, optional
            limit: Maximum number of objects in the collection.

        Returns:
            Collection of Objects.
        """
        return Collection(self._exchange(queries=[self._find_query(constraints)]))

    def ingest_pytorch_dataset(self, dataset: Dataset, converter=None):
        """
        *Helper to ingest Pytorch Datasets into aperturedb**

        Args:
            dataset (Dataset): Object of type Dataset from pytorch.datasets
            converter (_type_, optional): A function that converts a iterable to information for ApertureDB objects. Defaults to None.
        """
        pass

    def ingest_kaggle_dataset(self, dataset_ref: str, generator=None, converter=None, kaggle_credentials=None):
        """
        **Helper to ingest Kaggle's Datasets into aperturedb.**

        Args:
            dataset_ref (str): A unique identitfier on kaggle for a dataset. In the form of <user>/<dataset-name>
            generator (_type_, optional): _description_. Many datasets in kaggle have a TOC. Some don't. Is needed for those, to generate an iterable.
            converter (_type_, optional): _description_. Map all the information to ApertureDB objects.
        """
        pass
