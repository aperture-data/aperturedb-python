
from typing import List, Tuple
import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
from aperturedb.Subscriptable import Subscriptable


class KaggleData(Subscriptable):
    """
    **Class to wrap around a Dataset retrieved from kaggle**

    A DataSet downloaded from kaggle does not implement a standard mechanism to iterate over its values
    This class intends to provide an abstraction like that of a pytorch dataset
    where the iteration over Dataset elements yields an atomic record.

    :::note
    This class should be subclassed with specific implementations of generate_index and generate_query.
    :::

    Example subclass: [CelebADataKaggle](https://github.com/aperture-data/aperturedb-python/blob/develop/examples/CelebADataKaggle.py)

    Args:
        dataset_ref (str): URL of kaggle dataset, for example https://www.kaggle.com/datasets/jessicali9530/celeba-dataset
        records_count (int): number of records to provide to generate.

    """

    def __init__(
            self,
            dataset_ref: str,
            records_count: int = -1) -> None:
        self._collection = None
        self.records_count = records_count
        kaggle = KaggleApi()
        kaggle.authenticate()
        if "datasets/" in dataset_ref:
            dataset_ref = dataset_ref[dataset_ref.index(
                "datasets/") + len("datasets/"):]

        workdir = os.path.join("kaggleds", dataset_ref)

        files = kaggle.dataset_list_files(dataset_ref)

        # do not unzip from kaggle's API as it deletes the archive and
        # a subsequent run results in a redownload.
        x = kaggle.dataset_download_files(
            dataset=dataset_ref,
            path=workdir,
            quiet=False,
            unzip=False)

        archive = None
        for _, subdirs, dfiles in os.walk(workdir):
            if len(dfiles) == 1 and len(subdirs) == 0:
                archive = os.path.join(workdir, dfiles[0])

                with zipfile.ZipFile(archive, 'r') as zip_ref:
                    zip_ref.extractall(workdir)

                break
        self.workdir = workdir
        self.collection = self.generate_index(
            workdir, self.records_count).to_dict('records')

    def getitem(self, subscript):
        return self.generate_query(subscript)

    def __len__(self):
        return len(self.collection)

    def generate_index(self, root: str, records_count: int = -1) -> pd.DataFrame:
        """**Generate a way to access each record downloaded at the root**

        Args:
            root (str): Path to wich kaggle downloads a Dataset.

        Returns:
            pd.DataFrame: The Data loaded in a dataframe.
        """
        raise Exception("To be implemented by subclass")

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
