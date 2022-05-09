
from typing import Callable
import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile


class KaggleDataset(object):
    """
    **Class to wrap around a Dataset retrieved from kaggle**

    """

    def __init__(
            self,
            dataset_ref: str,
            indexer: Callable[[str], pd.DataFrame] = None) -> None:
        """
        A DataSet downloaded from kaggle does not implement a standard mechanism to iterate over it's values
        This class intends to provide an abstracion like that of a pytorch dataset.
        Where the iteration over Dataset elements yields a Row from the dataframe.

        Args:
            dataset_ref (str): URL of kaggle dataset, for example 'https://www.kaggle.com/datasets/crawford/cat-dataset'
            indexer (Callable[[str], pd.DataFrame], optional): A function that takes a root in file system.
                It generates a DatFrame by grouping the files inside the root by custom logic. Defaults to None.
        """
        self._collection = None
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
            unzip=False)

        archive = None
        for _, subdirs, dfiles in os.walk(workdir):
            if len(dfiles) == 1 and len(subdirs) == 0:
                archive = os.path.join(workdir, dfiles[0])

                with zipfile.ZipFile(archive, 'r') as zip_ref:
                    zip_ref.extractall(workdir)

                break

        if len(files.files) > 0 and indexer is None:
            # This can be inferred from the files of the DS. Usually is a csv, json, sqlite, etc.
            csvs = filter(lambda f: f.fileType == ".csv", files.files)
            index = sorted(csvs, key=lambda f: -f.totalBytes)[0]

            if index is None:
                print(
                    f"Could not determine the index.  Files in the dataset = {files}")

            print(f"Found index {index}")

            self.collection = pd.read_csv(os.path.join(
                workdir, index.name)).to_dict('records')
        else:
            # There happen to be image files, or multiple index files along with some annotation files.
            # If this is the case, the class needs to be instantiated with custom indexer.
            try:
                self.collection = indexer(workdir).to_dict('records')
            except Exception as e:
                help = f"""Failed getting an index to the dataset.
                    Specify custom indexer (https://python.docs.aperturedata.io/autoapi/aperturedb/KaggleDataset/index.html) .
                    """
                raise Exception({
                    "exception": e,
                    "help": help
                })

    def __getitem__(self, index):
        if index >= len(self.collection):
            raise StopIteration
        return self.collection[index]

    def __len__(self):
        return len(self.collection)
