from collections import namedtuple
import os
from typing import List, Tuple
import zipfile
import pandas as pd
from unittest.mock import MagicMock, patch
from kaggle.api.kaggle_api_extended import KaggleApi
from aperturedb.KaggleData import KaggleData


kf = namedtuple("files", ["files"])
file = namedtuple("file", ["fileType", "totalBytes", "name"])


def custom_download(workdir, files):
    def dload_zip(dataset, path, quiet, unzip):
        assert(dataset in workdir)
        assert(path == workdir)
        assert(not unzip)
        assert(not quiet)
        # Simulate a download from kaggle.
        # the kaggle API leads to a zipped dataset downloaded into the dataset_ref path.
        os.makedirs(workdir, exist_ok=True)
        with zipfile.ZipFile(os.path.join(workdir, "file.zip"), "w") as zip:
            for file in files:
                zip.writestr(file["name"], file["contents"])
    return dload_zip


class GoodGuysBadGuysImageDataKaggle(KaggleData):
    def __init__(self):
        super().__init__(dataset_ref="gpiosenka/good-guysbad-guys-image-data-set")

    def generate_index(self, root: str, records_count: int) -> pd.DataFrame:
        return pd.read_csv(os.path.join(root, "index.csv"))

    def generate_query(self, idx: int) -> Tuple[List[dict], List[bytes]]:
        return super().generate_query(idx)


class CatDataKaggle(KaggleData):
    def __init__(self):
        super().__init__(dataset_ref="crawford/cat-dataset")

    def generate_index(self, root: str, records_count: int) -> pd.DataFrame:
        related_files = []
        for path, b, c in os.walk(root):
            for f in c:
                if f.endswith("jpg"):
                    related_files.append({
                        "image": os.path.join(path, f),
                        "annotation": os.path.join(path, f"{f}.cat")
                    })
        return pd.json_normalize(related_files)


class TestKaggleIngest():
    def common_setup(self) -> None:
        self.root = "kaggleds"
        self.authenticates = False

    def does_auth(self):
        # When kaggle.authenticate would be invoked.
        self.authenticates = True

    def test_builtin_indexer_csv(self):
        """
        This type of dataset comes wiht 1 CSV file, and Kaggle Dataset is able to
        process it without the need for a custom indexer.
        """
        self.common_setup()
        dataset_ref = "gpiosenka/good-guysbad-guys-image-data-set"
        f = file(".csv", 1024, "index.csv")
        k = kf(files = [f])
        archive_files = [{"name": "index.csv",
                          "contents": "filename,class\r\n001.jpg,1"}]
        dz = custom_download(f"kaggleds/{dataset_ref}", files=archive_files)

        # Mock the kaggle methods that are expected to be called from Kaggle Dataset.
        with patch.multiple(KaggleApi,
                            dataset_list_files = MagicMock(return_value=k),
                            dataset_download_files = MagicMock(side_effect=dz),
                            authenticate = MagicMock(
                                side_effect = self.does_auth)
                            ) as mocks:
            dataset = GoodGuysBadGuysImageDataKaggle()
        assert len(dataset) == 1
        assert os.path.exists(
            os.path.join(self.root, dataset_ref, "file.zip")) == True
        assert self.authenticates == True

    def test_ingest_annotations_images_without_index(self):
        self.common_setup()
        dataset_ref = "crawford/cat-dataset"
        f = file(".csv", 1024, "index.csv")
        k = kf(files = [f])
        archive_files = [
            {"name": "cat1.jpg", "contents": "filename,class\r\n001.jpg,1"},
            {"name": "cat1.jpg.cat", "contents": "1 2 3 4"},
            {"name": "cat2.jpg", "contents": "filename,class\r\n001.jpg,1"},
            {"name": "cat2.jpg.cat", "contents": "5 6 7 8"}
        ]
        dz = custom_download(f"kaggleds/{dataset_ref}", files=archive_files)

        # Mock the kaggle methods that are expected to be called from Kaggle Dataset.
        with patch.multiple(KaggleApi,
                            dataset_list_files = MagicMock(return_value=k),
                            dataset_download_files = MagicMock(side_effect=dz),
                            authenticate = MagicMock(
                                side_effect = self.does_auth)
                            ) as mocks:

            dataset = CatDataKaggle()
        assert len(dataset) == 2
        assert os.path.exists(
            os.path.join(self.root, dataset_ref, "file.zip")) == True
        assert self.authenticates == True
