
from typing import List, Tuple
import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
from aperturedb.Subscriptable import Subscriptable


class KaggleData(Subscriptable):
    """
    **Class to wrap around a Dataset retrieved from kaggle**

    A DataSet downloaded from kaggle does not implement a standard mechanism to iterate over it's values
    This class intends to provide an abstracion like that of a pytorch dataset.
    Where the iteration over Dataset elements yields an atomic record.

    .. note::
        This class should be subclassed with specefic implementations of generate_index and generate_query.

    Example subclass:

    .. code-block:: python

        class CelebADataKaggle(KaggleData):
           def __init__(self, **kwargs) -> None:
               self.records_count = kwargs["records_count"]
               self.embedding_generator = kwargs["embedding_generator"]
               self.search_set_name = kwargs["search_set_name"]
               self.records_count = kwargs["records_count"]
               super().__init__(dataset_ref = "jessicali9530/celeba-dataset",
                                records_count=self.records_count)

           def generate_index(self, root: str, records_count=-1) -> pd.DataFrame:
               attr_index = pd.read_csv(
                   os.path.join(root, "list_attr_celeba.csv"))
               bbox_index = pd.read_csv(
                   os.path.join(root, "list_bbox_celeba.csv"))
               landmarks_index = pd.read_csv(os.path.join(
                   root, "list_landmarks_align_celeba.csv"))
               partition_index = pd.read_csv(
                   os.path.join(root, "list_eval_partition.csv"))
               rows = attr_index.combine_first(bbox_index).combine_first(
                   landmarks_index).combine_first(partition_index)
               original_size = len(rows)
               records_count = records_count if records_count > 0 else original_size

               rows = rows[:records_count]

               print(
                   f"Created {len(rows)} items from {original_size} in the original dataset.")
               return rows

           def generate_query(self, idx: int) -> Tuple[List[dict], List[bytes]]:
               record = self.collection[idx]
               p = record
               q = [
                   {
                       "AddImage": {
                           "_ref": 1,
                           "properties": {
                               c: p[c] for c in p.keys()
                           },
                       }
                   }, {
                       "AddBoundingBox": {
                           "_ref": 2,
                           "image": 1,
                           "rectangle": {
                               "x": p["x_1"],
                               "y": p["y_1"],
                               "width": p["width"],
                               "height": p["height"]
                           }
                       }
                   }, {
                       "AddDescriptor": {
                           "set": self.search_set_name,
                           "connect": {
                               "ref": 1
                           }
                       }
                   }
               ]
               q[0]["AddImage"]["properties"]["keypoints"] = f"10 {p['lefteye_x']} {p['lefteye_y']} {p['righteye_x']} {p['righteye_y']} {p['nose_x']} {p['nose_y']} {p['leftmouth_x']} {p['leftmouth_y']} {p['rightmouth_x']} {p['rightmouth_y']}"
               image_file_name = os.path.join(
                   self.workdir,
                   'img_align_celeba/img_align_celeba',
                   p["image_id"])
               blob = open(image_file_name, "rb").read()
               embedding = self.embedding_generator(Image.open(image_file_name))
               serialized = embedding.cpu().detach().numpy().tobytes()
               return q, [blob, serialized]

        Args:
            dataset_ref (str): URL of kaggle dataset, for example 'https://www.kaggle.com/datasets/crawford/cat-dataset'
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
