
from typing import Callable, Tuple
import os
from matplotlib.pyplot import plot_date
import pandas as pd
from aperturedb.ImageLoader import ImageGeneratorCSV, ImageLoader
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.models.kaggle_models_extended import File

from aperturedb import Connector
import zipfile

from aperturedb.ParallelLoader import ParallelLoader


class DataHelper(object):
    """
    **Class to deal with Data in aperturedb**

    """

    def ingest_kaggle(
            self,
            dataset_ref: str,
            indexer: Callable[[str], pd.DataFrame] = None,
            generator: Callable[[str, str, pd.Index,
                                 Tuple[pd.Index, pd.Series]], dict] = None,
            query_generator: Callable[[str, str, Tuple[pd.Index, pd.Series]], None] = None) -> bool:
        """**Helper to ingest Dataset from kaggle into apertureDB**

        Different datasets on kaggle can be structured in a variety of ways, but the most common method is to have a top
        level csv, json file or sqlite file.

        In some cases, it's just a list of files, with correspondingly named annotations.
        Which needs some custom logic to be converted into an iterable. An example follows that lets such a DB be ingested.
        This funciton is specified as an argument called indexer.

        Example::

                def get_collection_from_file_system(root):
                    related_files = []
                    for path, b, c in os.walk(root):
                        for f in c:
                            if f.endswith("jpg"):
                                related_files.append({
                                    "image": os.path.join(path, f),
                                    "annotation": os.path.join(path, f"{f}.cat")
                                })
                    return pd.json_normalize(related_files)

        Args:
            dataset_ref (str): URL of kaggle dataset, for example 'https://www.kaggle.com/datasets/crawford/cat-dataset'
            indexer (Callable[[str], pd.DataFrame], optional): A function that takes a root in file system.
                It generates a DatFrame by grouping the files inside the root by custom logic. Defaults to None.
            generator (Callable[[str, str, pd.Index, Tuple[pd.Index, pd.Series]], dict], optional): If the dataset can be simply used with builtin loaders,
                like Image, Descriptor, Connection etc . Defaults to None.
            query_generator: If the dataset needs to be ingested with a non standard loader. The generator argument will be ignored.

        Raises:
            Exception: Could not iterate on the elements of the input Dataset
            Exception: Could not transform input dataset to Loader or Query generator.

        Returns:
            bool: True if no errors were encountered, False otherwise.
        """
        kaggle = KaggleApi()
        kaggle.authenticate()
        if "datasets/" in dataset_ref:
            dataset_ref = dataset_ref[dataset_ref.index(
                "datasets/") + len("datasets/"):]

        workdir = os.path.join("kaggleds", dataset_ref)

        files = kaggle.dataset_list_files(dataset_ref)
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
            # This can be inferred from the files of the DS. Uusally is a csv, json, sqlite, etc.
            csvs = filter(lambda f: f.fileType == ".csv", files.files)
            index = sorted(csvs, key=lambda f: -f.totalBytes)[0]

            if index is None:
                print(
                    f"Could not determine the index.  Files in the dataset = {files}")

            print(f"Found index {index}")

            collection = pd.read_csv(os.path.join(workdir, index.name))
        else:
            # There happen to be image files, along with some annotation files.
            try:
                collection = indexer(workdir)
            except Exception as e:
                help = f"Failed getting an index to the dataset. Specify custom indexer (https://...) ."
                raise Exception({
                    "exception": e,
                    "help": help
                })

        con = Connector.Connector(user="admin", password="admin")
        loader = ImageLoader(con)
        ploader = ParallelLoader(con)

        def process_row(row):
            custom_row = generator(
                dataset_ref,
                workdir,
                collection.columns,
                row)

            # Add some standard properties to specifically address this dataset.
            properties = custom_row["properties"]
            properties["index"] = row[0]
            properties["dataset_name"] = dataset_ref
            properties["dataset_source"] = "kaggle"
            return custom_row

        if query_generator is not None:
            try:
                ploader.ingest(
                    generator=list(map(lambda row: query_generator(
                        dataset_ref, workdir, collection.columns, row), collection.iterrows())),
                    numthreads=1,
                    batchsize=1,
                    stats=True
                )
            except Exception as e:
                help = f"Failed emitting a transcation for {collection.columns}."
                raise Exception({
                    "exception": e,
                    "help": help
                })
        else:
            try:
                loader.ingest(
                    generator=list(
                        map(lambda row: process_row(row), collection.iterrows())),
                    numthreads=1,
                    batchsize=1,
                    stats=True)
            except Exception as e:
                help = f"Failed ingesting a row from dataset with columns {collection.columns}. Perhaps define a custom generator"
                raise Exception({
                    "exception": e,
                    "help": help
                })
