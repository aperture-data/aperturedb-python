import os
import unittest
from test_Base import TestBase
import numpy as np
from PIL import Image
import io
import pandas as pd
import os


@unittest.skip("Not intended to be run on CI.")
class TestKaggleIngest(TestBase):
    def setUp(self) -> None:
        from aperturedb import DataHelper
        self.ds = DataHelper.DataHelper()
        return super().setUp()

    def test_ingest_images_in_dir(self):
        def process_row(dataset_ref, workdir, columns, row):
            value = {
                "img_blob": open(os.path.join(workdir, row[1]["filepaths"]), "rb").read(),
                "properties": {
                    c: row[1][c] for c in columns
                }
            }
            return value
        self.ds.ingest_kaggle(
            dataset_ref="gpiosenka/good-guysbad-guys-image-data-set",
            generator=process_row
        )

    def test_ingest_images_in_csv(self):
        def custom_generator(dataset_ref, workdir, columns, row):
            def image_to_byte_array(image: Image) -> bytes:
                c = image.convert("L")
                imgByteArr = io.BytesIO()
                c.save(imgByteArr, format='JPEG')
                imgByteArr = imgByteArr.getvalue()
                return imgByteArr

            def PixelsToMatrix(x):
                return np.array(x.split()).astype(np.float32).reshape(48, 48)
            value = {
                "index": row[0],
                "img_blob": image_to_byte_array(Image.fromarray(PixelsToMatrix(row[1]["pixels"]))),
                "properties": {
                    c: row[1][c] for c in columns[:-1]
                }
            }
            return value
        self.ds.ingest_kaggle(
            dataset_ref="https://www.kaggle.com/datasets/nipunarora8/age-gender-and-ethnicity-face-data-csv",
            generator=custom_generator
        )

    def test_ingest_annotations(self):
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

        def process_row(dataset_ref, workdir, columns, row):
            value = {
                "img_blob": open(row[1]["image"], "rb").read(),
            }
            return value

        self.ds.ingest_kaggle(
            indexer=get_collection_from_file_system,
            dataset_ref="crawford/cat-dataset",
            generator=process_row
        )
