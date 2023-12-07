from typing import List, Tuple
from aperturedb.KaggleData import KaggleData
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


class CelebADataKaggle(KaggleData):
    """
    **ApertureDB ingestable Dataset based off
    [CelebA on kaggle](https://www.kaggle.com/datasets/jessicali9530/celeba-dataset)**
    """

    def __init__(self, **kwargs) -> None:
        self.records_count = -1
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

        logger.info(
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
                    "image_ref": 1,
                    "rectangle": {
                        "x": p["x_1"],
                        "y": p["y_1"],
                        "width": p["width"] if p["width"] > 0 else 1,
                        "height": p["height"] if p["height"] > 0 else 1,
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
        return q, [blob]
