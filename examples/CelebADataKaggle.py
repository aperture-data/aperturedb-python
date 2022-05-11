from typing import List, Tuple
from aperturedb.KaggleData import KaggleData
import pandas as pd
import os
from PIL import Image


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
        t = [
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
        t[0]["AddImage"]["properties"]["keypoints"] = f"10 {p['lefteye_x']} {p['lefteye_y']} {p['righteye_x']} {p['righteye_y']} {p['nose_x']} {p['nose_y']} {p['leftmouth_x']} {p['leftmouth_y']} {p['rightmouth_x']} {p['rightmouth_y']}"
        image_file_name = os.path.join(
            self.workdir,
            'img_align_celeba/img_align_celeba',
            p["image_id"])
        blob = open(image_file_name, "rb").read()
        embedding = self.embedding_generator(Image.open(image_file_name))
        serialized = embedding.cpu().detach().numpy().tobytes()
        return t, [blob, serialized]
