from pydoc import Helper
from aperturedb import KaggleDataset, Utils, ParallelQuery
from PIL import Image
import os
import pandas as pd
from facenet import generate_embedding
from ExamplesHelper import ExamplesHelper


search_set_name = "similar_celebreties"


def add_set(helper: ExamplesHelper):
    con = helper.create_connector()
    utils = Utils.Utils(con)
    utils.remove_descriptorset(search_set_name)
    utils.add_descriptorset(search_set_name, 512,
                            metric="L2", engine="FaissFlat")


def main(helper: ExamplesHelper):
    dataset_ref = "jessicali9530/celeba-dataset"
    # kaggle datasets do not conform to an enforcing structure.
    # This function is needed in addition to the item conversion (ref Example 1) to
    # have an iterable withe all the inormation needed per item.

    def get_collection_from_multiple_index(root):
        attr_index = pd.read_csv(
            os.path.join(root, "list_attr_celeba.csv"))
        bbox_index = pd.read_csv(
            os.path.join(root, "list_bbox_celeba.csv"))
        landmarks_index = pd.read_csv(os.path.join(
            root, "list_landmarks_align_celeba.csv"))
        partition_index = pd.read_csv(
            os.path.join(root, "list_eval_partition.csv"))
        rows = attr_index.combine_first(bbox_index).combine_first(
            landmarks_index).combine_first(partition_index)[:helper.params.images_count]
        print(f"Discovered {len(rows)} items in the original dataset.")
        return rows

    add_set(helper)

    dataset = KaggleDataset.KaggleDataset(
        dataset_ref=dataset_ref,
        indexer=get_collection_from_multiple_index
    )
    print(len(dataset))

    # define a function to convert an item from celebA to queries for
    # aperturedb.
    def process_item(dataset_ref, workdir, columns, row):
        p = row
        t = [
            {
                "AddImage": {
                    "_ref": 1,
                    "properties": {
                        c: p[c] for c in columns
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
                    "set": search_set_name,
                    "connect": {
                        "ref": 1
                    }
                }
            }
        ]
        t[0]["AddImage"]["properties"]["keypoints"] = f"10 {p['lefteye_x']} {p['lefteye_y']} {p['righteye_x']} {p['righteye_y']} {p['nose_x']} {p['nose_y']} {p['leftmouth_x']} {p['leftmouth_y']} {p['rightmouth_x']} {p['rightmouth_y']}"
        image_file_name = os.path.join(
            workdir,
            'img_align_celeba/img_align_celeba',
            p["image_id"])
        blob = open(image_file_name, "rb").read()
        embedding = generate_embedding(Image.open(image_file_name))
        serialized = embedding.cpu().detach().numpy().tobytes()
        return t, [blob, serialized]

    loader = ParallelQuery.ParallelQuery(helper.create_connector())
    loader.query(
        generator = list(map(lambda item: process_item(
            dataset_ref=dataset_ref,
            workdir = os.path.join("kaggleds", dataset_ref),
            columns = item.keys(),
            row = item
        ), dataset)),
        batchsize = 1,
        numthreads = 1,
        stats = True
    )


if __name__ == "__main__":
    main(ExamplesHelper(mandatory_params={
        "images_count": {
            "type": int,
            "help": "The number of images to ingest into aperturedb"
        }
    }))
