from aperturedb import DataHelper, Connector, Utils
from PIL import Image
import argparse
import os
import pandas as pd
from facenet import generate_embedding, errors


search_set_name = "similar_celebreties"


def add_set(params):
    con = Connector.Connector(user=params.db_username,
                              password=params.db_password)
    utils = Utils.Utils(con)
    utils.remove_descriptorset(search_set_name)
    utils.add_descriptorset(search_set_name, 512,
                            metric="L2", engine="FaissFlat")


def main(params):

    # kaggle datasets do not conform to a enforcing structure.
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
            landmarks_index).combine_first(partition_index)
        print(f"Discovered {len(rows)} items in the original dataset.")
        return rows

    # define a function to convert an item from celebA to queries for
    # aperturedb.
    def process_item(dataset_ref, workdir, columns, row):
        p = row[1]
        t = [
            {
                "AddImage": {
                    "_ref": 1,
                    "properties": {
                        c: row[1][c] for c in columns
                    },
                }
            }, {
                "AddBoundingBox": {
                    "_ref": 2,
                    "image": 1,
                    "rectangle": {
                        "x": row[1]["x_1"],
                        "y": row[1]["y_1"],
                        "width": row[1]["width"],
                        "height": row[1]["height"]
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
            row[1]["image_id"])
        blob = open(image_file_name, "rb").read()
        embedding = generate_embedding(Image.open(image_file_name))
        serialized = embedding.cpu().detach().numpy().tobytes()
        # print(len(serialized))
        return t, [blob, serialized]

    add_set(params)

    ds = DataHelper.DataHelper()
    ds.ingest_kaggle(
        indexer=get_collection_from_multiple_index,
        query_generator=process_item,
        dataset_ref="jessicali9530/celeba-dataset"
    )

    print(f"Errors = {errors}")


def get_args():
    parser = argparse.ArgumentParser()

    # Database config
    parser.add_argument('-db_host', type=str, default="localhost")
    parser.add_argument('-db_port', type=int, default=55555)
    parser.add_argument('-db_username', type=str, default="admin")
    parser.add_argument('-db_password', type=str, default="admin")

    params = parser.parse_args()
    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
