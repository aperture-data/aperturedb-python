import argparse
import io

from torchvision.datasets import CocoDetection
from aperturedb import ParallelQuery, Connector
from PIL import Image


def main(params):
    def image_to_byte_array(image: Image) -> bytes:
        imgByteArr = io.BytesIO()
        image.save(imgByteArr, format="JPEG")
        imgByteArr = imgByteArr.getvalue()
        return imgByteArr

    coco_detection = CocoDetection(
        root="coco/val2017",
        annFile="coco/annotations/person_keypoints_val2017.json")

    # coco dataset loads as an iterable with Tuple (X, [y1, y2 .... yn])
    # where X is the image (PIL.Image) and y's are multiple dicts with properties like:
    # area, bbox, category_id, image_id, id, iscrowd, keypoints, num_keypoints, segmentation
    con = Connector.Connector(user=params.db_username,
                              password=params.db_password)
    query = ParallelQuery.ParallelQuery(con)

    # define a function to convert an item from CocoDetection to queries for
    # aperturedb.
    def process_item(item):
        q = [{
            "AddImage": {
                "_ref": 1
            }
        }]
        blob = image_to_byte_array(item[0])
        q[0]["AddImage"]["properties"] = {
            "dataset_name": "prepare_aperturedb",
            "id": item[1][0]["image_id"]
        }
        return q, [blob]

    # Lets use some images from the coco which are annotated for the purpose of the demo
    images = []
    for t in coco_detection:
        X, y = t
        if len(y) > 0:
            images.append(t)
            if len(images) == params.images_count:
                break

    query.query(
        generator = list(map(lambda item: process_item(item), images)),
        batchsize=1,
        numthreads=1,
        stats=True
    )
    print(f"Inserted {params.images_count} images to aperturedb")


def get_args():
    parser = argparse.ArgumentParser()

    # Database config
    parser.add_argument('-db_host', type=str, default="localhost")
    parser.add_argument('-db_port', type=int, default=55555)
    parser.add_argument('-db_username', type=str, default="admin")
    parser.add_argument('-db_password', type=str, default="admin")
    parser.add_argument('-images_count', type=int,
                        help="The number of images to ingest into aperturedb", required=True)

    params = parser.parse_args()
    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
