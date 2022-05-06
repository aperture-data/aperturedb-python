import io

from torchvision.datasets import CocoDetection
from aperturedb import ParallelQuery
from PIL import Image
from ExamplesHelper import ExamplesHelper


def main(config: ExamplesHelper):
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
    query = ParallelQuery.ParallelQuery(config.create_connector())

    # define a function to convert an item from CocoDetection to queries for
    # aperturedb.
    def process_item(item):
        q = [{
            "AddImage": {
                "properties": {
                    "dataset_name": "prepare_aperturedb",
                    "id": item[1][0]["image_id"]
                }
            }
        }]

        blob = image_to_byte_array(item[0])
        return q, [blob]

    # Lets use some images from the coco which are annotated for the purpose of the demo
    images = []
    for t in coco_detection:
        X, y = t
        if len(y) > 0:
            images.append(t)
            if len(images) == config.params.images_count:
                break

    query.query(
        generator = list(map(lambda item: process_item(item), images)),
        batchsize=1,
        numthreads=1,
        stats=True
    )
    print(f"Inserted {config.params.images_count} images to aperturedb")


if __name__ == "__main__":
    main(ExamplesHelper(mandatory_params={
        "images_count": {
            "type": int,
            "help": "The number of images to ingest into aperturedb"
        }
    }))
