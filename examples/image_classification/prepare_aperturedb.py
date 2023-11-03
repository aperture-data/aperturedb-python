import io

from aperturedb.ParallelLoader import ParallelLoader
from PIL import Image
import dbinfo
from CocoDataPyTorch import CocoDataPyTorch
import argparse


def main(params):
    # Define a helper function to convert PIL.image to a bytes array.
    def image_to_byte_array(image: Image) -> bytes:
        imgByteArr = io.BytesIO()
        image.save(imgByteArr, format="JPEG")
        imgByteArr = imgByteArr.getvalue()
        return imgByteArr

    coco_detection = CocoDataPyTorch("prepare_aperturedb")

    # Lets use some images from the coco which are annotated for the purpose of the demo
    images = []
    for t in coco_detection:
        X, y = t
        if len(y) > 0:
            images.append(t)
            if len(images) == params.images_count:
                break

    loader = ParallelLoader(dbinfo.create_connector())
    loader.ingest(generator = images, stats=True)
    print(f"Inserted {params.images_count} images to aperturedb")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-images_count', type=int, required=True,
                        help="The number of images to ingest into aperturedb")
    return parser.parse_args()


if __name__ == "__main__":
    main(get_args())
