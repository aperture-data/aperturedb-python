import io

from aperturedb.ParallelLoader import ParallelLoader
from PIL import Image
from ExamplesHelper import ExamplesHelper
from CocoDataPytorch import CocoDataPytorch


def main(config: ExamplesHelper):
    # Define a helper function to convert PIL.image to a bytes array.
    def image_to_byte_array(image: Image) -> bytes:
        imgByteArr = io.BytesIO()
        image.save(imgByteArr, format="JPEG")
        imgByteArr = imgByteArr.getvalue()
        return imgByteArr

    coco_detection = CocoDataPytorch("prepare_aperturedb")

    # Lets use some images from the coco which are annotated for the purpose of the demo
    images = []
    for t in coco_detection:
        X, y = t
        if len(y) > 0:
            images.append(t)
            if len(images) == config.params.images_count:
                break

    loader = ParallelLoader(config.create_connector())
    loader.ingest(generator = images, stats=True)
    print(f"Inserted {config.params.images_count} images to aperturedb")


if __name__ == "__main__":
    main(ExamplesHelper(mandatory_params={
        "images_count": {
            "type": int,
            "help": "The number of images to ingest into aperturedb"
        }
    }))
