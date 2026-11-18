from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.CommonLibrary import create_connector
from CocoDataPyTorch import CocoDataPyTorch
import argparse


def main(params):
    client = create_connector()
    coco_detection = CocoDataPyTorch("prepare_aperturedb", client=client)

    # Lets use some images from the coco which are annotated for the purpose of the demo
    images = []
    for index, (_, annotations) in enumerate(coco_detection.loaded_dataset):
        if annotations:
            images.append(coco_detection[index])
            if len(images) == params.images_count:
                break

    loader = ParallelLoader(client)
    loader.ingest(generator = images, stats=True)
    print(f"Inserted {params.images_count} images to aperturedb")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-images_count', type=int, required=True,
                        help="The number of images to ingest into aperturedb")
    return parser.parse_args()


if __name__ == "__main__":
    main(get_args())
