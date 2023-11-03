import argparse
from aperturedb import Utils, ParallelLoader
import dbinfo
from CelebADataKaggle import CelebADataKaggle
from aperturedb.transformers.facenet_pytorch_embeddings import FacenetPyTorchEmbeddings
from aperturedb.transformers.common_properties import CommonProperties
from aperturedb.transformers.image_properties import ImageProperties

search_set_name = "similar_celebreties"


def main(params):
    utils = Utils.Utils(dbinfo.create_connector())
    utils.remove_descriptorset(search_set_name)

    dataset = CelebADataKaggle()

    # Here's a pipeline that adds extra properties to the celebA dataset
    dataset = CommonProperties(
        dataset,
        adb_data_source="kaggle-celebA",
        adb_main_object="Face")

    # some useful properties for the images
    dataset = ImageProperties(dataset)

    # Add the embeddings generated through facenet.
    dataset = FacenetPyTorchEmbeddings(dataset)

    # Limit the number of images to ingest
    dataset = dataset[:params.images_count]
    print(len(dataset))

    loader = ParallelLoader.ParallelLoader(dbinfo.create_connector())
    loader.ingest(dataset, stats=True)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-images_count', type=int, required=True,
                        help="The number of images to ingest into aperturedb")
    return parser.parse_args()


if __name__ == "__main__":
    main(get_args())
