from aperturedb import Utils, ParallelLoader
from facenet import generate_embedding
from ExamplesHelper import ExamplesHelper
from CelebADataKaggle import CelebADataKaggle

search_set_name = "similar_celebreties"


def main(helper: ExamplesHelper):
    utils = Utils.Utils(helper.create_connector())
    utils.remove_descriptorset(search_set_name)
    utils.add_descriptorset(search_set_name, 512,
                            metric="L2", engine="FaissFlat")

    dataset = CelebADataKaggle(
        records_count=helper.params.images_count,
        embedding_generator=generate_embedding,
        search_set_name=search_set_name
    )
    print(len(dataset))

    loader = ParallelLoader.ParallelLoader(helper.create_connector())
    loader.ingest(dataset, stats=True)


if __name__ == "__main__":
    main(ExamplesHelper(mandatory_params={
        "images_count": {
            "type": int,
            "help": "The number of images to ingest into aperturedb"
        }
    }))
