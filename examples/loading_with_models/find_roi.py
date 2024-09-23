import json
from aperturedb.Descriptors import Descriptors
from aperturedb.CommonLibrary import create_connector
from aperturedb.Query import ObjectType

client = create_connector()

with open("text_embedding.json", "r") as f:
    # Load the embeddings from the json file. Look at get_tl_embedding.py for more details
    # on how it was generated.
    embeddings = json.load(f)

    # We will search from a set of descriptors in the DB called "marengo26".
    descriptorset = "marengo26"

    # Find similar descriptors to the text_embedding in the descriptorset.
    descriptors = Descriptors(client)
    descriptors.find_similar(
        descriptorset,
        embeddings["text_embedding"],
        k_neighbors=3,
        distances=True)

    # Find connected clips to the descriptors.
    clip_descriptors = descriptors.get_connected_entities(ObjectType.CLIP)

    # Show the metadata of the clips.
    for clips in clip_descriptors:
        for clip in clips:
            print(clip)
            print("-----")
