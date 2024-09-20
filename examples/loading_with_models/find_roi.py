import json
from aperturedb.Descriptors import Descriptors
from aperturedb.CommonLibrary import create_connector
from aperturedb.Query import ObjectType

client = create_connector()

with open("text_embedding.json", "r") as f:
    embeddings = json.load(f)
    descriptorset = "marengo26"
    descriptors = Descriptors(client)
    descriptors.find_similar(
        descriptorset,
        embeddings["text_embedding"],
        k_neighbors=3,
        distances=True)
    clip_descriptors = descriptors.get_connected_entities(ObjectType.CLIP)
    for clips in clip_descriptors:
        for clip in clips:
            print(clip)
            print("-----")
