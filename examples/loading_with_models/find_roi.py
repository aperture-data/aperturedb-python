import json
from aperturedb.Descriptors import Descriptors
from aperturedb.Utils import create_connector


client = create_connector()


with open("text_embedding.json", "r") as f:
    embeddings = json.load(f)
    descriptorset = "marengo26"
    d = Descriptors(client)
    d.find_similar(
        descriptorset, embeddings["text_embedding"], 3, distances=True)
    for de in d:
        print(de)
