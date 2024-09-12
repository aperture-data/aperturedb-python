import time
import AlexNetClassifier as alexnet
from aperturedb import PyTorchDataset
from aperturedb.CommonLibrary import create_connector

client = create_connector()

out_file_name = "classification.txt"
query = [{
    "FindImage": {
        "constraints": {
            "dataset_name": ["==", "prepare_aperturedb"]
        },
        "operations": [
            {
                "type": "resize",
                "width": 256,
                "height": 256
            }
        ],
        "results": {
            "list": ["image_id"],
        }
    }
}]


classifier = alexnet.AlexNetClassifier()
with open(out_file_name, 'w') as classification:
    dataset = PyTorchDataset.ApertureDBDataset(
        client=client, query=query, label_prop='image_id')
    start = time.time()
    for item in dataset:
        image, id = item
        label, conf = classifier.classify(image)
        classification.write(f"{id}: {label}, confidence = {conf}\n")
    print("\rRetrieval performance (imgs/s):",
          len(dataset) / (time.time() - start), end="")

print(f"\nWritten classification results into {out_file_name}")
