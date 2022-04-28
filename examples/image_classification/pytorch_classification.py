import time
from aperturedb import Connector
import AlexNetClassifier as alexnet

db = Connector.Connector(user="admin", password="admin")
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
        "batch": {},
        "results": {
            "list": ["id"],
        }
    }
}]

try:
    res, blobs = db.query(query)
    total_elements = res[0]["FindImage"]["batch"]["total_elements"]
    print("Total elements that match criteria:", total_elements)
except Exception as e:
    print(db.get_last_response_str())
    raise e

images_per_round = 10
rounds = total_elements // images_per_round

classifier = alexnet.AlexNetClassifier()
with open(out_file_name, 'w') as classification:
    for i in range(rounds):
        # ApertureDB API doc:
        # https://docs.aperturedata.io/blocks/results.html#batch-examples
        query[0]["FindImage"]["batch"] = {
            "batch_size": images_per_round,
            "batch_id": i
        }
        try:
            start = time.time()
            res, blobs = db.query(query)
            print("\rRetrieval performance (imgs/s):",
                  len(blobs) / (time.time() - start), end="")
        except Exception as e:
            print(db.get_last_response_str())
            raise e

        for id, image in enumerate(blobs):
            image_id = res[0]["FindImage"]["entities"][id]["id"]
            label, conf = classifier.classify(image)
            classification.write(f"{image_id}: {label}, confidence = {conf}\n")

print(f"\nWritten classification results into {out_file_name}")
