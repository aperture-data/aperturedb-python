from aperturedb.PyTorchData import PyTorchData
from torchvision.datasets import CocoDetection
from PIL import Image
import io


def image_to_byte_array(image: Image) -> bytes:
    imgByteArr = io.BytesIO()
    image.save(imgByteArr, format="JPEG")
    imgByteArr = imgByteArr.getvalue()
    return imgByteArr


class CocoDataPytorch(PyTorchData):
    """
    **ApertureDB ingestable Dataset, which is sourced from
    [CocoDetection (torchvision.datasets)](https://pytorch.org/vision/main/generated/torchvision.datasets.CocoDetection.html#torchvision.datasets.CocoDetection)**
    """

    def __init__(self) -> None:
        """
        COCO dataset loads as an iterable with Tuple (X, [y1, y2 .... yn])
        where X is the image (PIL.Image) and y's are multiple dicts with properties like:
        area, bbox, category_id, image_id, id, iscrowd, keypoints, num_keypoints, segmentation
        """
        coco_detection = CocoDetection(
            root="coco/val2017",
            annFile="coco/annotations/person_keypoints_val2017.json")
        super().__init__(coco_detection)

    def generate_query(self, idx: int):
        item = self.loaded_dataset[idx]
        img_ref = (idx % 99998) + 1
        q = [{
            "AddImage": {
                "_ref": img_ref
            }
        }]
        blob = image_to_byte_array(item[0])
        if len(item[1]) > 0:
            meta_info = item[1][0]
            q[0]["AddImage"]["properties"] = {
                # Hack: Concatenate the list types as aperturedb does not support arrays for properties yet.
                p: " ".join(map(str, meta_info[p])) if isinstance(meta_info[p], list) else meta_info[p] for p in meta_info
            }
            # If bounding box is present, make an aperturedb object connected to the image
            if "bbox" in meta_info:
                bbox = meta_info["bbox"]
                q.append({
                    "AddBoundingBox": {
                        "image_ref": img_ref,
                        "rectangle": {
                            "x": int(bbox[0]),
                            "y": int(bbox[1]),
                            "width": int(bbox[2]),
                            "height": int(bbox[3])
                        },
                        "label": str(meta_info["category_id"])
                    }
                })

        return q, [blob]
