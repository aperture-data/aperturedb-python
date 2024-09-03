from aperturedb.PyTorchData import PyTorchData
from torchvision.datasets import CocoDetection
from aperturedb.Images import image_to_bytes
import cv2


def polygonFromMask(maskedArr):
    # adapted from https://github.com/hazirbas/coco-json-converter/blob/master/generate_coco_json.py
    contours, _ = cv2.findContours(
        maskedArr, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    segmentation = []
    valid_poly = 0
    for contour in contours:
        # Valid polygons have >= 6 coordinates (3 points)
        if contour.size >= 6:
            segmentation.append(contour.astype(float).flatten().tolist())
            valid_poly += 1
    if valid_poly == 0:
        raise ValueError
    return segmentation


class CocoDataPyTorch(PyTorchData):
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
            root="/mnt/data/datasets/coco/data/images/val2017/",
            annFile="/mnt/data/datasets/coco/data/annotations/stuff_val2017.json")
        self.coco_detection = coco_detection
        super().__init__(coco_detection)

    def generate_query(self, idx: int):
        item = self.loaded_dataset[idx]
        q = [{
            "AddImage": {
                "_ref": 2
            }
        }]
        blob = image_to_bytes(item[0])
        if len(item[1]) > 0:
            meta_info = item[1][0]
            category_id = meta_info["category_id"]
            category_info = self.coco_detection.coco.loadCats(category_id)[0]

            q[0]["AddImage"]["properties"] = {
                # Hack: Concatenate the list types as aperturedb does not support arrays for properties yet.
                p: " ".join(map(str, meta_info[p])) if isinstance(meta_info[p], list) else meta_info[p] for p in meta_info
            }

            q.insert(0, {
                "AddEntity": {
                    "_ref": 1,
                    "class": "SuperCategory",
                    "if_not_found": {
                        "name": ["==", category_info["supercategory"]]
                    },
                    "properties": {
                        "name": category_info["supercategory"]
                    }
                }
            })

            # If bounding box is present, make an aperturedb object connected to the image
            if "bbox" in meta_info:
                bbox = meta_info["bbox"]
                q.append({
                    "AddBoundingBox": {
                        "image_ref": 2,
                        "rectangle": {
                            "x": int(bbox[0]),
                            "y": int(bbox[1]),
                            "width": int(bbox[2]),
                            "height": int(bbox[3])
                        },
                        "label": str(category_info["name"])
                    }
                })

            if "segmentation" in meta_info:
                # Convert RLE to polygons in adb
                # https://github.com/cocodataset/cocoapi/issues/476
                m = self.coco_detection.coco.annToMask(meta_info)
                polygons = polygonFromMask(m)
                adb_polygons = []
                for polygon in polygons:
                    adb_polygon = []
                    for i in range(0, len(polygon), 2):
                        adb_polygon.append([polygon[i], polygon[i + 1]])
                    adb_polygons.append(adb_polygon)
                for adb_polygon in adb_polygons:
                    q.append({
                        "AddPolygon": {
                            "image_ref": 2,
                            "label": str(category_info["name"]),
                            "polygons": [adb_polygon]
                        }
                    })

        return q, [blob]
