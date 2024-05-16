import os
import stat
import errno
import fuse
from fuse import Fuse

from aperturedb.Images import Images
import logging
from threading import Lock
import json
from tqdm import tqdm

logger = logging.getLogger(__name__)

if not hasattr(fuse, '__version__'):
    raise RuntimeError(
        "your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

meta_info = 'labels.json'


def generate_coco_meta_data(images: Images):
    """
    This function generates the meta data for the COCO dataset
    details can be found here: https://cocodataset.org/#format-data
    """
    properties = images.get_properties(images.get_props_names())

    # Hardcoded for now
    meta_licenses = [
        {
            "id": 1,
            "name": "Attribution-NonCommercial-ShareAlike License",
            "url": "http://creativecommons.org/licenses/by-nc-sa/2.0/",
        }
    ]

    meta_categories = [{
        "id": 1,
        "name": "unknown",
        "supercategory": "labels"
    }, {
        # Hardcoded for now
        "id": 2,
        "name": "Face points",
        "keypoints": ["lefteye", "righteyee", "nose", "leftmouth", "rightmouth"],
        "skeleton": [[0, 1, 3, 4], [2, 3, 4]]
    }
    ]

    meta_images = [{
        "id": id,
        "licensce": 1,
        "file_name": f"{id}.jpg",
        "height": properties[id]["adb_image_height"],
        "width": properties[id]["adb_image_width"],
    } for ind, id in enumerate(images.images_ids)]

    # Add attached bounding boxes
    categories = []
    meta_annotations = []

    for ind, image_id in tqdm(enumerate(images.images_ids)):
        if image_id in images.images_bboxes:
            for bidx, box in enumerate(images.images_bboxes[image_id]['bboxes']):
                annotation = {
                    "id": 2 * bidx,
                    "image_id": image_id,
                    "category_id": 1,
                    "bbox": [
                        int(box[0][0]),
                        int(box[0][1]),
                        int(box[1][0] - box[0][0]),
                        int(box[2][1] - box[0][1])
                    ]
                }
                meta_annotations.append(annotation)
                label = images.images_bboxes[image_id]['tags'][bidx]
                if label not in categories:
                    categories.append(label)

                # Figure the category id (offset by 1 for the unknown category)
                annotation["category_id"] = categories.index(label) + 3

        if image_id in images.images_polygons:
            for bidx, seg in enumerate(images.images_polygons[image_id]['polygons']):
                xs = [x for coords in seg[0]
                      for i, x in enumerate(coords) if i % 2 == 0]
                ys = [y for coords in seg[0]
                      for i, y in enumerate(coords) if i % 2 == 1]
                annotation = {
                    "id": 2 * bidx + 1,
                    "image_id": image_id,
                    "category_id": 1,
                    "segmentation": [[x for coords in seg[0] for x in coords]],
                    "tags": [k for k in properties[image_id] if properties[image_id][k] == 1],
                    "bbox": [
                        int(min(xs)),
                        int(min(ys)),
                        int(max(xs) - min(xs)),
                        int(max(ys) - min(ys))
                    ]
                }
                meta_annotations.append(annotation)
                label = images.images_polygons[image_id]['tags'][bidx]
                if label not in categories:
                    categories.append(label)

                # Figure the category id (offset by 1 for the unknown category)
                annotation["category_id"] = categories.index(label) + 3

        # Add attached keypoints. This is very DS specific, till we figure out
        # how to make this generic
        if "keypoints" in properties[image_id] and properties[image_id]["keypoints"] != None:
            ckps = []
            kps = properties[image_id]["keypoints"].split(" ")[1:]
            for i in range(0, len(kps), 2):
                ckps.append(float(kps[i]))
                ckps.append(float(kps[i + 1]))
                ckps.append(2)
            meta_annotations.append(
                {
                    "id": 2 * ind + 1,
                    "image_id": image_id,
                    "keypoints": ckps,
                    "num_keypoints": 5,
                    "category_id": 2
                }
            )

    for i, c in enumerate(categories):
        meta_categories.append({
            "id": i + 3,
            "name": c,
            "supercategory": "labels"
        })

    meta_data = bytes(json.dumps({
        "licenses": meta_licenses,
        "categories": meta_categories,
        "images": meta_images,
        "annotations": meta_annotations
    }, indent=2), 'utf-8')

    return meta_data, properties


class ADStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class ADFS(Fuse):
    def __init__(self, images: Images, *args, **kw):
        super().__init__(*args, **kw)
        self._images = images
        self.iolock = Lock()
        self.stats = {}

        for i, img in enumerate(images.images_ids):
            logger.debug(img)
            try:
                images.get_bboxes_by_index(i)
                images.retrieve_polygons(i)
            except Exception as e:
                print(images.images_bboxes)
                raise

        self.meta_data, self.properties = generate_coco_meta_data(images)
        logger.info(f"Meta data generated with length = {len(self.meta_data)}")

    def getattr(self, path):
        logger.info(f"getattr: {path=}")
        st = ADStat()
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        elif path == f"/data":
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        elif path == f"/{meta_info}":
            st.st_mode = stat.S_IFREG | 0o444
            st.st_nlink = 1
            st.st_size = len(self.meta_data)
        else:
            st.st_mode = stat.S_IFREG | 0o444
            st.st_nlink = 1
            st.st_size = self.stats[path]
        logger.info(
            f"getattr: {path=}, {st.st_mode=}, {st.st_nlink=}, {st.st_size=}")
        return st

    def readdir(self, path, offset):
        if path == '/':
            direntries = [
                ('.', 0),
                ('..', 4096),
                ('data', 4096),
                (meta_info, len(self.meta_data))
            ]
        elif path == '/data':
            filegen = set()
            for iid in self.properties:
                filegen.add((f"{iid}.jpg",
                             self.properties[iid]["adb_image_size"]))

            direntries = [('.', 4096), ('..', 4096)] + [f for f in filegen]

        logger.info(f"readdir: {path=}, {len(direntries)=}")
        for r in direntries:
            self.stats[os.path.join(path, r[0])] = r[1]
            yield fuse.Direntry(r[0])

    def open(self, path, flags):
        logger.info(f"path = {path}, flags = {flags}")
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        logger.info(f"path = {path}, size = {size}, offset = {offset}")
        filename = os.path.basename(path)
        logger.info(f"Filename = {filename}")
        if filename == meta_info:
            try:
                img = self.meta_data
                slen = len(img)
            except Exception as e:
                logger.exception(e)
        else:
            try:
                image_id = filename[:-4]
                idx = self._images.images_ids.index(image_id)
                logger.info(f"Image id = {image_id}, index = {idx}")
                self.iolock.acquire()
                img = self._images.get_image_by_index(idx)
                self.iolock.release()
                slen = len(img)
                logger.debug(f"Type = {type(img)}, len = {slen}")
            except Exception as e:
                logger.exception(e)
                logger.error("Error occured")

        if offset < slen:
            if offset + size > slen:
                size = slen - offset
            buf = img[offset:offset + size]
        else:
            buf = b''
        return buf


def mount_images_from_aperturedb(images: Images):
    server = ADFS(
        version="%prog " + fuse.__version__,
        usage=fuse.Fuse.fusage,
        dash_s_do='setsingle',
        images=images)
    server.parse(errex=1)
    server.main()
