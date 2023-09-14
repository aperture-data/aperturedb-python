import os
import stat
import errno
import fuse
from fuse import Fuse

from aperturedb.Images import Images
import logging
from threading import Lock
import json

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

    # Hardcoded for now
    meta_categories = [{
        "id": 1,
        "name": "celebreties",
        "supercategory": "people"
    }, {
        "id": 2,
        "name": "Face points",
        "keypoints": ["lefteye", "righteyee", "nose", "leftmouth", "rightmouth"],
        "skeleton": [[0, 1, 3, 4], [2, 3, 4]]
    }]

    for i, p in enumerate(images.get_props_names()):
        meta_categories.append({
            "id": 3 + i,
            "name": p,
            "supercategory": "classes"
        })

        meta_images = [{
            "id": id,
            "licensce": 1,
            "file_name": f"{id}.jpg",
            "height": 218,
            "width": 178
        } for ind, id in enumerate(images.images_ids)]

        # logger.debug(self._images.images_bboxes)

        meta_annotations = [
            {
                "id": 2 * ind,
                "image_id": info,
                "category_id": 1,
                "bbox": [
                    images.images_bboxes[str(info)]['bboxes'][0]['x'],
                    images.images_bboxes[str(info)]['bboxes'][0]['y'],
                    images.images_bboxes[str(
                        info)]['bboxes'][0]['width'],
                    images.images_bboxes[str(
                        info)]['bboxes'][0]['height'],
                ],
                "segmentation": [],
                "tags": [k for k in properties[str(info)] if properties[str(info)][k] == 1]
            } for ind, info in enumerate(images.images_ids)
        ]

        for i, p in enumerate(properties):
            ckps = []
            kps = properties[p]["keypoints"].split(" ")[1:]
            for i in range(0, len(kps), 2):
                ckps.append(float(kps[i]))
                ckps.append(float(kps[i + 1]))
                ckps.append(2)
            meta_annotations.append(
                {
                    "id": 2 * i + 1,
                    "image_id": p,
                    "keypoints": ckps,
                    "num_keypoints": 5,
                    "category_id": 2
                }
            )

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
            except Exception as e:
                print(images.images_bboxes)
                raise

        self.meta_data, self.properties = generate_coco_meta_data(images)

        logger.info(f"Meta data generated with length = {len(self.meta_data)}")

    def getattr(self, path):
        st = ADStat()
        logger.info(f"getattr: {path}")
        if path == '/':
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        elif path == f"/data":
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        else:
            logger.info(f"stats = {self.stats}")
            st.st_mode = stat.S_IFREG | 0o444
            st.st_nlink = 1
            st.st_size = self.stats[path]
        return st

    def readdir(self, path, offset):
        logger.debug(f"{path}")
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
                             self.properties[iid]["image_size"]))

            direntries = [('.', 4096), ('..', 4096)] + [f for f in filegen]
        print(direntries)
        logger.info(f"{direntries}")
        for r in direntries:
            self.stats[os.path.join(path, r[0])] = r[1]
            yield fuse.Direntry(r[0])

    def open(self, path, flags):
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        logger.debug(f"path = {path}, size = {size}, offset = {offset}")
        filename = os.path.basename(path)
        logger.debug(f"Filename = {filename}")
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


def mount_images_from_aperturdb(images: Images):
    server = ADFS(
        version="%prog " + fuse.__version__,
        usage=fuse.Fuse.fusage,
        dash_s_do='setsingle',
        images=images)
    server.parse(errex=1)
    server.main()
