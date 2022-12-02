import time
import requests
import boto3

import numpy as np
import cv2

from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_PATH   = "filename"
HEADER_URL    = "url"
HEADER_S3_URL = "s3_url"
PROPERTIES    = "properties"
CONSTRAINTS   = "constraints"
IMG_FORMAT    = "format"


class ImageGeneratorCSV(CSVParser.CSVParser):
    """**ApertureDB Image Data generator.**

    .. warning::
        Deprecated. Use :class:`~aperturedb.ImageDataCSV.ImageDataCSV` instead.

    .. note::
        Is backed by a csv file with the following columns (format optional):

            ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

            OR

            ``url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

            OR

            ``s3_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``
            ...

    Example csv file::

        filename,id,label,constaint_id,format
        /home/user/file1.jpg,321423532,dog,321423532,jpg
        /home/user/file2.jpg,42342522,cat,42342522,png
        ...


    """

    def __init__(self, filename, check_image=True, n_download_retries=3):

        super().__init__(filename)

        self.check_image = check_image

        self.format_given     = IMG_FORMAT in self.header
        self.props_keys       = [x for x in self.header[1:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.props_keys       = [x for x in self.props_keys if x != IMG_FORMAT]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

        self.source_type      = self.header[0]
        if self.source_type not in [HEADER_PATH, HEADER_URL, HEADER_S3_URL]:
            print("Source not recognized: " + self.source_type)
            raise Exception("Error loading image: " + filename)

        self.n_download_retries = n_download_retries

    # TODO: we can add support for slicing here.
    def getitem(self, idx):

        data = {}

        img_ok = True
        img = None

        if self.source_type == HEADER_PATH:
            image_path   = self.df.loc[idx, HEADER_PATH]
            img_ok, img  = self.load_image(image_path)
        elif self.source_type == HEADER_URL:
            image_path   = self.df.loc[idx, HEADER_URL]
            img_ok, img  = self.load_url(image_path)
        elif self.source_type == HEADER_S3_URL:
            image_path   = self.df.loc[idx, HEADER_S3_URL]
            img_ok, img  = self.load_s3_url(image_path)

        if not img_ok:
            print("Error loading image: " + filename)
            raise Exception("Error loading image: " + filename)

        data["img_blob"] = img
        if self.format_given:
            data["format"] = self.df.loc[idx, IMG_FORMAT]

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

    def load_image(self, filename):
        if self.check_image:
            try:
                a = cv2.imread(filename)
                if a.size <= 0:
                    print("IMAGE SIZE ERROR:", filename)
                    return false, None
            except:
                print("IMAGE ERROR:", filename)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except:
            print("IMAGE ERROR:", filename)
        return False, None

    def check_image_buffer(self, img):
        try:
            decoded_img = cv2.imdecode(img, cv2.IMREAD_COLOR)

            # Check image is correct
            decoded_img = decoded_img if decoded_img is not None else img

            return True
        except:
            return False

    def load_url(self, url):
        retries = 0
        while True:
            imgdata = requests.get(url)
            if imgdata.ok:
                imgbuffer = np.frombuffer(imgdata.content, dtype='uint8')
                if self.check_image and not self.check_image_buffer(imgbuffer):
                    print("IMAGE ERROR: ", url)
                    return False, None

                return imgdata.ok, imgdata.content
            else:
                if retries >= self.n_download_retries:
                    break
                print("WARNING: Retrying object:", url)
                retries += 1
                time.sleep(2)

        return False, None

    def load_s3_url(self, s3_url):
        retries = 0

        # The connections by boto3 cause ResourceWarning. Known
        # issue: https://github.com/boto/boto3/issues/454
        s3 = boto3.client('s3')

        while True:
            try:
                bucket_name = s3_url.split("/")[2]
                object_name = s3_url.split("s3://" + bucket_name + "/")[-1]
                s3_response_object = s3.get_object(
                    Bucket=bucket_name, Key=object_name)
                img = s3_response_object['Body'].read()
                imgbuffer = np.frombuffer(img, dtype='uint8')
                if self.check_image and not self.check_image_buffer(imgbuffer):
                    print("IMAGE ERROR: ", s3_url)
                    return False, None

                return True, img
            except:
                if retries >= self.n_download_retries:
                    break
                print("WARNING: Retrying object:", s3_url)
                retries += 1
                time.sleep(2)

        print("S3 ERROR:", s3_url)
        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] not in [HEADER_PATH, HEADER_URL, HEADER_S3_URL]:
            raise Exception(
                "Error with CSV file field: filename. Must be first field")


class ImageLoader(ParallelLoader.ParallelLoader):
    """
    **ApertureDB Image Loader.**

    This class is to be used in combination with a **generator** object,
    for example :class:`~aperturedb.ImageLoader.ImageGeneratorCSV`,
    which is a class that implements iterable inteface and generates "image data" elements.

    Example::

        image_data = {
            "properties":  properties,
            "constraints": constraints,
            "operations":  operations,
            "format":      format ("jpg", "png", etc),
            "img_blob":    (bytes),
        }
    """

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "image"

    def generate_batch(self, image_data):

        q = []
        blobs = []

        for data in image_data:

            ai = {
                "AddImage": {
                }
            }

            if "properties" in data:
                ai["AddImage"]["properties"] = data["properties"]
            if "constraints" in data:
                ai["AddImage"]["if_not_found"] = data["constraints"]
            if "operations" in data:
                ai["AddImage"]["operations"] = data["operations"]
            if "format" in data:
                ai["AddImage"]["format"] = data["format"]

            if "img_blob" not in data or len(data["img_blob"]) == 0:
                print("WARNING: Skipping empty image.")
                continue

            blobs.append(data["img_blob"])
            q.append(ai)

        return q, blobs
