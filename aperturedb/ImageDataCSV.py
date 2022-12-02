import time
import requests
import boto3

import numpy as np
import cv2

from aperturedb import CSVParser
import logging

logger = logging.getLogger(__name__)

HEADER_PATH   = "filename"
HEADER_URL    = "url"
HEADER_S3_URL = "s3_url"
HEADER_GS_URL = "gs_url"
PROPERTIES    = "properties"
CONSTRAINTS   = "constraints"
IMG_FORMAT    = "format"


class ImageDataCSV(CSVParser.CSVParser):
    """**ApertureDB Image Data.**

    This class loads the Image Data which is present in a csv file,
    and converts it into a series of aperturedb queries.


    .. note::
        Is backed by a csv file with the following columns (format optional):

            ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

            OR

            ``url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

            OR

            ``s3_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

            OR

            ``gs_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``
            ...

    Example csv file::

        filename,id,label,constraint_id,format
        /home/user/file1.jpg,321423532,dog,321423532,jpg
        /home/user/file2.jpg,42342522,cat,42342522,png
        ...

    Example usage:

    .. code-block:: python

        data = ImageDataCSV("/path/to/ImageData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)


    .. important::
        In the above example, the constraint_id ensures that an Image with the specified
        id would be only inserted if it does not already exist in the database.


    """

    def __init__(self, filename, check_image=True, n_download_retries=3, df=None, use_dask=False):
        self.loaders = [self.load_image, self.load_url,
                        self.load_s3_url, self.load_gs_url]
        self.source_types = [HEADER_PATH,
                             HEADER_URL, HEADER_S3_URL, HEADER_GS_URL]

        super().__init__(filename, df=df, use_dask=use_dask)

        self.check_image = check_image
        if not use_dask:
            self.format_given     = IMG_FORMAT in self.header
            self.props_keys       = [x for x in self.header[1:]
                                     if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.props_keys       = [
                x for x in self.props_keys if x != IMG_FORMAT]
            self.constraints_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

            self.source_type      = self.header[0]

            if self.source_type not in self.source_types:
                logger.error("Source not recognized: " + self.source_type)
                raise Exception("Error loading image: " + filename)
            self.source_loader    = {
                st: sl for st, sl in zip(self.source_types, self.loaders)
            }

            self.n_download_retries = n_download_retries
            self.command = "AddImage"

    def getitem(self, idx):
        idx = self.df.index.start + idx
        image_path = self.df.loc[idx, self.source_type]
        img_ok, img = self.source_loader[self.source_type](image_path)

        if not img_ok:
            logger.error("Error loading image: " + image_path)
            raise Exception("Error loading image: " + image_path)

        q = []
        blobs = []
        custom_fields = {}
        if self.format_given:
            custom_fields["format"] = self.df.loc[idx, IMG_FORMAT]
        ai = self._basic_command(idx, custom_fields)
        blobs.append(img)
        q.append(ai)

        return q, blobs

    def load_image(self, filename):
        if self.check_image:
            try:
                a = cv2.imread(filename)
                if a.size <= 0:
                    logger.error("IMAGE SIZE ERROR:", filename)
                    return False, None
            except Exception as e:
                logger.error("IMAGE ERROR:", filename)
                logger.exception(e)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except Exception as e:
            logger.error("IMAGE ERROR:", filename)
            logger.exception(e)
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
                    logger.error("IMAGE ERROR: ", url)
                    return False, None

                return imgdata.ok, imgdata.content
            else:
                if retries >= self.n_download_retries:
                    break
                logger.warning("WARNING: Retrying object:", url)
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
                    logger.error("IMAGE ERROR: ", s3_url)
                    return False, None

                return True, img
            except:
                if retries >= self.n_download_retries:
                    break
                logger.warning("WARNING: Retrying object:", s3_url)
                retries += 1
                time.sleep(2)

        logger.error("S3 ERROR:", s3_url)
        return False, None

    def load_gs_url(self, gs_url):
        retries = 0
        from google.cloud import storage
        client = storage.Client()
        while True:
            try:
                bucket_name = gs_url.split("/")[2]
                object_name = gs_url.split("gs://" + bucket_name + "/")[-1]

                blob = client.bucket(bucket_name).blob(
                    object_name).download_as_bytes()
                imgbuffer = np.frombuffer(blob, dtype='uint8')
                if self.check_image and not self.check_image_buffer(imgbuffer):
                    logger.error("IMAGE ERROR: ", gs_url)
                    return False, None
                return True, blob
            except:
                if retries >= self.n_download_retries:
                    break
                logger.warning("WARNING: Retrying object:", gs_url)
                retries += 1
                time.sleep(2)

        logger.error("GS ERROR:", gs_url)
        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] not in self.source_types:
            raise Exception(
                f"Error with CSV file field: {self.header[0]}. Must be first field")
