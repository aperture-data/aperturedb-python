import time
import requests
import boto3

import numpy as np
import cv2

from aperturedb import CSVParser
from aperturedb.EntityUpdateDataCSV import SingleEntityUpdateDataCSV
from aperturedb.BlobNewestDataCSV import BlobNewestDataCSV
from aperturedb.SparseAddingDataCSV import SparseAddingDataCSV
import logging
import os

logger = logging.getLogger(__name__)

HEADER_PATH   = "filename"
HEADER_URL    = "url"
HEADER_S3_URL = "s3_url"
HEADER_GS_URL = "gs_url"
PROPERTIES    = "properties"
CONSTRAINTS   = "constraints"
IMG_FORMAT    = "format"


class ImageDataProcessor():
    """
    **Processing for Image data, used when loading images**
    """

    def __init__(self, check_image, n_download_retries):
        self.loaders = [self.load_image, self.load_url,
                        self.load_s3_url, self.load_gs_url]
        self.source_types = [HEADER_PATH,
                             HEADER_URL, HEADER_S3_URL, HEADER_GS_URL]

        self.check_image = check_image
        self.n_download_retries = n_download_retries
        self.s3 = None

    def set_processor(self, use_dask, source_type):
        self.source_type = source_type
        if use_dask == False and self.source_type == HEADER_S3_URL:
            # The connections by boto3 cause ResourceWarning. Known
            # issue: https://github.com/boto/boto3/issues/454
            self.s3 = boto3.client('s3')

    def get_indices(self):
        return {
            "entity": {
                "_Image": self.get_indexed_properties()
            }
        }

    def load_image(self, filename):

        if self.check_image:
            try:
                a = cv2.imread(filename)
                if a.size <= 0:
                    logger.error(f"IMAGE SIZE ERROR: {filename}")
                    return False, None
            except Exception as e:
                logger.error(f"IMAGE ERROR: {filename}")
                logger.exception(e)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except Exception as e:
            logger.error(f"IMAGE ERROR: {filename}")
            logger.exception(e)
        return False, None

    def check_image_buffer(self, img):

        if not self.check_image:
            return True

        try:
            # This is a full decoding of the image, a compute intensive operation.
            # decoded_img = cv2.imdecode(img, cv2.IMREAD_COLOR)
            # if decoded_img is None:
            #     logger.error("Error decoding image")
            #     print(img[0], img[1], img[2], img[3], img[4], img[5], img[6], img[7])
            #     return False

            # Instead, we do a much lighter check on the header of the image,
            # which is already in memory.
            # Check header signature for file format:
            # https://en.wikipedia.org/wiki/List_of_file_signatures

            # NOTE: Make sure we are checking for the same values
            #       as Image.cc on ApertureDB
            # Ideally, we don't want this logic here because it is replicated
            # on the server side, and we have to maintain it in both places.
            # But because it is expensive to send the image to the server
            # only to find out it is bad, we do it here.

            # JPEG
            if img[0] == 255 and img[1] == 216:
                return True
            # PNG
            if img[0] == 137 and img[1] == 80 and img[2] == 78 and img[3] == 71:
                return True
            # TIF
            if img[0] == 73 and img[1] == 73 and img[2] == 42:
                return True

        except Exception as e:
            logger.exception(e)
            return False

        return False  # When header is not recognized

    def load_url(self, url):
        retries = 0
        while True:
            imgdata = requests.get(url)
            if imgdata.ok:
                imgbuffer = np.frombuffer(imgdata.content, dtype='uint8')
                if not self.check_image_buffer(imgbuffer):
                    logger.error(f"IMAGE ERROR: {url}")
                    return False, None

                return imgdata.ok, imgdata.content
            else:
                if retries >= self.n_download_retries:
                    break
                logger.warning("Retrying object:", url)
                retries += 1
                time.sleep(2)

        return False, None

    def load_s3_url(self, s3_url):
        retries = 0

        while True:
            try:
                bucket_name = s3_url.split("/")[2]
                object_name = s3_url.split("s3://" + bucket_name + "/")[-1]
                s3_response_object = self.s3.get_object(
                    Bucket=bucket_name, Key=object_name)
                img = s3_response_object['Body'].read()
                imgbuffer = np.frombuffer(img, dtype='uint8')
                if not self.check_image_buffer(imgbuffer):
                    logger.error(f"IMAGE ERROR:{s3_url} ")
                    return False, None

                return True, img
            except Exception as e:
                if retries >= self.n_download_retries:
                    break
                logger.warning(f"Retrying object: {s3_url}")
                retries += 1
                time.sleep(2)

        logger.error(f"S3 ERROR: {s3_url}")
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
                if not self.check_image_buffer(imgbuffer):
                    logger.warning(f"IMAGE ERROR: {gs_url}")
                    return False, None
                return True, blob
            except:
                if retries >= self.n_download_retries:
                    break
                logger.warning("Retrying object: {gs_url}")
                retries += 1
                time.sleep(2)

        logger.error(f"GS ERROR: {gs_url}")
        return False, None


class ImageDataCSV(CSVParser.CSVParser, ImageDataProcessor):
    """**ApertureDB Image Data.**

    This class loads the Image Data which is present in a csv file,
    and converts it into a series of aperturedb queries.


    :::note Is backed by a csv file with the following columns:

    ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

    OR

    ``url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

    OR

    ``s3_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

    OR

    ``gs_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``
    :::

    Example csv file::

        filename,id,label,constraint_id,format
        /home/user/file1.jpg,321423532,dog,321423532,jpg
        /home/user/file2.jpg,42342522,cat,42342522,png
        ...

    Example usage:

    ``` python

        data = ImageDataCSV("/path/to/ImageData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)
    ```


    :::info
    In the above example, the constraint_id ensures that an Image with the specified
    id would be only inserted if it does not already exist in the database.
    :::
    """

    def __init__(self, filename, check_image=True, n_download_retries=3, **kwargs):

        ImageDataProcessor.__init__(
            self, check_image, n_download_retries)
        CSVParser.CSVParser.__init__(self, filename, **kwargs)

        source_type = self.header[0]
        self.set_processor(self.use_dask, source_type)

        self.format_given     = IMG_FORMAT in self.header
        self.props_keys       = [x for x in self.header[1:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.props_keys       = [
            x for x in self.props_keys if x != IMG_FORMAT]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

        if self.source_type not in self.source_types:
            logger.error("Source not recognized: " + self.source_type)
            raise Exception("Error loading image: " + filename)
        self.source_loader    = {
            st: sl for st, sl in zip(self.source_types, self.loaders)
        }

        self.relative_path_prefix = os.path.dirname(self.filename) \
            if self.source_type == HEADER_PATH and self.blobs_relative_to_csv else ""

        self.command = "AddImage"

    def getitem(self, idx):
        idx = self.df.index.start + idx

        image_path = os.path.join(
            self.relative_path_prefix, self.df.loc[idx, self.source_type])
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

    def get_indices(self):
        return {
            "entity": {
                "_Image": self.get_indexed_properties()
            }
        }

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] not in self.source_types:
            raise Exception(
                f"Error with CSV file field: {self.header[0]}. Must be first field")


class ImageUpdateDataCSV(SingleEntityUpdateDataCSV, ImageDataProcessor):
    """
    **ApertureDB Image CSV Parser for Adding an Image and updating the properties on the image.**
    Usage is in EntityUpdateDataCSV.

    Note that this class will not change a blob in an existing entity. If looking to change a blob in
     an existing entity, look at ImageForceNewestDataCSV, but be careful of all caveats.

    """

    def __init__(self, filename, check_image=True, n_download_retries=3, **kwargs):
        ImageDataProcessor.__init__(
            self, check_image, n_download_retries)
        SingleEntityUpdateDataCSV.__init__(
            self, "Image", filename, **kwargs)

        source_type = self.header[0]
        self.set_processor(self.use_dask, source_type)

        # this class loads a blob, so must set the first query to have a blob.
        self.blobs_per_query = [1, 0]
        self.format_given     = IMG_FORMAT in self.header
        self.props_keys = list(filter(lambda prop: prop not in [
                               IMG_FORMAT, "filename"], self.props_keys))
        if self.source_type not in self.source_types:
            logger.error("Source not recognized: " + self.source_type)
            raise Exception("Error loading image: " + filename)
        self.source_loader    = {
            st: sl for st, sl in zip(self.source_types, self.loaders)
        }

    def getitem(self, idx):
        blob_set = []
        [query_set, empty_blobs] = super().getitem(idx)

        image_path = os.path.join(
            self.relative_path_prefix, self.df.loc[idx, self.source_type])
        img_ok, img = self.source_loader[self.source_type](image_path)
        if not img_ok:
            logger.error("Error loading image: " + image_path)
            raise Exception("Error loading image: " + image_path)
        # element has 2 queries, each with 1 blob.
        blob_set = [[img], []]
        # must wrap the blob return for this item in a list
        return [query_set, [blob_set]]

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] not in self.source_types:
            raise Exception(
                f"Error with CSV file field: {self.header[0]}. Must be first field")
        else:
            SingleEntityUpdateDataCSV.validate(self)


class ImageForceNewestDataCSV(BlobNewestDataCSV, ImageDataProcessor):
    """
    **ApertureDB Image CSV Parser for Maintaining a Blob set with changing blob data.**
    See BlobNewestDataCSV for usage.
    """

    def __init__(self, filename, check_image=True, n_download_retries=3, **kwargs):
        ImageDataProcessor.__init__(
            self, check_image, n_download_retries)
        BlobNewestDataCSV.__init__(
            self, "Image", filename, **kwargs)

        source_type = self.header[0]
        self.set_processor(self.use_dask, source_type)

        self.format_given     = IMG_FORMAT in self.header
        self.props_keys = list(filter(lambda prop: prop not in [
                               IMG_FORMAT, "filename"], self.props_keys))
        if self.source_type not in self.source_types:
            logger.error("Source not recognized: " + self.source_type)
            raise Exception("Error loading image: " + filename)
        self.source_loader    = {
            st: sl for st, sl in zip(self.source_types, self.loaders)
        }

    def read_blob(self, idx):
        relative_path_prefix = os.path.dirname(self.filename) \
            if self.source_type == HEADER_PATH else ""
        image_path = os.path.join(
            relative_path_prefix, self.df.loc[idx, self.source_type])
        img_ok, img = self.source_loader[self.source_type](image_path)
        if not img_ok:
            logger.error("Error loading image: " + image_path)
            raise Exception("Error loading image: " + image_path)
        return img


class ImageSparseAddDataCSV(SparseAddingDataCSV, ImageDataProcessor):
    """
    **ApertureDB Spare Loading Image Data.**

    ImageSparseAddDataCSV should be used the same as ImageDataCSV.

    See SparseAddingDataCSV for description of when to use this versus ImageDataCSV.


    Example csv file::

        filename,id,label,constraint_id,format
        /home/user/file1.jpg,321423532,dog,321423532,jpg
        /home/user/file2.jpg,42342522,cat,42342522,png
        ...

    Example usage:

    ```python

        data = ImageSparseAddDataCSV("/path/to/ImageData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)
    ```
    """

    def __init__(self, filename, check_image=True, n_download_retries=3, **kwargs):
        ImageDataProcessor.__init__(
            self, check_image, n_download_retries)
        SparseAddingDataCSV.__init__(self, "Image", filename, **kwargs)
        source_type = self.header[0]
        self.set_processor(self.use_dask, source_type)

        self.format_given     = IMG_FORMAT in self.header
        self.props_keys = list(filter(lambda prop: prop not in [
                               IMG_FORMAT, "filename"], self.props_keys))
        if self.source_type not in self.source_types:
            logger.error("Source not recognized: " + self.source_type)
            raise Exception("Error loading image: " + filename)
        self.source_loader    = {
            st: sl for st, sl in zip(self.source_types, self.loaders)
        }

    def getitem(self, idx):
        blob_set = []
        [query_set, empty_blobs] = super().getitem(idx)

        image_path = os.path.join(
            self.relative_path_prefix, self.df.loc[idx, self.source_type])
        img_ok, img = self.source_loader[self.source_type](image_path)

        if not img_ok:
            logger.error("Error loading image: " + image_path)
            raise Exception("Error loading image: " + image_path)
        # element has 2 queries, only second has blob
        blob_set = [[], [img]]
        # must wrap the blob return for this item in a list
        return [query_set, [blob_set]]
