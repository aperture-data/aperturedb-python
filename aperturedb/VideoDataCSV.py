import cv2
import logging
import os
from aperturedb import CSVParser
from aperturedb.Sources import Sources
import boto3


logger = logging.getLogger(__name__)

HEADER_PATH = "filename"
PROPERTIES = "properties"
CONSTRAINTS = "constraints"
HEADER_URL = "url"
HEADER_S3_URL = "s3_url"
HEADER_GS_URL = "gs_url"


class VideoDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB Video Data.**

    This class loads the Video Data which is present in a CSV file,
    and converts it into a series of ApertureDB queries.


    :::note Is backed by a CSV file with the following columns:

    ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

    OR

    ``url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

    OR

    ``s3_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``

    OR

    ``gs_url``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``, ``format``
    :::

    Example CSV file::

        filename,id,label,constraint_id
        /home/user/file1.mp4,321423532,dog,321423532
        /home/user/file2.mp4,42342522,cat,4234252
        ...

    Example usage:

    ``` python

        data = VideoDataCSV("/path/to/VideoData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```

    or

    ``` bash

        adb ingest from-csv /path/to/VideoData.csv --ingest-type VIDEO
    ```


    :::info
    In the above example, the constraint_id ensures that an Video with the specified
    id would be only inserted if it does not already exist in the database.
    :::
    """

    def __init__(self, filename: str, check_video: bool = True, **kwargs):
        self.source_types = [HEADER_PATH,
                             HEADER_URL, HEADER_S3_URL, HEADER_GS_URL]
        super().__init__(filename, **kwargs)
        self.check_video = check_video

        self.props_keys = [x for x in self.header[1:]
                           if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.command = "AddVideo"
        self.loaders = {
            HEADER_PATH: self.load_video,
            HEADER_URL: self.load_url,
            HEADER_S3_URL: self.load_s3_url,
            HEADER_GS_URL: self.load_gs_url
        }
        self.source_type = self.header[0]
        if self.use_dask == False and self.source_type == HEADER_S3_URL:
            s3_client = boto3.client('s3')
            self.sources = Sources(3, s3_client=s3_client)
        else:
            self.sources = Sources(3)

    def get_indices(self):
        return {
            "entity": {
                "_Video": self.get_indexed_properties()
            }
        }

    def check_video_buf(self, video) -> bool:
        # check if video is valid
        print(len(video))
        return True

    def getitem(self, idx):
        self.relative_path_prefix = os.path.dirname(self.filename) \
            if self.source_type == HEADER_PATH and self.blobs_relative_to_csv else ""
        uri = os.path.join(self.relative_path_prefix,
                           self.df.loc[idx, self.source_type])
        video_ok, video = self.loaders[self.source_type](uri)

        if not video_ok:
            logger.error("Error loading video: " + uri)
            raise Exception("Error loading video: " + uri)

        q = []
        blobs = []
        av = self._basic_command(idx)
        blobs.append(video)
        q.append(av)

        return q, blobs

    def load_video(self, filename):

        if self.check_video:
            try:
                a = cv2.VideoCapture(filename)
                if a.isOpened() == False:
                    logger.error(f"Video reading Error: {filename}")
            except Exception as e:
                logger.error(f"Video Error: {filename}")
                logger.exception(e)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except Exception as e:
            logger.error(f"Video Error: {filename}")
            logger.exception(e)

        return False, None

    def load_url(self, url):
        return self.sources.load_from_http_url(url, self.check_video_buf)

    def load_s3_url(self, s3_url):
        return self.sources.load_from_s3_url(s3_url, self.check_video_buf)

    def load_gs_url(self, gs_url):
        return self.sources.load_from_gs_url(gs_url, self.check_video_buf)

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] not in self.source_types:
            field = self.header[0]
            allowed = ", ".join(self.source_types)
            raise Exception(
                f"Error with CSV file field: {field}. Allowed values: {allowed}")
