import cv2
import logging
from aperturedb import CSVParser

logger = logging.getLogger(__name__)

HEADER_PATH = "filename"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"


class VideoDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB Video Data.**

    This class loads the Video Data which is present in a csv file,
    and converts it into a series of aperturedb queries.


    .. note::

        Expects a csv file with the following columns:

            ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        filename,id,label,constaint_id
        /home/user/file1.jpg,321423532,dog,321423532
        /home/user/file2.jpg,42342522,cat,4234252
        ...

    Example usage:

    .. code-block:: python

        data = ImageDataCSV("/path/to/VideoData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)
    """

    def __init__(self, filename, check_video=True):

        super().__init__(filename)

        self.check_video = check_video

        self.props_keys       = [x for x in self.header[1:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.command = "AddVideo"

    def getitem(self, idx):
        filename   = self.df.loc[idx, HEADER_PATH]
        video_ok, video = self.load_video(filename)

        if not video_ok:
            logger.error("Error loading video: " + filename)
            raise Exception("Error loading video: " + filename)

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
                    logger.error("Video reading Error:", filename)
            except Exception as e:
                logger.error("Video Error:", filename)
                logger.exception(e)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except Exception as e:
            logger.error("Video Error:", filename)
            logger.exception(e)

        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != HEADER_PATH:
            raise Exception(
                "Error with CSV file field: filename. Must be first field")
