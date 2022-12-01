import cv2

from aperturedb import ParallelLoader
from aperturedb import CSVParser

HEADER_PATH = "filename"
PROPERTIES  = "properties"
CONSTRAINTS = "constraints"


class VideoGeneratorCSV(CSVParser.CSVParser):
    """
    **ApertureDB Video Data loader.**

    .. warning::
        Deprecated. Use :class:`~aperturedb.VideoDataCSV.VideoDataCSV` instead.


    .. important::

        Expects a csv file with the following columns:

            ``filename``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        filename,id,label,constaint_id
        /home/user/file1.jpg,321423532,dog,321423532
        /home/user/file2.jpg,42342522,cat,4234252
        ...


    """

    def __init__(self, filename, check_video=True):

        super().__init__(filename)

        self.check_video = check_video

        self.props_keys       = [x for x in self.header[1:]
                                 if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]

    def getitem(self, idx):

        filename   = self.df.loc[idx, HEADER_PATH]
        data = {}

        video_ok, video = self.load_video(filename)
        if not video_ok:
            print("Error loading video: " + filename)
            raise Exception("Error loading video: " + filename)

        data["video_blob"] = video

        properties  = self.parse_properties(self.df, idx)
        constraints = self.parse_constraints(self.df, idx)

        if properties:
            data[PROPERTIES] = properties

        if constraints:
            data[CONSTRAINTS] = constraints

        return data

    def load_video(self, filename):

        if self.check_video:
            try:
                a = cv2.VideoCapture(filename)
                if a.isOpened() == False:
                    print("Video reading Error:", filename)
            except:
                print("Video Error:", filename)

        try:
            fd = open(filename, "rb")
            buff = fd.read()
            fd.close()
            return True, buff
        except:
            print("Video Error:", filename)

        return False, None

    def validate(self):

        self.header = list(self.df.columns.values)

        if self.header[0] != HEADER_PATH:
            raise Exception(
                "Error with CSV file field: filename. Must be first field")


class VideoLoader(ParallelLoader.ParallelLoader):
    """**ApertureDB Video Loader.**

    This class is to be used in combination with a "generator",
    for example :class:`~aperturedb.VideoLoader.VideoGeneratorCSV`.
    The generator must be an iterable object that generated `video_data`
    elements.

    Example::

            image_data = {
                "properties":  properties,
                "constraints": constraints,
                "operations":  operations,
                "video_blob":    (bytes),
            }
    """

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "video"

    def generate_batch(self, video_data):

        q = []
        blobs = []

        for data in video_data:

            ai = {
                "AddVideo": {
                }
            }

            if "properties" in data:
                ai["AddVideo"]["properties"] = data["properties"]
            if "constraints" in data:
                ai["AddVideo"]["if_not_found"] = data["constraints"]
            if "operations" in data:
                ai["AddVideo"]["operations"] = data["operations"]
            if "format" in data:
                ai["AddVideo"]["format"] = data["format"]

            if "video_blob" not in data or len(data["video_blob"]) == 0:
                print("WARNING: Skipping empty video.")
                continue

            blobs.append(data["video_blob"])
            q.append(ai)

        return q, blobs
