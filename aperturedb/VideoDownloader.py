import time
import requests
import os
from os import path

import cv2
import numpy as np

from aperturedb import Parallelizer
from aperturedb import CSVParser
from aperturedb import ProgressBar

HEADER_PATH = "filename"
HEADER_URL  = "url"


class VideoDownloaderCSV(CSVParser.CSVParser):
    """
        **ApertureDB Video Downloader.**

    .. important::

        Expects a csv file with AT LEAST a ``url`` column, and
        optionally a ``filename`` field.
        If ``filename`` is not present, it is taken from the url.
    """

    def __init__(self, filename, check_video=True):

        self.has_filename = False
        self.check_video = check_video

        super().__init__(filename)

    def __getitem__(self, idx):

        url = self.df.loc[idx, HEADER_URL]

        if self.has_filename:
            filename = self.df.loc[idx, HEADER_PATH]
        else:
            filename = self.url_to_filename(url)

        return url, filename

    def url_to_filename(self, url):

        filename = url.split("/")[-1]
        folder = "/tmp/videos/"

        return folder + filename

    def validate(self):

        self.header = list(self.df.columns.values)

        if HEADER_URL not in self.header:
            raise Exception("Error with CSV file field: url. Must be a field")

        if HEADER_PATH in self.header:
            self.has_filename = True


class VideoDownloader(Parallelizer.Parallelizer):

    def __init__(self, ):

        super().__init__()

        self.type = "video"

        self.check_video = False

    def check_if_video_is_ok(self, filename, url):

        if not os.path.exists(filename):
            return False

        try:
            a = cv2.VideoCapture(filename)
            if a.isOpened() == False:
                print("Video present but error reading it:", url)
                return False
        except BaseException:
            print("Video present but error decoding:", url)
            return False

        return True

    def download_video(self, url, filename):

        start = time.time()

        if self.check_video and self.check_if_video_is_ok(filename, url):
            return

        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        videodata = requests.get(url)
        if videodata.ok:
            fd = open(filename, "wb")
            fd.write(videodata.content)
            fd.close()

            try:
                a = cv2.VideoCapture(filename)
                if a.isOpened() == False:
                    print("Downloaded Video size error:", url)
                    os.remove(filename)
                    self.error_counter += 1
            except BaseException:
                print("Downloaded Video cannot be decoded:", url)
                os.remove(filename)
                self.error_counter += 1
        else:
            print("URL not found:", url)
            self.error_counter += 1

        self.times_arr.append(time.time() - start)

    def worker(self, thid, generator, start, end):

        for i in range(start, end):

            url, filename = generator[i]

            self.download_video(url, filename)

            if thid == 0 and self.stats:
                self.pb.update((i - start) / (end - start))

    def print_stats(self):

        print("====== ApertureDB VideoDownloader Stats ======")

        times = np.array(self.times_arr)
        print("Avg Video download time(s):", np.mean(times))
        print("Img download time std:", np.std(times))
        print("Avg download throughput (videos/s)):",
              1 / np.mean(times) * self.numthreads)

        print("Total time(s):", self.total_actions_time)
        print("Overall throughput (videos/s):",
              self.total_actions / self.total_actions_time)
        print("Total errors encountered:", self.error_counter)
        print("=============================================")
