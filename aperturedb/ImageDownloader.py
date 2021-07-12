import time
import requests
import os
from os import path

import cv2
import numpy as np

from aperturedb import ParallelLoader
from aperturedb import CSVParser
from aperturedb import ProgressBar

HEADER_PATH = "filename"
HEADER_URL  = "url"

class ImageDownloaderCSV(CSVParser.CSVParser):

    '''
        ApertureDB Image Downloader.
        Expects a csv file with AT LEAST a "url" column, and
        optionally a "filename" field.
        If "filename" is not present, it is taken from the url.

    '''

    def __init__(self, filename, check_image=True):

        self.has_filename = False
        self.check_image = check_image

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
        folder = "/tmp/images/"

        return folder + filename

    def validate(self):

        self.header = list(self.df.columns.values)

        if HEADER_URL not in self.header:
            raise Exception("Error with CSV file field: url. Must be a field")

        if HEADER_PATH in self.header:
            self.has_filename = True

class ImageDownloader(ParallelLoader.ParallelLoader):

    def __init__(self, db, dry_run=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "image"

        self.check_img = False

    def check_if_image_is_ok(self, filename, url):

        if not os.path.exists(filename):
            return False

        try:
            a = cv2.imread(filename)
            if a.size <= 0:
                print("Image present but error reading it:", url)
                return False
        except:
            print("Image present but error decoding:", url)
            return False

        return True

    def download_image(self, url, filename):

        start = time.time()

        if self.check_img and self.check_if_image_is_ok(filename, url):
            return

        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        imgdata = requests.get(url)
        if imgdata.ok:
            fd = open(filename, "wb")
            fd.write(imgdata.content)
            fd.close()

            try:
                a = cv2.imread(filename)
                if a.size <= 0:
                    print("Downloaded image size error:", url)
                    os.remove(filename)
                    self.error_counter += 1
            except:
                print("Downloaded image cannot be decoded:", url)
                os.remove(filename)
                self.error_counter += 1
        else:
            print("URL not found:", url)
            self.error_counter += 1

        self.times_arr.append(time.time() - start)

    def worker(self, thid, generator, start, end):

        if thid == 0 and self.stats:
            pb = ProgressBar.ProgressBar("download_progress.txt")

        for i in range(start, end):

            url, filename = generator[i]

            self.download_image(url, filename)

            if thid == 0 and self.stats:
                pb.update((i - start) / (end - start))

        if thid == 0 and self.stats:
            pb.update(1)

    def print_stats(self):

        print("====== ApertureDB ImageDownloader Stats ======")

        times = np.array(self.times_arr)
        print("Avg image download time(s):", np.mean(times))
        print("Img download time std:", np.std (times))
        print("Avg download throughput (images/s)):",
            1 / np.mean(times) * self.numthreads)

        print("Total time(s):", self.ingestion_time)
        print("Overall throughput (img/s):",
            self.total_elements / self.ingestion_time)
        print("Total errors encountered:", self.error_counter)
        print("=============================================")
