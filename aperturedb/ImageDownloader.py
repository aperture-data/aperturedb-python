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

    def __init__(self, filename):

        self.has_filename = False

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

    def __init__(self, db=None, dry_run=False, n_download_retries=0, check_if_present=False):

        super().__init__(db, dry_run=dry_run)

        self.type = "image"

        self.check_img = check_if_present
        self.images_already_downloaded = 0
        self.n_download_retries = n_download_retries

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
            self.images_already_downloaded += 1
            self.times_arr.append(time.time() - start)
            return

        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        retries = 0
        while True:
            imgdata = requests.get(url)
            if imgdata.ok:
                break
            else:
                if retries >= self.n_download_retries:
                    break
                print("WARNING: Retrying object:", url)
                retries += 1
                time.sleep(2)

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
            pb = ProgressBar.ProgressBar()

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
        if len(times) <= 0:
            print("Error: No downloads.")
            return

        if self.images_already_downloaded > 0:
            print("Images already present:", self.images_already_downloaded)

        print("Images downloaded:", len(times) - self.images_already_downloaded)
        print("Avg image time(s):", np.mean(times))
        print("Image time std:", np.std (times))
        print("Throughput (images/s)):",
            1 / np.mean(times) * self.numthreads)

        print("Total time(s):", self.ingestion_time)
        print("Overall throughput (img/s):",
            self.total_elements / self.ingestion_time)
        if self.error_counter > 0:
            print("Errors encountered:", self.error_counter)
        print("=============================================")
