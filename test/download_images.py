import argparse
import random
import math

from aperturedb import ImageDownloader

def main(params):

    loader = ImageDownloader.ImageDownloader(None)
    loader.ingest(ImageDownloader.ImageDownloaderCSV(params.in_file, check_image=True),
                    numthreads=32,
                    stats=True)

def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-in_file', type=str, default="input/url_images.adb.csv")

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    args = get_args()
    main(args)
