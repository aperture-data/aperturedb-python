import argparse
import sys
from aperturedb import ImageDownloader


def main(params):

    downloader = ImageDownloader.ImageDownloader(
        check_if_present=True, n_download_retries=2)
    downloader.batched_run(ImageDownloader.ImageDownloaderCSV(params.in_file),
                           numthreads=32,
                           batchsize=1,
                           stats=True)
    return downloader.error_counter == 0


def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-in_file', type=str, default="input/url_images.adb.csv")

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    sys.exit(0 if main(args) else 1)
