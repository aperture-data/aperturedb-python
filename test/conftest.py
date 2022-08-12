import pytest
from aperturedb.Connector import Connector
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV
from aperturedb.DescriptorDataCSV import DescriptorDataCSV
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.BBoxDataCSV import BBoxDataCSV
from aperturedb.Utils import Utils

import dbinfo


@pytest.fixture(scope="module")
def db():
    return Connector(
        port=dbinfo.DB_PORT,
        user=dbinfo.DB_USER,
        password=dbinfo.DB_PASSWORD)


@pytest.fixture(scope="module")
def insert_data_from_csv(db):
    def insert_data_from_csv(in_csv_file, rec_count=-1):
        file_data_pair = {
            "./input/persons.adb.csv": EntityDataCSV,
            "./input/images.adb.csv": ImageDataCSV,
            "./input/connections-persons-images.adb.csv": ConnectionDataCSV,
            "./input/bboxes.adb.csv": BBoxDataCSV,
            "./input/blobs.adb.csv": BlobDataCSV,
            "./input/descriptorset.adb.csv": DescriptorSetDataCSV,
            "./input/setA.adb.csv": DescriptorDataCSV,
            "./input/setB.adb.csv": DescriptorDataCSV,
            "./input/s3_images.adb.csv": ImageDataCSV,
            "./input/http_images.adb.csv": ImageDataCSV,
            './input/bboxes-constraints.adb.csv': BBoxDataCSV
        }

        data = file_data_pair[in_csv_file](in_csv_file)
        if rec_count != -1:
            data = data[:rec_count]

        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert loader.error_counter == 0
        return data

    return insert_data_from_csv


@pytest.fixture(scope="module")
def utils(db):
    return Utils(db)
