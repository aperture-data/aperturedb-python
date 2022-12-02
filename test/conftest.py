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
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
from aperturedb.Query import Query
from aperturedb.Utils import Utils

import dbinfo


@pytest.fixture()
def db():
    return Connector(
        port=dbinfo.DB_PORT,
        user=dbinfo.DB_USER,
        password=dbinfo.DB_PASSWORD)


def pytest_generate_tests(metafunc):
    if "insert_data_from_csv" in metafunc.fixturenames and metafunc.module.__name__ in \
            ["test.test_Data"]:
        metafunc.parametrize("insert_data_from_csv", [
                             True, False], indirect=True, ids=["with_dask", "without_dask"])


@pytest.fixture()
def insert_data_from_csv(db, request):
    def insert_data_from_csv(in_csv_file, rec_count=-1):
        if rec_count > 0 and rec_count < 80:
            request.param = False
            print("Not enough records to test parallel loader. Using serial loader.")
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
            "./input/bboxes-constraints.adb.csv": BBoxDataCSV,
            "./input/gs_images.adb.csv": ImageDataCSV
        }
        use_dask = False
        if hasattr(request, "param"):
            use_dask = request.param
        data = file_data_pair[in_csv_file](in_csv_file, use_dask=use_dask)
        if rec_count != -1:
            data = data[:rec_count]

        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=99,
                      numthreads=8,
                      stats=True,
                      )
        assert loader.error_counter == 0
        return data

    return insert_data_from_csv


@pytest.fixture()
def utils(db):
    return Utils(db)


@pytest.fixture()
def images(insert_data_from_csv):
    return insert_data_from_csv("./input/images.adb.csv")


@pytest.fixture()
def retired_persons(db, insert_data_from_csv, utils):
    utils.remove_entities(class_name="Person")
    loaded = insert_data_from_csv("./input/persons.adb.csv")
    constraints = Constraints()
    constraints.greaterequal("age", 60)
    retired_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person", constraints=constraints))[0]
    return retired_persons
