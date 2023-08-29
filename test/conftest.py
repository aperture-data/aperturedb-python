import pytest
import mark_checker
from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ParallelQuerySet import ParallelQuerySet
from aperturedb.BlobDataCSV import BlobDataCSV
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.EntityUpdateDataCSV import SingleEntityUpdateDataCSV
from aperturedb.ConnectionDataCSV import ConnectionDataCSV
from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV
from aperturedb.DescriptorDataCSV import DescriptorDataCSV
from aperturedb.ImageDataCSV import ImageDataCSV, ImageUpdateDataCSV, ImageForceNewestDataCSV, ImageSparseAddDataCSV
from aperturedb.BBoxDataCSV import BBoxDataCSV
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
from aperturedb.Query import Query
from aperturedb.Utils import Utils

import dbinfo

# These are test fixtures and can be used in any
# pytest tests as function parmeters with same names.


def pytest_generate_tests(metafunc):
    if "db" in metafunc.fixturenames:
        metafunc.parametrize("db", [
            {"db": Connector(
                host = dbinfo.DB_TCP_HOST,
                port = dbinfo.DB_TCP_PORT,
                user = dbinfo.DB_USER,
                password = dbinfo.DB_PASSWORD,
                use_ssl = True)},
            {"db": ConnectorRest(
                host = dbinfo.DB_REST_HOST,
                port = dbinfo.DB_REST_PORT,
                user = dbinfo.DB_USER,
                password = dbinfo.DB_PASSWORD,
                use_ssl = False
            )}
        ], indirect=True, ids=["TCP", "HTTP"])
    if all(func in metafunc.fixturenames for func in ["insert_data_from_csv", "modify_data_from_csv"]) and \
            metafunc.module.__name__ in ["test.test_Data"]:
        metafunc.parametrize("insert_data_from_csv,modify_data_from_csv", [
                             [True, True], [False, False]], indirect=True, ids=["with_dask", "without_dask"])
    elif "insert_data_from_csv" in metafunc.fixturenames and metafunc.module.__name__ in \
            ["test.test_Data"]:
        metafunc.parametrize("insert_data_from_csv", [
                             True, False], indirect=True, ids=["with_dask", "without_dask"])
    elif "modify_data_from_csv" in metafunc.fixturenames and metafunc.module.__name__ in \
            ["test.test_Data"]:
        metafunc.parametrize("modify_data_from_csv", [
                             True, False], indirect=True, ids=["with_dask", "without_dask"])


@pytest.fixture()
def db(request):
    db = request.param['db']
    utils = Utils(db)
    assert utils.remove_all_objects()
    return db


def check_response_regressions(queries, input_blobs, responses, output_blobs):
    # Check that responses have no blobs
    first_command = list(responses[0].keys())[0]
    assert "blobs_start" not in responses[0][
        first_command], f"responses[0]={responses[0]}"


@pytest.fixture()
def insert_data_from_csv(db, request):
    """
    A helper function that processes various .csv files supported
    by aperturedb, and maps a corresponding DataCSV class that can be
    used to parse semantics of the .csv file
    """
    def insert_data_from_csv(in_csv_file, rec_count=-1, expected_error_count=0, loader_result_lambda=None):
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
            "./input/gs_images.adb.csv": ImageDataCSV,
            "./input/images_to_modify.adb.csv": ImageDataCSV,
            "./input/images_to_modify.adb.csv": ImageDataCSV,
            "./input/persons-exist-base.adb.csv": EntityDataCSV,
            "./input/persons-some-exist.adb.csv": EntityDataCSV
        }
        use_dask = False
        if hasattr(request, "param"):
            use_dask = request.param
        data = file_data_pair[in_csv_file](in_csv_file, use_dask=use_dask,
                                           blobs_relative_to_csv=True)

        setattr(data, "response_handler", check_response_regressions)
        data.strict_response_validation = True

        if rec_count != -1:
            data = data[:rec_count]

        loader = ParallelLoader(db)
        loader.ingest(data, batchsize=99,
                      numthreads=8,
                      stats=True,
                      )

        # make sure all indices are present
        if hasattr(data, "get_indices"):
            expected_indices = data.get_indices()
            observed_indices = loader.get_existing_indices()
            for tp, classes in expected_indices.items():
                for cls, props in classes.items():
                    for prop in props:
                        assert prop in observed_indices[tp][cls]

        assert loader.error_counter == 0
        assert len(data) - \
            loader.get_suceeded_queries() == expected_error_count
        if loader_result_lambda is not None:
            loader_result_lambda(loader, data)
        assert len(data) - \
            loader.get_suceeded_queries() == expected_error_count
        if loader_result_lambda is not None:
            loader_result_lambda(loader, data)

        # Preserve loader so that dask manager is not auto deleted.
        # ---------------
        # Previously, dask's cluster and client were entirely managed in a context
        # insede dask manager. A change upstream broke that, and now we keep the DM
        # in scope just so that we can do computations after ingestion, as those
        # things are lazily evaluated.
        return data, loader

    return insert_data_from_csv


class UpdatePersonEntityDataCSV(SingleEntityUpdateDataCSV):
    def __init__(self, filename, **kwargs):
        super().__init__("Person", filename, **kwargs)


@pytest.fixture()
def modify_data_from_csv(db, request):
    def modify_data_from_csv(in_csv_file, rec_count=-1):
        if rec_count > 0 and rec_count < 80:
            request.param = False
            print("Not enough records to test parallel loader. Using serial loader.")
        file_data_pair = {
            "./input/persons-update.adb.csv": UpdatePersonEntityDataCSV,
            "./input/persons-update-oldversion.adb.csv": UpdatePersonEntityDataCSV,
            "./input/persons-update-newversion.adb.csv": UpdatePersonEntityDataCSV,
            "./input/persons-update-olderage.adb.csv": UpdatePersonEntityDataCSV,
            "./input/images_updateif_baseload.adb.csv": ImageUpdateDataCSV,
            "./input/images_updateif_mixednew.adb.csv": ImageUpdateDataCSV,
            "./input/images_updateif_fail_baseload.adb.csv": ImageUpdateDataCSV,
            "./input/images_updateif_fail_updates.adb.csv": ImageUpdateDataCSV,
            "./input/images_forceupdate_baseload.adb.csv": ImageForceNewestDataCSV,
            "./input/images_forceupdate_mixednew.adb.csv": ImageForceNewestDataCSV,
            "./input/images_forceupdate_fail_base.adb.csv": ImageForceNewestDataCSV,
            "./input/images_forceupdate_fail_updates.adb.csv": ImageForceNewestDataCSV,
            "./input/images_forceupdate_blob_baseload.adb.csv": ImageForceNewestDataCSV,
            "./input/images_forceupdate_updates.adb.csv": ImageForceNewestDataCSV,
            "./input/images_sparseload_base.adb.csv": ImageSparseAddDataCSV,
            "./input/images_sparseload_full.adb.csv": ImageSparseAddDataCSV
        }
        use_dask = False
        if hasattr(request, "param"):
            use_dask = request.param
        data = file_data_pair[in_csv_file](
            in_csv_file, use_dask=use_dask, blobs_relative_to_csv=True)
        if rec_count != -1:
            data = data[:rec_count]

        loader = ParallelQuerySet(db)
        loader.query(data, batchsize=99,
                     numthreads=1,
                     stats=True,
                     )
        assert loader.error_counter == 0
        return data, loader

    return modify_data_from_csv


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
    retired_persons = Entities.retrieve(
        db, spec=Query.spec(
            with_class="Person", constraints=constraints))
    return retired_persons


@pytest.fixture()
def config():
    config = {
        "first": {
            "host": "localhost",
            "port": 55555,
            "username": "admin",
            "password": "admin",
            "name": "first",
            "use_ssl": True,
            "use_rest": False
        },
        "active": "first"
    }
    return config


def pytest_configure(config):
    mark_checker.store_config(config)
