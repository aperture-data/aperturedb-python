import numpy as np
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.Query import QueryBuilder

import logging
logger = logging.getLogger(__name__)


class TestLoaderSuccess():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def test_Loader(self, utils, insert_data_from_csv):
        # Assert that we have a clean slate to begin with.
        assert utils.remove_all_indexes()
        assert utils.remove_all_objects() == True
        # initial load
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/persons-exist-base.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))

        # default configuration does not consider object exists to be a query failure
        def assert_partial(loader, test_data):
            self.assertEqual(len(data) + len(test_data) - loader.get_objects_existed(),
                             utils.count_entities("Person"))
        data, _ = insert_data_from_csv(in_csv_file = "./input/persons-some-exist.adb.csv",
                                       loader_result_lambda = assert_partial)

        # change to disallow object exist to qualify as success.
        old_status = ParallelQuery.getSuccessStatus()
        ParallelQuery.setSuccessStatus([0])

        # Assert that we have a clean slate to begin with.
        assert utils.remove_all_indexes()
        assert utils.remove_all_objects() == True
        # initial load
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/persons-exist-base.adb.csv")
        # default configuration does not consider object exists to be a query
        # failure
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/persons-some-exist.adb.csv",
            expected_error_count = 3,
            loader_result_lambda=assert_partial)

        # reset success status to default
        ParallelQuery.setSuccessStatus(old_status)
