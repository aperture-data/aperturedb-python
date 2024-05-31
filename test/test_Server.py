import pytest
from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb.ParallelLoader import ParallelLoader
import dbinfo
import pandas as pd

import logging
logger = logging.getLogger(__name__)


class TestBadResponses():

    def test_Error_code_2(self, db: Connector, insert_data_from_csv, monkeypatch):
        count = 0
        original_q = db._query

        def test_response_half_exist(a: Connector, query, blobs):
            nonlocal count
            if "AddImage" not in query[0]:
                count += 1
                resp = original_q(query, blobs)
                return resp
            response = []
            for i in range(len(query)):
                result = {"info": "Object Exists!",
                          "status": 2} if i % 2 == 0 else {"status": 0}
                response.append({"AddImage": result})

            return (response, [])
        monkeypatch.setattr(Connector, "_query", test_response_half_exist)
        monkeypatch.setattr(ParallelLoader, "get_existing_indices", lambda x: {
                            "entity": {"_Image": {"id"}}})
        data, loader = insert_data_from_csv(
            in_csv_file = "./input/images.adb.csv")
        assert loader.error_counter == 0
        assert loader.get_succeeded_queries() == len(data)
        assert loader.get_succeeded_commands() == len(data)

    def test_Error_code_3(self, db: Connector, insert_data_from_csv, monkeypatch):
        count = 0
        original_q = db._query

        def test_response_half_non_unique(a: Connector, query, blobs):
            nonlocal count
            if "AddImage" not in query[0]:
                count += 1
                resp = original_q(query, blobs)
                return resp
            response = None
            for i in range(len(query)):
                result = {
                    'info': 'JSON Command 1: expecting 1 but got 2', 'status': 3}
                response = result
                break

            return (response, [])
        monkeypatch.setattr(Connector, "_query", test_response_half_non_unique)
        monkeypatch.setattr(ConnectorRest, "_query",
                            test_response_half_non_unique)
        monkeypatch.setattr(ParallelLoader, "get_existing_indices", lambda x: {
                            "entity": {"_Image": {"id"}}})
        input_data = pd.read_csv("./input/images.adb.csv")
        data, loader = insert_data_from_csv(
            in_csv_file = "./input/images.adb.csv", expected_error_count = len(input_data))
        assert loader.error_counter == 0, f"Error counter: {loader.error_counter=}"
        assert loader.get_succeeded_queries(
        ) == 0, f"Queries: {loader.get_succeeded_queries()=}"
        assert loader.get_succeeded_commands(
        ) == 0, f"Commands: {loader.get_succeeded_commands()=}"

    def test_AuthFailure(self, monkeypatch):

        def failed_auth_query(conn_obj, ignored_query):
            # generate a response from the server which is not the expected Auth result.
            # _query returns the server response json and an array of blobs.
            return ({"info": "Internal Server Error 42", "status": -1, "ignored": ignored_query}, [])

        monkeypatch.setattr(Connector, "_query", failed_auth_query)

        with pytest.raises(Exception) as conn_exception:
            db = Connector(
                host = dbinfo.DB_TCP_HOST,
                port = dbinfo.DB_TCP_PORT,
                user = dbinfo.DB_USER,
                password = dbinfo.DB_PASSWORD,
                use_ssl = True)
            db.query([{
                "FindImage": {
                    "results": {
                        "limit": 5
                    }
                }
            }])

        assert "Unexpected response" in str(conn_exception.value)
