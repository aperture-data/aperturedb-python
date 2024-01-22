import pytest
from aperturedb.Connector import Connector
import dbinfo

import logging
logger = logging.getLogger(__name__)


class TestBadResponses():

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
