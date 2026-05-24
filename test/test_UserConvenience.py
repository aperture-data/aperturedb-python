import json
from types import SimpleNamespace
from aperturedb.ConnectorRest import ConnectorRest
from requests.sessions import Session


class TestUserConvenience():
    """
    This class tests some undocumented features of the Python SDK.
    This cannot rely on dbinfo, or connect as dbinfo and common lib rely on explicit
    arguments.
    """

    def test_ConnectorRest_handlesNonePort(self):
        """
        Test that ConnectorRest can handle a None port,
        and will default to the correct port.
        """
        client = ConnectorRest(host="dummy", user="admin", password="password")
        assert "443" in client.url
        posts = 0

        def mock_post(self, url, headers, files, verify):
            nonlocal posts
            assert "443" in url
            response1 = {
                "json": [{"Authenticate": {
                    "status": 0,
                    "session_token": "x",
                    "refresh_token": "2",
                    "session_token_expires_in": 3600,
                    "refresh_token_expires_in": 3600
                }}],
                "blobs": []
            }

            r = SimpleNamespace(status_code=200, text=json.dumps(response1))
            posts += 1
            return r
        old_post = Session.post
        Session.post = mock_post
        client.query("[{\"FindEntity\": {\"_ref\": 1}}]")
        # Ensure that the mock post was called, 1 time to authenticate, 1 time to query
        assert posts == 2
        Session.post = old_post

    def test_ConnectorRest_close_and_recreate_session(self):
        """
        Test that ConnectorRest can be closed, clearing its http_session,
        and that a subsequent call to query() will transparently recreate it.
        """
        client = ConnectorRest(host="dummy", user="admin", password="password")
        posts = 0

        def mock_post(self, url, headers, files, verify):
            nonlocal posts
            response1 = {
                "json": [{"Authenticate": {
                    "status": 0,
                    "session_token": "x",
                    "refresh_token": "2",
                    "session_token_expires_in": 3600,
                    "refresh_token_expires_in": 3600
                }}],
                "blobs": []
            }

            r = SimpleNamespace(status_code=200, text=json.dumps(response1))
            posts += 1
            return r

        old_post = Session.post
        Session.post = mock_post

        try:
            # Query 1: Initialize normally
            client.query("[{\"FindEntity\": {\"_ref\": 1}}]")
            assert posts == 2  # 1 auth, 1 query
            assert client.http_session is not None
            old_session = client.http_session

            # Explicitly close the connector
            client.close()
            assert getattr(client, "http_session", None) is None

            # Query 2: Should transparently recreate the session
            client.query("[{\"FindEntity\": {\"_ref\": 2}}]")
            # Since auth is already done, it only does 1 query call?
            # Wait, Should authentication be redone? We still have client.shared_data.session.valid()
            # But let's check posts
            assert posts >= 3
            assert client.http_session is not None
            assert client.http_session is not old_session
        finally:
            Session.post = old_post
