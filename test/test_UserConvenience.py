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
