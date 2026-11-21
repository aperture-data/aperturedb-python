import json
import pytest
from aperturedb.Configuration import Configuration
from types import SimpleNamespace
from aperturedb.ConnectorRest import ConnectorRest
from requests.sessions import Session


class TestUserConvenience():
    """
    This class tests some undocumented features of the Python SDK.
    This cannot rely on dbinfo, or connect as dbinfo and common lib rely on explicit
    arguments.
    """

    @pytest.mark.parametrize("use_ssl, expected_port", [(True, 443), (False, 80)])
    def test_ConnectorRest_handlesNonePort(self, monkeypatch, use_ssl, expected_port):
        """
        Test that ConnectorRest can handle a None port,
        and will default to the correct port.
        """
        client = ConnectorRest(host="dummy", user="admin",
                               password="password", use_ssl=use_ssl)
        assert client.port == expected_port
        assert client.config.port == expected_port
        assert client.config.use_rest is True
        reconstructed = ConnectorRest(config=client.config)
        assert reconstructed.url == client.url
        assert reconstructed.port == expected_port
        assert reconstructed.config.use_rest is True
        reconstructed.close()
        posts = 0

        def mock_post(self, url, headers, files, verify, stream, allow_redirects):
            nonlocal posts
            assert stream is True
            assert allow_redirects is False
            assert str(expected_port) in url
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

            body = json.dumps(response1).encode()
            r = SimpleNamespace(
                status_code=200,
                headers={"Content-Type": "application/json"},
                iter_content=lambda chunk_size: iter([body]),
                close=lambda: None)
            posts += 1
            return r
        monkeypatch.setattr(Session, "post", mock_post)
        client.query("[{\"FindEntity\": {\"_ref\": 1}}]")
        # One discovery request, one authentication, and one query.
        assert posts == 3
        client.close()

    def test_ConnectorRest_close_and_recreate_session(self, monkeypatch):
        """
        Test that ConnectorRest can be closed, clearing its http_session,
        and that a subsequent call to query() will transparently recreate it.
        """
        configuration = Configuration(
            host="configured", port=8081, username="admin", password="password",
            name="explicit-config", use_ssl=False)
        client = ConnectorRest(host="ignored", port=9999,
                               use_ssl=True, config=configuration)
        assert client.url == "http://configured:8081/api/"
        assert client.config.port == 8081
        assert client.config.use_rest is True
        assert configuration.use_rest is False
        reconstructed = ConnectorRest(config=client.config)
        assert reconstructed.url == client.url
        assert reconstructed.port == 8081
        assert reconstructed.config.use_rest is True
        reconstructed.close()
        posts = 0

        def mock_post(self, url, headers, files, verify, stream, allow_redirects):
            nonlocal posts
            assert stream is True
            assert allow_redirects is False
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

            body = json.dumps(response1).encode()
            r = SimpleNamespace(
                status_code=200,
                headers={"Content-Type": "application/json"},
                iter_content=lambda chunk_size: iter([body]),
                close=lambda: None)
            posts += 1
            return r

        monkeypatch.setattr(Session, "post", mock_post)

        try:
            # Query 1: Initialize normally
            client.query("[{\"FindEntity\": {\"_ref\": 1}}]")
            assert posts == 3  # 1 discovery, 1 auth, 1 query
            assert client.http_session is not None
            old_session = client.http_session

            # Explicitly close the connector
            client.close()
            assert getattr(client, "http_session", None) is None

            # Query 2: Should transparently recreate the session
            client.query("[{\"FindEntity\": {\"_ref\": 2}}]")
            assert posts == 5  # Rediscovery and query reuse the valid session.
            assert client.http_session is not None
            assert client.http_session is not old_session
        finally:
            client.close()
