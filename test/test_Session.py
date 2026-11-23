import socket
import time
import logging
import ssl
import pytest
import dbinfo

from aperturedb.Connector import Connector
from aperturedb.ConnectorRest import ConnectorRest

logger = logging.getLogger(__name__)


class TestSession():
    """
    These check operation of the Connector Session
    and the error handling with the Network layer
    """

    def test_sessionRenew(self, db: Connector):
        """
        Verifies that Session renew works
        """

        # force session token expiry
        db.shared_data.session.session_token_ttl = 1
        logger.debug("Connected? {0}".format(
            "yes" if db.connected else "no"))
        logger.debug(
            "Session valid? {0}".format(
                "yes" if db.shared_data.session.valid() else "no"))
        logger.debug("Valid length: {0}".format(
            db.shared_data.session.session_token_ttl))
        time.sleep(2)
        query = [{
            "FindImage": {
                "limit": 5,
                "results": {"count": True}
            }
        }]
        responses, blobs = db.query(query)
        logger.debug(responses)
        logger.debug("Valid : {0}".format(
            db.shared_data.session.valid()))
        assert db.shared_data.session.valid() == True

    @pytest.mark.tcp
    def test_SSL_error_on_query(self, monkeypatch):
        db = Connector(
            dbinfo.DB_TCP_HOST, dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER, dbinfo.DB_PASSWORD, ca_cert=dbinfo.CA_CERT,
            retry_max_attempts=3, retry_interval_seconds=0)
        try:
            db.query([{"GetStatus": {}}])
            original = db._start_query_response
            count = 0

            def interrupted(query, blobs, release):
                nonlocal count
                count += 1
                if count == 1:
                    raise ssl.SSLError("SSL Error")
                return original(query, blobs, release)

            monkeypatch.setattr(db, "_start_query_response", interrupted)
            db.shared_data.session.session_token_ttl = 0
            with pytest.raises(ConnectionError):
                db.query([{"GetStatus": {}}])
            # A refresh transport failure cannot replay a potentially received
            # request, and must release the shared session lock without hanging.
            assert count == 1
            assert not db.connected
            response, _ = db.query([{"GetStatus": {}}])
            assert response[0]["GetStatus"]["status"] == 0
            assert db.shared_data.session.valid()
        finally:
            db.close()

    def test_socket_connect_error_initial(self, monkeypatch):
        connect_attempts = 0

        def mock_connect(host, port):
            nonlocal connect_attempts
            connect_attempts += 1
            raise ConnectionRefusedError("Connection Refused")
        monkeypatch.setattr(socket.socket, "connect",
                            lambda h, p: mock_connect(h, p))

        # Create new db connection.

        new_db = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            ca_cert=dbinfo.CA_CERT,
            retry_max_attempts=3,
            retry_interval_seconds=0)
        try:
            new_db.query(
                [{"FindImage": {"limit": 5, "results": {"count": True}}}])
        except Exception as e:
            # Check the exception is not an obscure one.
            assert "self.connected=False" in e.args[0]

        assert connect_attempts == 3

    @pytest.mark.tcp
    def test_socket_send_error_initial(self, monkeypatch):
        send_attempts = 0

        def mock_send(x, buff):
            nonlocal send_attempts
            send_attempts += 1
            raise socket.error("Connection broke when send")
        monkeypatch.setattr(socket.socket, "sendall", mock_send)

        # Create new db connection.
        new_db = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            ca_cert=dbinfo.CA_CERT,
            retry_max_attempts=3,
            retry_interval_seconds=0)
        try:
            new_db.query(
                [{"FindImage": {"limit": 5, "results": {"count": True}}}])
        except Exception as e:
            # Check the exception is not an obscure one.
            assert "self.connected=False" in e.args[0]

        # Only initial hello negotiation is retried; no query was sent.
        assert send_attempts == 3

    @pytest.mark.tcp
    def test_socket_recv_error_initial(self, monkeypatch):
        connect_attempts = 0

        def mock_recv(x, buff):
            nonlocal connect_attempts
            connect_attempts += 1
            # raise socket.error("Connection broke when recv")
            raise ConnectionResetError("Connection broke when recv")
        monkeypatch.setattr(socket.socket, "recv", mock_recv)

        # Create new db connection.

        new_db = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            ca_cert=dbinfo.CA_CERT,
            retry_max_attempts=3,
            retry_interval_seconds=0)
        try:
            new_db.query(
                [{"FindImage": {"limit": 5, "results": {"count": True}}}])
        except Exception as e:
            # Check the exception is not an obscure one.
            assert "self.connected=False" in e.args[0]

        assert connect_attempts == 3

    def test_con_close_on_send_query(self, db: Connector, monkeypatch):
        if isinstance(db, ConnectorRest):
            return
        db.query([{"GetStatus": {}}])
        original = db._start_query_response
        count = 0

        def interrupted(query, blobs, release):
            nonlocal count
            count += 1
            db.conn.close()
            return original(query, blobs, release)

        monkeypatch.setattr(db, "_start_query_response", interrupted)
        with pytest.raises(ConnectionError):
            db.query([{"GetStatus": {}}])
        assert count == 1
        assert not db.connected

    def test_con_close_on_recv_query(self, db: Connector, monkeypatch):
        if isinstance(db, ConnectorRest):
            return
        db.query([{"GetStatus": {}}])
        original = db._start_query_response
        count = 0

        def interrupted(query, blobs, release):
            nonlocal count
            count += 1
            original_receive = db._wire_input.receive

            def failed_receive(size):
                db.conn.close()
                return original_receive(size)

            with monkeypatch.context() as local:
                local.setattr(db._wire_input, "receive", failed_receive)
                return original(query, blobs, release)

        monkeypatch.setattr(db, "_start_query_response", interrupted)
        with pytest.raises(ConnectionError):
            db.query([{"GetStatus": {}}])
        # The server may already have executed this query. No implicit replay.
        assert count == 1
        assert not db.connected

    def test_invalid_session_recovery(self, db: Connector, monkeypatch):
        # simulate session invalidation
        original_query = db._query
        enable_mock = True

        def mock_refresher(query, blobs=[], try_resume=True):
            # when enabled, the mock will return a status -1 for the RefreshToken
            # and a schema query will be used to get not authenticated.
            nonlocal enable_mock
            print(f"{query=}")
            r, b = original_query(query, blobs, try_resume)
            if enable_mock:
                if "RefreshToken" in query[0]:
                    r[0]["RefreshToken"]["status"] = -1
                if "GetSchema" in query[0]:
                    r = {"info": "Not Authenticated!", "status": -1}
                if "Authenticate" in query[0]:
                    enable_mock = False
            print(f"{r=}")
            return r, b
        monkeypatch.setattr(
            db,
            "_query",
            mock_refresher)

        # Ensure a session validation error
        db.shared_data.session.session_token_ttl = 0
        resp, blobs = db.query([{'GetSchema': {}}], [])
        print(f"{resp=}")
        assert enable_mock == False, "Authentication query was not invoked"

    def test_connect_at_construction(self, monkeypatch):
        # We want to test that connect() is called when connect=True.
        # mock connect so it doesn't fail on missing server
        def mock_connect(self):
            self.connected = True

            class MockConn:
                def close(self): pass
            self.conn = MockConn()

        monkeypatch.setattr(Connector, "connect", mock_connect)

        new_db = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            ca_cert=dbinfo.CA_CERT,
            connect=True)
        assert getattr(new_db, "connected", False) == True

        # also test default behavior
        new_db_lazy = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            ca_cert=dbinfo.CA_CERT)
        assert getattr(new_db_lazy, "connected", False) == False
