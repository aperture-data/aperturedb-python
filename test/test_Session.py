import socket
import time
import logging
import ssl
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
                "results": {
                    "limit": 5
                }
            }
        }]
        responses, blobs = db.query(query)
        logger.debug(responses)
        logger.debug("Valid : {0}".format(
            db.shared_data.session.valid()))
        assert db.shared_data.session.valid() == True

    def test_SSL_error_on_query(self, monkeypatch):
        db = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            retry_max_attempts=3,
            retry_interval_seconds=0)
        db.query([{"FindImage": {"results": {"limit": 5}}}])
        original_send_msg = db._send_msg
        count = 0

        # This will raise an SSL error on the first call to _send_msg (which is an refresh token)
        # The later calls will succeed.
        # When we try to renew the session, that attempts to take a lock on the session.
        # And this is the hang.
        def mock_send_msg(msg):
            nonlocal count
            count += 1
            logger.debug(count)
            if count == 1:
                raise ssl.SSLError("SSL Error")

            result  = original_send_msg(msg)
            logger.debug(result)
            return result

        monkeypatch.setattr(db, "_send_msg", lambda x: mock_send_msg(x))

        # force session token expiry
        db.shared_data.session.session_token_ttl = 1
        time.sleep(2)
        query = [{
            "FindImage": {
                "results": {
                    "limit": 5
                }
            }
        }]
        responses, blobs = db.query(query)
        logging.debug(responses)
        assert db.shared_data.session.valid() == True

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
            retry_max_attempts=3,
            retry_interval_seconds=0)
        try:
            new_db.query([{"FindImage": {"results": {"limit": 5}}}])
        except Exception as e:
            # Check the exception is not an obscure one.
            assert "self.connected=False" in e.args[0]

        # Check that we tried to connect 3 times.
        assert connect_attempts == 3

    def test_socket_send_error_initial(self, monkeypatch):
        send_attempts = 0

        def mock_send(x, buff):
            nonlocal send_attempts
            send_attempts += 1
            raise socket.error("Connection broke when send")
        monkeypatch.setattr(socket.socket, "send", mock_send)

        # Create new db connection.
        new_db = Connector(
            dbinfo.DB_TCP_HOST,
            dbinfo.DB_TCP_PORT,
            dbinfo.DB_USER,
            dbinfo.DB_PASSWORD,
            retry_max_attempts=3,
            retry_interval_seconds=0)
        try:
            new_db.query([{"FindImage": {"results": {"limit": 5}}}])
        except Exception as e:
            # Check the exception is not an obscure one.
            assert "self.connected=False" in e.args[0]

        # Check that we tried to send 5 (connect hello:2) + query:3) times.
        assert send_attempts == 5

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
            retry_max_attempts=3,
            retry_interval_seconds=0)
        try:
            new_db.query([{"FindImage": {"results": {"limit": 5}}}])
        except Exception as e:
            # Check the exception is not an obscure one.
            assert "self.connected=False" in e.args[0]

        # Check that we tried to connect 3 times.
        assert connect_attempts == 3

    def test_con_close_on_send_query(self, db: Connector, monkeypatch):
        if not isinstance(db, ConnectorRest):
            original_send_msg = db._send_msg
            count = 0

            def mock_send_msg(msg):
                nonlocal count
                count += 1
                logger.debug(count)
                if count < 2:
                    # Fail for the firs 2 attempts
                    db.conn.close()

                result  = original_send_msg(msg)
                logger.debug(result)
                return result

            monkeypatch.setattr(db, "_send_msg", mock_send_msg)

            query = [{
                "FindImage": {
                    "results": {
                        "limit": 5
                    }
                }
            }]
            response, blobs = db.query(query)
            assert(response[0]["FindImage"]["status"] == 0)
            assert count == 3

    def test_con_close_on_recv_query(self, db: Connector, monkeypatch):
        if not isinstance(db, ConnectorRest):
            original_recv_msg = db._recv_msg
            count = 0

            def mock_recv_msg():
                nonlocal count
                count += 1
                logger.debug(count)
                if count < 2:
                    # Fail for the firs 2 attempts
                    db.conn.close()

                result  = original_recv_msg()
                logger.debug(result)
                return result

            monkeypatch.setattr(db, "_recv_msg", mock_recv_msg)

            query = [{
                "FindImage": {
                    "results": {
                        "limit": 5
                    }
                }
            }]
            response, blobs = db.query(query)
            assert(response[0]["FindImage"]["status"] == 0)
            assert count == 3

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
