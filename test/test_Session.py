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
        try:
            # force session token expiry
            db.shared_data.session.session_token_ttl = 1
            logging.debug("Connected? {0}".format(
                "yes" if db.connected else "no"))

            logging.debug(
                "Session valid? {0}".format(
                    "yes" if db.shared_data.session.valid() else "no"))
            logging.debug("Valid length: {0}".format(
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
            logging.debug(responses)
            assert db.shared_data.session.valid() == True
        except Exception as e:
            print(e)
            print("Failed to renew Session")
            assert False

    def test_SSL_error_on_query(self, db: Connector, monkeypatch):

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
        try:
            new_db = Connector(
                dbinfo.DB_TCP_HOST,
                dbinfo.DB_TCP_PORT,
                dbinfo.DB_USER,
                dbinfo.DB_PASSWORD,
                retry_connect_max_attempts=3,
                retry_connect_interval_seconds=0)
        except Exception as e:
            # Check the exception is not an obscure one.
            assert str(e).startswith("Could not connect to apertureDB server:")

        # Check that we tried to connect 3 times.
        assert connect_attempts == 3

    def test_socket_send_error_initial(self, monkeypatch):
        connect_attempts = 0

        def mock_send(x, buff):
            nonlocal connect_attempts
            connect_attempts += 1
            raise socket.error("Connection broke when send")
        monkeypatch.setattr(socket.socket, "send", mock_send)

        # Create new db connection.
        try:
            new_db = Connector(
                dbinfo.DB_TCP_HOST,
                dbinfo.DB_TCP_PORT,
                dbinfo.DB_USER,
                dbinfo.DB_PASSWORD,
                retry_connect_max_attempts=3,
                retry_connect_interval_seconds=0)
        except Exception as e:
            # Check the exception is not an obscure one.
            assert str(e).startswith("Could not connect to apertureDB server:")

        # Check that we tried to connect 3 times.
        assert connect_attempts == 3

    def test_socket_recv_error_initial(self, monkeypatch):
        connect_attempts = 0

        def mock_recv(x, buff):
            nonlocal connect_attempts
            connect_attempts += 1
            # raise socket.error("Connection broke when recv")
            raise ConnectionResetError("Connection broke when recv")
        monkeypatch.setattr(socket.socket, "recv", mock_recv)

        # Create new db connection.
        try:
            new_db = Connector(
                dbinfo.DB_TCP_HOST,
                dbinfo.DB_TCP_PORT,
                dbinfo.DB_USER,
                dbinfo.DB_PASSWORD,
                retry_connect_max_attempts=3,
                retry_connect_interval_seconds=0)
        except Exception as e:
            # Check the exception is not an obscure one.
            assert str(e).startswith("Could not connect to apertureDB server:")

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
