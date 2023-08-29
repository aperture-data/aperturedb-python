import time
import logging
import ssl

from aperturedb.Connector import Connector

logger = logging.getLogger(__name__)


class TestSession():

    '''
        These check operation of the Connector Session
    '''

    def test_sessionRenew(self, db: Connector):
        '''
            Verifies that Session renew works
        '''

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
        # force session token expiry
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

            logger.debug(f"Cut {msg}")
            result  = original_send_msg(msg)
            logger.debug(result)
            return result

        monkeypatch.setattr(db, "_send_msg", lambda x: mock_send_msg(x))
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
