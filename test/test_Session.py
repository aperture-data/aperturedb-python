import time
import logging

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
