import time

from test_Base import TestBase

import logging
logger = logging.getLogger(__name__)


class TestSession(TestBase):

    '''
        These check operation of the Connector Session
    '''

    def test_sessionRenew(self):
        '''
            Verifies that Session renew works
        '''

        try:
            db = self.create_connection()
            # force session token expiry
            db.session.session_token_ttl = 1
            logging.debug("Connected? {0}".format(
                "yes" if db.connected else "no"))
            logging.debug(
                "Session valid? {0}".format(
                    "yes" if db.session.valid() else "no"))
            logging.debug("Valid length: {0}".format(
                db.session.session_token_ttl))
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
            self.assertTrue(db.session.valid(), "Failed to renew Session")
        except Exception:
            self.fail("Failed to renew Session")
