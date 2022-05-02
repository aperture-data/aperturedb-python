import time

from test_Base import TestBase

from aperturedb import Connector


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
            print( "Connected? {0}".format( "yes" if db.connected else "no" ))
            print( "Session valid? {0}".format( "yes" if db.session.valid() else "no"))
            print( "Valid length: {0}".format( db.session.session_token_ttl ))
            time.sleep(2)
            query = [{
                "FindImage" : {
                    "results":{
                        "limit":5
                        }
                    }
                }]
            responses,blobs = db.query(query)
            print(responses)
            self.assertTrue( db.session.valid(), "Failed to renew Session")
        except Exception:
            self.fail("Failed to renew Session")
