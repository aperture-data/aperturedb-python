#!/usr/bin/env python3
from aperturedb import Connector 
import time
if __name__ == '__main__':

    c = Connector.Connector( 'localhost', 55557, 'admin','admin')
    print( "Connected? {0}".format( "yes" if c.connected else "no" ))
    print( "Session valid? {0}".format( "yes" if c.session.valid() else "no"))
    print( "Valid length: {0}".format( c.session.session_token_ttl ))
    time.sleep(5)
    query = [{
        "FindImage" : {
            "results":{
                "limit":5
                }
            }
        }]
    responses,blobs = c.query(query)
    print(responses)
