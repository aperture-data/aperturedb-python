#
# The MIT License
#
# @copyright Copyright (c) 2017 Intel Corporation
# @copyright Copyright (c) 2021 ApertureData Inc
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from __future__ import annotations
import os
import requests
import time
import json
import logging

from threading import Lock
from types import SimpleNamespace
from dataclasses import dataclass
from aperturedb.Connector import Connector
from aperturedb.Configuration import Configuration

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = 1


class UnauthorizedException(Exception):
    pass


@dataclass
class Session():

    session_token:      str
    refresh_token:      str
    session_token_ttl:  int
    refresh_token_ttl:  int
    session_started:    time.time = time.time()

    def valid(self) -> bool:
        session_age = time.time() - self.session_started

        # This triggers refresh if the session is about to expire.
        if session_age > self.session_token_ttl - \
                int(os.getenv("SESSION_EXPIRTY_OFFSET_SEC", 10)):
            return False

        return True


class ConnectorRest(Connector):
    """
    **Class to use aperturedb's REST interface**

    Args:
        str (host): Address of the host to connect to.
        int (port): Port to connect to.
        str (user): Username to specify while establishing a connection.
        str (password): Password to specify while connecting to the db.
        str (token): Token to use while connecting to the database.
        bool (use_ssl): Use SSL to encrypt communication with the database.
    """

    def __init__(self, host="localhost", port=None,
                 user="", password="", token="",
                 use_ssl=True, shared_data=None,
                 config: Configuration = None):

        if config is None:
            self.host = host

            if port is None:
                self.port = 443 if use_ssl else 80
            else:
                self.port = port

            self.use_ssl = use_ssl
            self.config = Configuration(
                host=self.host,
                port=self.port,
                use_ssl=self.use_ssl,
                username=user,
                password=password,
                name="runtime"
            )
        else:
            self.config = config
            self.host = config.host
            self.port = config.port
            self.use_ssl = config.use_ssl

        self.connected = False
        # Session is useful because it does not add "Connection: close header"
        # Since we will be making same call to the same URL, making a session
        # REF: https://requests.readthedocs.io/en/latest/user/advanced/
        self.http_session = requests.Session()

        self.last_response   = ''
        self.last_query_time = 0

        self.url = ('https' if self.use_ssl else 'http') + \
            '://' + host + ':' + str(port) + '/api/'

        if shared_data is None:
            self.shared_data = SimpleNamespace()
            self.shared_data.session = None
            self.shared_data.lock = Lock()
            try:
                self._authenticate(user, password, token)
            except Exception as e:
                raise Exception("Authentication failed:", str(e))
        else:
            self.shared_data = shared_data

    def __del__(self):
        logger.info("Done with connector")
        self.http_session.close()

    def _query(self, query, blob_array = [], try_resume=True):
        response_blob_array = []
        # Check the query type
        if not isinstance(query, str):  # assumes json
            query_str = json.dumps(query)
        else:
            query_str = query

        files = [
            ('query', (None, query_str)),
        ]

        for blob in blob_array:
            files.append(('blobs', blob))

        # Set Auth token, only when not authenticated before
        if self.shared_data.session:
            headers = {'Authorization': "Bearer " +
                       self.shared_data.session.session_token}
        else:
            headers = None
        tries = 0
        response = SimpleNamespace()
        response.status_code = 0
        while tries < 3:
            tries += 1
            response = self.http_session.post(self.url,
                                              headers = headers,
                                              files   = files,
                                              verify  = self.use_ssl)
            if response.status_code == 200:
                # Parse response:
                json_response       = json.loads(response.text)
                import base64
                response_blob_array = [base64.b64decode(
                    b) for b in json_response['blobs']]
                self.last_response  = json_response["json"]
                break
            logger.error(
                f"Response not OK = {response.status_code} {response.text[:1000]}\n\
                    attempt [{tries}/3] .. PID = {os.getpid()}")
            time.sleep(1)

        if tries == 3:
            raise Exception(
                f"Could not query apertureDB {self.config} using REST.")
        return (self.last_response, response_blob_array)
