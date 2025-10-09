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

from types import SimpleNamespace
from typing import Optional
from aperturedb.Connector import Connector
from aperturedb.Configuration import Configuration
from requests.adapters import HTTPAdapter
import ssl

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = 1


class CustomHTTPAdapter(HTTPAdapter):
    def __init__(self, ca_cert: Optional[str]):
        self.ca_cert = ca_cert
        super().__init__()

    def init_poolmanager(self, *args, **kwargs):
        # this creates a default context with secure default settings,
        # which enables server certficiate verification using the
        # system's default CA certificates
        context = ssl.create_default_context()
        if self.ca_cert:
            context.load_verify_locations(cafile=self.ca_cert)

        # alternatively, you could create your own context manually
        # but this does NOT enable server certificate verification
        # context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

        super().init_poolmanager(*args, **kwargs, ssl_context=context)


class ConnectorRest(Connector):
    """
    **Class to use ApertureDB's REST interface**

    This class is used to connect to ApertureDB using the REST interface.

    Args:
        str (host): Address of the host to connect to.
        int (port): Port to connect to.
        str (user): Username to specify while establishing a connection.
        str (password): Password to specify while connecting to ApertureDB.
        str (token): Token to use while connecting to the database.
        bool (use_ssl): Use SSL to encrypt communication with the database.
        str (key): Apeture Key, configuration as a deflated compressed string
    """

    def __init__(self, host="localhost", port=None,
                 user="", password="", token="",
                 use_ssl=True, ca_cert=None, verify_hostname=True, shared_data=None,
                 config: Optional[Configuration] = None,
                 key: Optional[str] = None):
        self.use_keepalive = False
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            token=token,
            use_ssl=use_ssl,
            ca_cert=ca_cert,
            verify_hostname=verify_hostname,
            shared_data=shared_data,
            config=config,
            key=key)

        # A Convenience feature to not require the port
        # Relies on common ports for http and https, but can be overriden
        if port is None:
            if config is None:
                self.port = 443 if use_ssl else 80
            else:
                self.port = config.port
        else:
            self.port = port

        self.connected = False
        # Session is useful because it does not add "Connection: close header"
        # Since we will be making same call to the same URL, making a session
        # REF: https://requests.readthedocs.io/en/latest/user/advanced/
        self.http_session = requests.Session()
        if self.config.verify_hostname:
            if self.config.ca_cert:
                adapter = CustomHTTPAdapter(ca_cert=self.config.ca_cert)
            else:
                adapter = CustomHTTPAdapter(ca_cert=None)
            self.http_session.mount('https://', adapter=adapter)

        self.last_response   = ''
        self.last_query_time = 0

        self.url = ('https' if self.use_ssl else 'http') + \
            '://' + self.host + ':' + str(self.port) + '/api/'

    def __del__(self):
        logger.info("Done with connector REST.")
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
        if self.shared_data.session and self.shared_data.session.valid():
            headers = {'Authorization': "Bearer " +
                       self.shared_data.session.session_token}
        else:
            headers = None
        tries = 0
        response = SimpleNamespace()
        response.status_code = 0
        while tries < self.config.retry_max_attempts:
            tries += 1
            try:
                # URL takes care of the scheme
                response = self.http_session.post(self.url,
                                                  headers = headers,
                                                  files   = files,
                                                  verify  = self.config.use_ssl and self.config.verify_hostname)
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
            except Exception as e:
                logger.error(f"Exception during http.post = {e}\n\
                        attempt [{tries}/3] .. PID = {os.getpid()}")

            time.sleep(self.config.retry_interval_seconds)

            self.connect()
            if try_resume:
                self._renew_session()

        if tries == self.config.retry_max_attempts:
            raise Exception(
                f"Could not query ApertureDB {self.config} using REST.")
        return (self.last_response, response_blob_array)

    def _connect(self):
        logger.info("Connecting to ApertureDB using REST")
        self.connected = True
