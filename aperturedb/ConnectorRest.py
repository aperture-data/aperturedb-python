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
from dataclasses import replace
import os
import requests
import time
import json
import logging

from typing import Optional
try:
    import pybase64 as base64
except ImportError:
    import base64

from aperturedb.Connector import Connector
from aperturedb.Configuration import Configuration
from aperturedb._query_wire import (
    BufferedInput, ProtocolError, HELLO_VERSION, HELLO_HEADER,
    LEGACY_HELLO_VERSION, CHUNK_SIZE, buffered_response, multipart_response)
from requests.adapters import HTTPAdapter
import ssl

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = HELLO_VERSION


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
        host (str): Address of the host to connect to.
        port (int): Port to connect to.
        user (str): Username to specify while establishing a connection.
        password (str): Password to specify while connecting to ApertureDB.
        token (str): Token to use while connecting to the database.
        use_ssl (bool): Use SSL to encrypt communication with the database.
        key (str): Aperture Key, configuration as a deflated compressed string
    """

    def __init__(self, host="localhost", port=None,
                 user="", password="", token="",
                 use_ssl=True, ca_cert=None, verify_hostname=True, shared_data=None,
                 config: Optional[Configuration] = None,
                 key: Optional[str] = None,
                 authenticate: bool = True):
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
            authenticate=authenticate,
            config=config,
            key=key)

        # Resolve HTTP defaults after key/config precedence has been applied.
        if self.port is None:
            self.port = Configuration.config_default_port(
                use_rest=True, use_ssl=self.use_ssl)
        self.config = replace(self.config, port=self.port, use_rest=True)

        self.connected = False
        # Session is useful because it does not add "Connection: close header"
        # Since we will be making same call to the same URL, making a session
        # REF: https://requests.readthedocs.io/en/latest/user/advanced/
        self.http_session = None
        self._init_session()

        self.last_response   = ''
        self.last_query_time = 0

        self.url = ('https' if self.use_ssl else 'http') + \
            '://' + self.host + ':' + str(self.port) + '/api/'

    def _init_session(self):
        self.http_session = requests.Session()
        if self.config.verify_hostname:
            if self.config.ca_cert:
                adapter = CustomHTTPAdapter(ca_cert=self.config.ca_cert)
            else:
                adapter = CustomHTTPAdapter(ca_cert=None)
            self.http_session.mount('https://', adapter=adapter)

    def close(self):
        if hasattr(self, 'http_session') and self.http_session is not None:
            self.http_session.close()
            self.http_session = None
        super().close()

    def __del__(self):
        self.close()

    def _http_input(self, response):
        chunks = iter(response.iter_content(chunk_size=CHUNK_SIZE))

        def receive(_):
            for chunk in chunks:
                if chunk:
                    return chunk
            return b''

        return BufferedInput(receive)

    def _post(self, query_str, blobs, version, authenticate=True):
        if getattr(self, 'http_session', None) is None:
            self._init_session()
        headers = {HELLO_HEADER: str(version)}
        session = self.shared_data.session
        if authenticate and session is not None:
            headers['Authorization'] = "Bearer " + session.session_token
        files = [('query', (None, query_str, 'application/json'))]
        files.extend(('blobs', ('blobs', blob, 'application/octet-stream'))
                     for blob in blobs)
        return self.http_session.post(
            self.url, headers=headers, files=files, stream=True,
            allow_redirects=False,
            verify=self.config.use_ssl and self.config.verify_hostname)

    def _read_http_response(self, response, release):
        def finished(success):
            try:
                response.close()
            except BaseException:
                release(False)
                raise
            else:
                release(success)

        if response.status_code != 200:
            raise ProtocolError(
                f"HTTP query was rejected with status {response.status_code}")
        header = response.headers.get(HELLO_HEADER)
        source = self._http_input(response)
        limit = self._message_limit()
        if header == str(HELLO_VERSION):
            self._hello_version = HELLO_VERSION
            return multipart_response(
                source, response.headers.get('Content-Type', ''), limit, finished)
        if header not in (None, str(LEGACY_HELLO_VERSION)):
            raise ProtocolError("Unsupported HTTP hello version")
        # 0.20 gateways have no hello header. Only a valid legacy envelope
        # identifies that protocol; HTML/proxy errors never trigger fallback.
        try:
            envelope = json.loads(source.all(limit))
            if not isinstance(envelope, dict) or not {'json', 'blobs'} <= envelope.keys():
                raise ValueError("Not a legacy response envelope")
            if not isinstance(envelope['json'], (dict, list)) or not isinstance(envelope['blobs'], list):
                raise ValueError("Invalid legacy response envelope")
            blobs = [base64.b64decode(blob, validate=True)
                     for blob in envelope['blobs']]
        except (TypeError, ValueError, UnicodeError) as error:
            raise ProtocolError(
                "Invalid legacy HTTP query response") from error
        self._hello_version = LEGACY_HELLO_VERSION
        return buffered_response(envelope['json'], blobs, finished)

    def _start_query_response(self, query_str, blobs, release):
        response = self._post(query_str, blobs, self._hello_version)
        try:
            return self._read_http_response(response, release)
        except BaseException:
            response.close()
            raise

    def _connect(self):
        # A harmless empty query discovers HTTP hello support before helpers
        # choose version-specific command forms. It never authenticates or
        # performs database writes, and its result is discarded.
        response = self._post('[]', [], HELLO_VERSION, authenticate=False)
        try:
            stream = self._read_http_response(response, lambda _: None)
            with stream:
                try:
                    next(stream.blobs)
                except StopIteration:
                    pass
                else:
                    raise ProtocolError(
                        "Protocol discovery returned unexpected blobs")
            self.connected = True
            self._ever_connected = True
        except BaseException:
            response.close()
            self._close_transport()
            raise
