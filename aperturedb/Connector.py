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
from . import queryMessage_pb2
import sys
import os
import socket
import struct
import time
import json
import ssl
import logging

import keepalive

from threading import Lock
from types import SimpleNamespace
from dataclasses import dataclass
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


class Connector(object):
    """
    **Class to facilitate connections with an instance of aperturedb**

    It lets the client execute any JSON query based on the [ApertureDB query language specification](/query_language/Overview/API%20Description)

    Args:
        str (host): Address of the host to connect to.
        int (port): Port to connect to.
        str (user): Username to specify while establishing a connection.
        str (password): Password to specify while connecting to the db.
        str (token): Token to use while connecting to the database.
        bool (use_ssl): Use SSL to encrypt communication with the database.
        bool (use_keepalive): Set keepalive on the connection with the database.
            This has two benefits: It reduces the chance of disconnection for a long-running query,
            and it means that diconnections are detected sooner.
            Turn this off to reduce traffic on high-cost network connections.
        Configuration (config): Configuration object to use for connection.
    """

    def __init__(self, host="localhost", port=55555,
                 user="", password="", token="",
                 use_ssl=True, shared_data=None, authenticate=True,
                 use_keepalive=True,
                 retry_connect_interval_seconds=1,
                 retry_connect_max_attempts=3,
                 config: Configuration = None):
        self.connected = False
        self.last_response   = ''
        self.last_query_time = 0

        if config is None:
            self.host = host
            self.port = port
            self.use_ssl = use_ssl
            self.use_keepalive = use_keepalive
            # Create a configuration object, to show better error messages
            self.config = Configuration(
                host=self.host,
                port=self.port,
                use_ssl=self.use_ssl,
                username=user,
                password=password,
                name="runtime",
                use_keepalive=use_keepalive,
                retry_connect_interval_seconds=retry_connect_interval_seconds,
                retry_connect_max_attempts=retry_connect_max_attempts
            )
        else:
            self.config = config
            self.host = config.host
            self.port = config.port
            self.use_ssl = config.use_ssl
            self.use_keepalive = config.use_keepalive

        self.conn = None
        connect_attempts = 1
        while True:
            self.connect(
                details=f"Will retry in {self.config.retry_connect_interval_seconds} seconds")
            if self.connected or connect_attempts == self.config.retry_connect_max_attempts:
                break
            time.sleep(self.config.retry_connect_interval_seconds)
            connect_attempts += 1

        if not self.connected:
            raise Exception(
                f"Could not connect to apertureDB server: {self.config}")

        if authenticate:
            self.authenticate(shared_data, user, password, token)

    def authenticate(self, shared_data, user, password, token):
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
        if self.connected:
            self.conn.close()
            self.connected = False

    def _send_msg(self, data):
        sent_len = struct.pack('@I', len(data))  # send size first
        x = self.conn.send(sent_len + data)
        return x == len(data) + 4

    def _recv_msg(self):
        recv_len = self.conn.recv(4)  # get message size
        if recv_len == b'':
            return None
        recv_len = struct.unpack('@I', recv_len)[0]
        response = bytearray(recv_len)
        read = 0
        while read < recv_len:
            packet = self.conn.recv(recv_len - read)
            if recv_len == b'':
                return None
            if not packet:
                logger.error("Error receiving")
                return None
            response[read:] = packet
            read += len(packet)

        return response

    def _authenticate(self, user, password="", token=""):

        query = [{
            "Authenticate": {
                "username": user,
            }
        }]

        if password:
            query[0]["Authenticate"]["password"] = password
        elif token:
            query[0]["Authenticate"]["token"] = token
        else:
            raise Exception(
                "Either password or token must be specified for authentication")

        response, _ = self._query(query)

        if not isinstance(response, (list, tuple)) or not "Authenticate" in response[0]:
            raise Exception(
                "Unexpected response from server upon authenticate request: " + str(response))
        session_info = response[0]["Authenticate"]
        if session_info["status"] != 0:
            raise Exception(session_info["info"])

        self.shared_data.session = Session(
            session_info["session_token"],
            session_info["refresh_token"],
            session_info["session_token_expires_in"],
            session_info["refresh_token_expires_in"],
            time.time())

    def _check_session_status(self):
        if not self.shared_data.session:
            return

        if not self.shared_data.session.valid():
            with self.shared_data.lock:
                self._refresh_token()

    def _refresh_token(self):
        query = [{
            "RefreshToken": {
                "refresh_token": self.shared_data.session.refresh_token
            }
        }]

        response, _ = self._query(query, [], try_resume=False)

        logger.info(f"Refresh token response: \r\n{response}")
        if isinstance(response, list):
            session_info = response[0]["RefreshToken"]
            if session_info["status"] != 0:
                raise UnauthorizedException(response)

            self.shared_data.session = Session(
                session_info["session_token"],
                session_info["refresh_token"],
                session_info["session_token_expires_in"],
                session_info["refresh_token_expires_in"],
                time.time())
        else:
            raise UnauthorizedException(response)

    def _connect(self):

        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        if self.use_keepalive:
            keepalive.set(self.conn)

        # TCP_QUICKACK only supported in Linux 2.4.4+.
        # We use startswith for checking the platform following Python's
        # documentation:
        # https://docs.python.org/dev/library/sys.html#sys.platform
        if sys.platform.startswith('linux'):
            self.conn.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)

        try:
            self.conn.connect((self.host, self.port))

            # Handshake with server to negotiate protocol

            protocol = 2 if self.use_ssl else 1

            hello_msg = struct.pack('@II', PROTOCOL_VERSION, protocol)

            # Send desire protocol
            self._send_msg(hello_msg)

            # Receive response from server
            response = self._recv_msg()

            version, server_protocol = struct.unpack('@II', response)

            if version != PROTOCOL_VERSION:
                logger.warning("Protocol version differ from server")

            if server_protocol != protocol:
                self.conn.close()
                self.connected = False
                raise Exception(
                    "Server did not accept protocol. Aborting Connection.")

            if self.use_ssl:

                # Server is ok with SSL, we switch over SSL.
                self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                self.context.check_hostname = False
                # TODO, we need to add support for local certificates
                # For now, we let the server send us the certificate
                self.context.verify_mode = ssl.VerifyMode.CERT_NONE
                self.conn = self.context.wrap_socket(self.conn)

        except BaseException as e:
            logger.error(f"Error connecting to server: {str(e)} {self.config}")
            self.conn.close()
            self.connected = False
            raise

        self.connected = True

    def connect(self, details: str = None):
        self.connected = False
        if not self.connected:
            # Connection is not established, keep trying.
            # nothing else will work until this is done.
            # This can help to get going amidst server restarts.
            try:
                self._connect()
            except socket.error as e:
                logger.error(
                    f"Error connecting to server: {self.config} \r\n{details}.", exc_info=True, stack_info=True)

    def _query(self, query, blob_array = [], try_resume=True):
        response_blob_array = []
        # Check the query type
        if not isinstance(query, str):  # assumes json
            query_str = json.dumps(query)
        else:
            query_str = query

        query_msg = queryMessage_pb2.queryMessage()
        # query has .json and .blobs
        query_msg.json = query_str

        # Set Auth token, only when not authenticated before
        if self.shared_data.session:
            query_msg.token = self.shared_data.session.session_token

        for blob in blob_array:
            query_msg.blobs.append(blob)

        # Serialize with protobuf and send
        data = query_msg.SerializeToString()

        # this is for session refresh attempts
        tries = 0
        while tries < 3:
            try:
                if self._send_msg(data):
                    response = self._recv_msg()
                    if response is not None:
                        querRes = queryMessage_pb2.queryMessage()
                        querRes.ParseFromString(response)
                        response_blob_array = [b for b in querRes.blobs]
                        self.last_response = json.loads(querRes.json)
                        break
            except ssl.SSLError as ssle:
                # This can happen in a scenario where multiple
                # processes might be accessing a single connection.
                # The copy does not make usable connections.
                logger.exception(ssle)
                logger.warning(f"SSL error on process {os.getpid()}")
            except OSError as ose:
                logger.exception(ose)
                logger.warning(f"OS error on process {os.getpid()}")
            tries += 1
            logger.warning(
                f"Connection broken. Reconnectng attempt [{tries}/{self.config.retry_connect_max_attempts}] .. PID = {os.getpid()}")
            self.conn.close()
            self.connected = False

            self.connect(
                details=f"Will retry in {self.config.retry_connect_interval_seconds} seconds")
            time.sleep(self.config.retry_connect_interval_seconds)

            # Try to resume the session, in cases where the connection is severed.
            # For example aperturedb server is restarted, or network is lost.
            # While this is useful bit of code, when executed in a refresh token
            # path, this can cause a deadlock. Hence the try_resume flag.
            if try_resume:
                self._renew_session()
        if tries == self.config.retry_connect_max_attempts:
            raise Exception(
                f"Could not query apertureDB using TCP.")
        return (self.last_response, response_blob_array)

    def query(self, q, blobs=[]):
        """
        Query the database with a query string or a json object.
        First it checks if the session is valid, if not, it refreshes the token.
        Then it sends the query to the server and returns the response.

        Args:
            q (json): native query to be sent
            blobs (list, optional): Blobs if needed with the query. Defaults to [].

        Raises:
            ConnectionError: Fatal error, connection to server lost

        Returns:
            _type_: _description_
        """
        self._renew_session()
        try:
            start = time.time()
            self.response, self.blobs = self._query(q, blobs)
            if not isinstance(
                    self.response,
                    list) and self.response["info"] == "Not Authenticated!":
                # The case where session is valid, but expires while query is sent.
                # Hope is that the query send won't be longer than the session
                # ttl.
                logger.warning(
                    f"Session expired while query was sent. Retrying... {self.config}", stack_info=True)
                self._renew_session()
                start = time.time()
                self.response, self.blobs = self._query(q, blobs)
            self.last_query_time = time.time() - start
            return self.response, self.blobs
        except BaseException as e:
            logger.critical("Failed to query", exc_info=True, stack_info=True)
            raise

    def _renew_session(self):
        count = 0
        while count < 3:
            try:
                self._check_session_status()
                break
            except UnauthorizedException as e:
                logger.warning(
                    f"[Attempt {count + 1} of 3] Failed to refresh token.", exc_info=True, stack_info=True)
                time.sleep(1)
                count += 1

    def create_new_connection(self) -> Connector:
        return type(self)(
            self.host,
            self.port,
            use_ssl=self.use_ssl,
            shared_data=self.shared_data)

    def get_last_response_str(self):

        return json.dumps(self.last_response, indent=4, sort_keys=False)

    def print_last_response(self):

        print(self.get_last_response_str())

    def get_last_query_time(self):

        return self.last_query_time

    def get_response(self):

        return self.response

    def get_blobs(self):

        return self.blobs

    # Return a flag on whether the last query succeeded:
    # i.e., all commands return status >= 0
    def last_query_ok(self):

        return self.check_status(self.response) >= 0

    def check_status(self, json_res):

        status = 0
        if (isinstance(self.response, dict)):
            if ("status" not in self.response):
                status = self.check_status(json_res[list(json_res.keys())[0]])
            else:
                status = json_res["status"]
        elif (isinstance(json_res, (tuple, list))):
            if ("status" not in json_res[0]):
                status = self.check_status(json_res[0])
            else:
                status = json_res[0]["status"]

        return status
