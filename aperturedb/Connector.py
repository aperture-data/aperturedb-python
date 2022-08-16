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

from . import queryMessage_pb2
import sys
import os
import socket
import struct
import time
import json
import ssl
import logging

from dataclasses import dataclass

logger = logging.getLogger(__name__)


PROTOCOL_VERSION = 1


@dataclass
class Session():

    session_token:      str
    refresh_token:      str
    session_token_ttl:  int
    refresh_token_ttl:  int
    session_started:    time.time = time.time()

    def valid(self) -> bool:
        session_age = time.time() - self.session_started

        if session_age > self.session_token_ttl:
            return False

        return True


class Connector(object):
    """
    .. _connector-label:

    **Class to facilitate connections with an instance of aperturedb**

    It lets the client execute any query as per the `Native Query specs <https://docs.aperturedata.io/>`_

    Args:
        str (host): Address of the host to connect to.
        int (port): Port to connect to.
        str (user): Username to specify while establishing a connection.
        str (password): Password to specify while connecting to the db.
        str (token): Token to use while connecting to the database.
        object (session):
        bool (use_ssl): Use SSL to encrypt communication with the database.
    """

    def __init__(self, host="localhost", port=55555,
                 user="", password="", token="",
                 session=None,
                 use_ssl=True):

        self.use_ssl = use_ssl

        self.host = host
        self.port = port

        self.connected = False
        self.last_response   = ''
        self.last_query_time = 0

        self.session = None

        self._connect()

        if session:
            self.session = session
        else:
            try:
                self._authenticate(user, password, token)
            except Exception as e:
                raise Exception("Authentication failed:", str(e))

    def __del__(self):

        self.conn.close()
        self.connected = False

    def _send_msg(self, data):

        sent_len = struct.pack('@I', len(data))  # send size first
        self.conn.send(sent_len)
        self.conn.send(data)

    def _recv_msg(self):

        recv_len = self.conn.recv(4)  # get message size

        recv_len = struct.unpack('@I', recv_len)[0]
        response = b''
        while len(response) < recv_len:
            packet = self.conn.recv(recv_len - len(response))
            if not packet:
                logger.error("Error receiving")
            response += packet

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

        session_info = response[0]["Authenticate"]
        if session_info["status"] != 0:
            raise Exception(session_info["info"])

        self.session = Session(session_info["session_token"],
                               session_info["refresh_token"],
                               session_info["session_token_expires_in"],
                               session_info["refresh_token_expires_in"],
                               )

    def _check_session_status(self):

        if not self.session:
            return

        if not self.session.valid():
            self._refresh_token()

    def _refresh_token(self):

        query = [{
            "RefreshToken": {
                "refresh_token": self.session.refresh_token
            }
        }]

        response, _ = self._query(query, [])

        session_info = response[0]["RefreshToken"]
        if session_info["status"] != 0:
            raise Exception(session_info["info"])

        self.session = Session(session_info["session_token"],
                               session_info["refresh_token"],
                               session_info["session_token_expires_in"],
                               session_info["refresh_token_expires_in"],
                               )

    def _connect(self):

        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

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
            logger.error(f"Error connecting to server: {str(e)}")
            self.conn.close()
            self.connected = False
            raise ConnectionError("Failed to connect to ApertureDB")

        self.connected = True

    def _query(self, query, blob_array = []):

        # Check the query type
        if not isinstance(query, str):  # assumes json
            query_str = json.dumps(query)
        else:
            query_str = query

        if not self.connected:
            return "NOT CONNECTED"

        query_msg = queryMessage_pb2.queryMessage()
        # query has .json and .blobs
        query_msg.json = query_str

        # Set Auth token, only when not authenticated before
        if self.session:
            query_msg.token = self.session.session_token

        for blob in blob_array:
            query_msg.blobs.append(blob)

        # Serialize with protobuf and send
        data = query_msg.SerializeToString()
        self._send_msg(data)

        response = self._recv_msg()

        querRes = queryMessage_pb2.queryMessage()
        querRes.ParseFromString(response)

        response_blob_array = [b for b in querRes.blobs]

        self.last_response = json.loads(querRes.json)

        return (self.last_response, response_blob_array)

    def query(self, q, blobs=[]):

        self._check_session_status()

        try:
            start = time.time()
            self.response, self.blobs = self._query(q, blobs)
            self.last_query_time = time.time() - start
            return self.response, self.blobs
        except BaseException as e:
            print(e)
            raise ConnectionError("ApertureDB disconnected")

    def create_new_connection(self):

        return Connector(self.host, self.port, session=self.session)

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
