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
from typing import Optional, Any
import sys
import os
import socket
import struct
import time
import json
import ssl
import logging
from datetime import datetime, timedelta

import keepalive

from threading import Lock
from types import SimpleNamespace
from dataclasses import dataclass
from contextlib import contextmanager

from aperturedb._query_wire import (
    BufferedInput, ProtocolError, QueryResponse, HELLO_VERSION,
    MAX_BLOBS, native_response)
from aperturedb.Configuration import Configuration
from aperturedb.LoggingUtils import censor_tokens

logger = logging.getLogger(__name__)


PROTOCOL_VERSION = HELLO_VERSION

# Protocol types
PROTOCOL_TCP = 1
PROTOCOL_SSL = 2

DEFAULT_PORT = 55555
LEGACY_NATIVE_HINT = "For ApertureDB 0.20, use HTTPS."
# aperturedb's param ADB_MAX_CONNECTION_MESSAGE_SIZE_MB = 2048 by default
DEFAULT_MAX_MESSAGE_SIZE_MB = 2048

# Query retry constants
DEFAULT_RETRY_INTERVAL_SECONDS = 1
DEFAULT_RETRY_MAX_ATTEMPTS = 3
DEFAULT_SESSION_EXPIRY_OFFSET_SEC = 10
DEFAULT_QUERY_CONNECTION_ERROR_SUPPRESSION_DELTA_SEC = 30

# Session renewal constants
RENEW_SESSION_MAX_ATTEMPTS = 3
RENEW_SESSION_RETRY_INTERVAL_SEC = 1

# Status codes
STATUS_OK = 0
STATUS_ERROR_DEFAULT = -2

SETUP_URL = "https://docs.aperturedata.dev/Setup/server/Local#run-aperturedb-along-with-webui-using-docker-compose"


class UnauthorizedException(Exception):
    pass


class UnauthenticatedException(Exception):
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
                int(os.getenv("SESSION_EXPIRY_OFFSET_SEC", DEFAULT_SESSION_EXPIRY_OFFSET_SEC)):
            return False

        return True


class Connector(object):
    """
    **Class to facilitate connections with an instance of ApertureDB**

    It lets the client execute any JSON query based on the [ApertureDB query language specification](/query_language/Overview/API%20Description)
    It manages the TCP connection to the database.

    !!! note
        - The connection is established only when a query is run.
        - A new connection is established for each instance that runs a query, and gets closed explicitly by calling `close()` or at destruction.

    Args:
        host (str): Address of the host to connect to.
        port (int): Port to connect to.
        user (str): Username to specify while establishing a connection.
        password (str): Password to specify while connecting to ApertureDB.
        token (str): Token to use while connecting to the database.
        use_ssl (bool): Use SSL to encrypt communication with the database.
        use_keepalive (bool): Set keepalive on the connection with the database.
            This has two benefits: It reduces the chance of disconnection for a long-running query,
            and it means that disconnections are detected sooner.
            Turn this off to reduce traffic on high-cost network connections.
        config (Configuration): Configuration object to use for connection.
        key (str): Aperture Key, configuration as a deflated compressed string

    The Configuration class options:
    host,port,user,password,token,use_ssl,use_keepalive,retry_interval_seconds,retrun_max_attempts
    are all ignored in the initializer if a config or key is passed in.

    If a key is passed in, it is chosen over a config.

    shared_data and authenticate are not Configuration class based options, and
    have no overrides.

    """

    def __init__(self, host="localhost", port=DEFAULT_PORT,
                 user="", password="", token="",
                 use_ssl=True,
                 ca_cert=None,
                 shared_data=None,
                 authenticate=True,
                 use_keepalive=True,
                 verify_hostname=True,
                 retry_interval_seconds=DEFAULT_RETRY_INTERVAL_SECONDS,
                 retry_max_attempts=DEFAULT_RETRY_MAX_ATTEMPTS,
                 config: Optional[Configuration] = None,
                 key: Optional[str] = None,
                 connect: bool = False):
        """
        Constructor for the Connector class.
        """
        self.connected = False
        self.last_response = ''
        self.last_query_time = 0
        self.authenticated = False
        self.last_query_timestamp = None
        # suppress connection warnings which occur more than this time
        # after the last query
        self.query_connection_error_suppression_delta = timedelta(
            seconds=DEFAULT_QUERY_CONNECTION_ERROR_SUPPRESSION_DELTA_SEC)

        # we either have a config ( as a key or as object ) or not.
        if key is not None or config is not None:
            if key is not None:
                self.config = Configuration.reinflate(key)
            else:
                self.config = config
        else:
            # Create a configuration object, to show better error messages
            self.config = Configuration(
                host=host,
                port=port,
                use_ssl=use_ssl,
                ca_cert=ca_cert,
                verify_hostname=verify_hostname,
                username=user,
                password=password,
                name="runtime",
                token=token,
                use_keepalive=use_keepalive,
                retry_interval_seconds=retry_interval_seconds,
                retry_max_attempts=retry_max_attempts
            )

        # now we do.
        self.host = self.config.host
        self.port = self.config.port
        self.use_ssl = self.config.use_ssl
        self.use_keepalive = self.config.use_keepalive
        self.token = self.config.token
        self.conn = None
        self._hello_version = None
        self._wire_input = None
        self._query_lock = Lock()
        self._query_lease = None
        self._active_response = None

        if shared_data is None:
            self.shared_data = SimpleNamespace()
            self.shared_data.session = None
            self.shared_data.lock = Lock()
        else:
            self.shared_data = shared_data

        self.should_authenticate = authenticate
        # One time flag to indicate if we ever connected,
        # to prevent logging of connection errors on first connect.
        self._ever_connected = False

        if connect:
            self.connect()

    def authenticate(self, shared_data, user, password, token):
        """
        Authenticate if this connector and its clones have no shared session.
        Called automatically by query().
        """
        self.shared_data = shared_data
        with shared_data.lock:
            if shared_data.session is None:
                self._authenticate(user, password, token)
            self.authenticated = True
            return shared_data.session

    @property
    def hello_version(self):
        self._ensure_connection()
        return self._hello_version

    def _close_transport(self):
        connection, self.conn = getattr(self, 'conn', None), None
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass
        self.connected = False
        self._hello_version = None
        self._wire_input = None

    def close(self):
        response = getattr(self, '_active_response', None)
        if response is not None and not response.closed:
            response.abort()
        self._close_transport()

    def __del__(self):
        self.close()

    @staticmethod
    def _message_limit():
        limit = int(os.getenv("MAX_MESSAGE_SIZE_MB",
                    DEFAULT_MAX_MESSAGE_SIZE_MB))
        if limit <= 0:
            raise ValueError("MAX_MESSAGE_SIZE_MB must be positive")
        return limit << 20

    @staticmethod
    def _exchange_hello(connection, protocol):
        header = struct.Struct('<III')
        reply = bytearray()
        try:
            connection.sendall(header.pack(8, HELLO_VERSION, protocol))
            while len(reply) < header.size:
                packet = connection.recv(header.size - len(reply))
                if not packet:
                    raise ProtocolError(
                        f"Incomplete native hello reply. {LEGACY_NATIVE_HINT}")
                reply.extend(packet)
        except (BrokenPipeError, ConnectionResetError, TimeoutError) as error:
            raise ConnectionError(
                f"Native hello failed ({error}). {LEGACY_NATIVE_HINT}") from error
        return header.unpack(reply)

    def _authenticate(self, user, password="", token=""):

        query = [{
            "Authenticate": {
            }
        }]

        if user is not None:
            query[0]["Authenticate"]["username"] = user

        if password:
            query[0]["Authenticate"]["password"] = password
        elif token:
            query[0]["Authenticate"]["token"] = token
        else:
            raise Exception(
                "Either password or token must be specified for authentication")

        # Authentication runs under the shared session lock. A transport
        # reconnect must not try to renew the session and acquire it again.
        response, _ = self._query(query, try_resume=False)

        session_info = None
        if isinstance(response, (list, tuple)) and response and isinstance(response[0], dict):
            session_info = response[0].get("Authenticate")
        if not isinstance(session_info, dict) or "status" not in session_info:
            raise ProtocolError(
                "Unexpected response from server upon authenticate request")
        if session_info["status"] != STATUS_OK:
            raise Exception(session_info["info"])

        self.shared_data.session = Session(
            session_info["session_token"],
            session_info["refresh_token"],
            session_info["session_token_expires_in"],
            session_info["refresh_token_expires_in"],
            time.time())

    @staticmethod
    def _is_session_rejected(response):
        # This transaction-level rejection guarantees commands did not run.
        # Command errors, including permission errors, must not be retried.
        if not isinstance(response, dict) or response.get("status") != -1:
            return False
        if "code" in response:
            return response["code"] == "unauthenticated"
        return response.get("info") == "Not Authenticated!"

    @staticmethod
    def _is_authentication_query(query):
        # A rejected explicit login or refresh is a credential failure. Those
        # commands do not use the request's current session for authorization.
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except (TypeError, ValueError):
                return False
        return isinstance(query, (list, tuple)) and len(query) == 1 and \
            isinstance(query[0], dict) and \
            any(name in query[0] for name in ("Authenticate", "RefreshToken"))

    def _check_session_status(self, rejected_session=None):
        with self.shared_data.lock:
            session = self.shared_data.session
            if session is not None and (session is rejected_session or
                                        not session.valid()):
                # Another connector may have renewed this shared session while
                # we waited for the lock or for a rejected query's response.
                self._refresh_token()

    def _refresh_token(self):
        query = [{
            "RefreshToken": {
                "refresh_token": self.shared_data.session.refresh_token
            }
        }]

        response, _ = self._query(query, [], try_resume=False)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Refresh token response: \r\n{censor_tokens(response)}")
        session_info = None
        if isinstance(response, (list, tuple)) and len(response) == 1 and \
                isinstance(response[0], dict):
            session_info = response[0].get("RefreshToken")

        if self._is_session_rejected(response) or (
                isinstance(session_info, dict) and
                session_info.get("status") is not None and
                session_info["status"] != STATUS_OK):
            # Revocation and session rotation can invalidate refresh tokens before
            # their TTL expires. Both transaction and command rejection formats
            # require login with the configured password or personal token.
            self.authenticated = False
            self.shared_data.session = None
            # The caller already holds the shared lock.
            self._authenticate(self.config.username,
                               self.config.password, self.token)
            self.authenticated = True
            return

        if not isinstance(session_info, dict) or \
                session_info.get("status") != STATUS_OK:
            raise UnauthorizedException(censor_tokens(response))

        self.shared_data.session = Session(
            session_info["session_token"],
            session_info["refresh_token"],
            session_info["session_token_expires_in"],
            session_info["refresh_token_expires_in"],
            time.time())

    def _build_ssl_context(self):
        """
        Builds an SSL context for the connection.
        There are 3 scenarios:
        1. verify_hostname is False, we don't verify the hostname
        2. verify_hostname is True and ca_cert is provided, we verify the hostname using the provided ca_cert
        3. verify_hostname is True and ca_cert is not provided, we verify the hostname using the system's default CA certificates
        """
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if not self.config.verify_hostname:
            self.context.check_hostname = False
            self.context.verify_mode = ssl.CERT_NONE
        elif self.config.ca_cert:
            self.context.load_verify_locations(
                cafile=self.config.ca_cert
            )
        else:
            self.context.load_default_certs(ssl.Purpose.SERVER_AUTH)
        return self.context

    def _connect(self):
        protocol = PROTOCOL_SSL if self.use_ssl else PROTOCOL_TCP
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn = connection
        try:
            connection.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            if self.use_keepalive:
                keepalive.set(connection)
            if sys.platform.startswith('linux'):
                connection.setsockopt(
                    socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            connection.settimeout(10)
            connection.connect((self.host, self.port))
            size, supported, selected = self._exchange_hello(
                connection, protocol)
            if size != 8:
                raise ProtocolError("Invalid server hello")
            if supported != HELLO_VERSION:
                raise ProtocolError(
                    f"Native transport requires hello version {HELLO_VERSION} "
                    f"(server reported {supported}). {LEGACY_NATIVE_HINT}")
            if selected != protocol:
                raise ProtocolError(
                    "Server did not accept the requested transport")
            if self.use_ssl:
                context = self._build_ssl_context()
                connection = context.wrap_socket(
                    connection,
                    server_hostname=self.host if self.config.verify_hostname else None)
                self.conn = connection
            connection.settimeout(None)
            self._hello_version = supported
            self._wire_input = BufferedInput(connection.recv)
            self.connected = True
            self._ever_connected = True
        except BaseException:
            self._close_transport()
            raise

    def _ensure_connection(self, details=None):
        if self.connected:
            return
        attempts = 0
        maximum = self.config.retry_max_attempts
        if not isinstance(maximum, int) or maximum < 0:
            raise ValueError(
                "retry_max_attempts must be a nonnegative integer")
        while not maximum or attempts < maximum:
            attempts += 1
            try:
                self._connect()
                return
            except (ProtocolError, ssl.SSLCertVerificationError, FileNotFoundError):
                raise
            except OSError as error:
                self._close_transport()
                if maximum and attempts >= maximum:
                    raise ConnectionError(
                        f"Could not connect to ApertureDB {self.config}; "
                        f"self.connected=False, attempts={attempts}: {error}") from error
                logger.warning("Connection attempt failed for %s%s", self.config,
                               f" ({details})" if details else "")
                time.sleep(self.config.retry_interval_seconds)

    def connect(self, details: str = None):
        if getattr(self, '_active_response', None) is not None:
            raise RuntimeError(
                "Finish or close the active query stream before reconnecting")
        self._close_transport()
        self._ensure_connection(details)

    def _prepare_query(self, query):
        # Helpers choose version-aware command forms before reaching transport.
        # Explicit user queries are never silently rewritten.
        return query

    def _start_query_response(self, query_str, blobs, release):
        session = self.shared_data.session
        token = "" if session is None else session.session_token
        limit = self._message_limit()
        token_json = json.dumps(token, ensure_ascii=False).encode('utf-8')
        query_json = query_str.encode('utf-8')
        size = len(token_json) + 1 + len(query_json) + 4
        size += sum(8 + len(blob) for blob in blobs)
        if len(blobs) > MAX_BLOBS or size > limit:
            raise ValueError("Query exceeds the configured message size limit")
        self.conn.sendall(token_json + b' ' + query_json)
        self.conn.sendall(struct.pack('!I', len(blobs)))
        for blob in blobs:
            self.conn.sendall(struct.pack('!Q', len(blob)))
            self.conn.sendall(blob)
        return native_response(self._wire_input, limit, release)

    def _open_query_stream(self, query, blob_array=None):
        if not self._query_lock.acquire(blocking=False):
            raise RuntimeError(
                "Finish or close the active query stream before another query")
        lease = object()
        self._query_lease = lease
        state = {'ready': False, 'outcome': None}

        def release(success):
            if not state['ready']:
                state['outcome'] = success
                return
            if self._query_lease is lease:
                self._active_response = None
                try:
                    if not success:
                        self._close_transport()
                finally:
                    self._query_lease = None
                    self._query_lock.release()

        try:
            self._ensure_connection("Initial connection from query")
            query = self._prepare_query(query)
            query_str = query if isinstance(
                query, str) else json.dumps(query, default=str)
            blobs = [] if blob_array is None else [
                bytes(blob) for blob in blob_array]
            try:
                response = self._start_query_response(
                    query_str, blobs, release)
            except OSError as error:
                if isinstance(error, ConnectionError):
                    raise
                raise ConnectionError(
                    "Query transport failed; the request was not replayed") from error
            self.last_response = response.json
            self._active_response = None if response.closed else response
            state['ready'] = True
            if state['outcome'] is not None:
                release(state['outcome'])
            return response
        except BaseException:
            state['ready'] = True
            release(False)
            raise

    def _query(self, query, blob_array=None, try_resume=True):
        # Session rejection can be retried by query(); uncertain transport
        # failures cannot. Even sendall() may have delivered a committed write.
        del try_resume
        with self._open_query_stream(query, blob_array) as response:
            return response.json, [blob.read() for blob in response.blobs]

    @contextmanager
    def query_stream(self, q, blobs=None):
        """Execute a query with JSON metadata and lazy ordered output blobs.

        Args:
            q (list | str): Command array or its JSON representation.
            blobs (list[bytes], optional): Ordered input blobs.

        Yields:
            QueryResponse: JSON metadata and ordered ``ResponseBlob`` objects.

        Example:
            ```python
            with db.query_stream(query) as result:
                print(result.json)
                for blob in result.blobs:
                    for chunk in blob.iter_chunks():
                        output.write(chunk)
            ```

        Consume each blob before advancing to the next. Exiting with unread
        bytes closes the connection. A late storage or network error raises
        ``ConnectionError``; valid JSON alone does not mean the response is
        complete. Exhaust ``result.blobs`` even when no blobs are expected,
        so the response terminator is checked. Another query is rejected while a stream
        remains active. Clone the connector for concurrent work.

        Queries are never replayed after uncertain transport failures.
        """
        manage_session = self.should_authenticate and not self._is_authentication_query(
            q)
        query_session = None
        if manage_session:
            self._renew_session()
            query_session = self.authenticate(
                shared_data=self.shared_data, user=self.config.username,
                password=self.config.password, token=self.token)
        started = time.time()
        response = self._open_query_stream(q, blobs)
        try:
            if manage_session and query_session is not None and self._is_session_rejected(response.json):
                # An authorization rejection guarantees no commands ran. It
                # must have no blobs and be complete before retrying safely.
                if not response.complete:
                    try:
                        next(response.blobs)
                    except StopIteration:
                        pass
                    else:
                        self.close()
                        raise ProtocolError(
                            "Session rejection included unexpected output blobs")
                self._renew_session(rejected_session=query_session)
                started = time.time()
                response = self._open_query_stream(q, blobs)
            self.response = response.json
            yield response
        finally:
            response.close()
            if response.complete:
                self.last_query_time = time.time() - started
                self.last_query_timestamp = datetime.now()

    def query(self, q, blobs=[]):
        """
        Execute a query, authenticating or refreshing the shared session as needed.

        If the server rejects the session before executing the query, renew the
        session and retry the query once. If refresh is rejected, log in again
        with the configured password or personal token. Permission errors and
        failures of explicit Authenticate or RefreshToken queries are returned
        without this automatic retry.

        Args:
            q (json): native query to be sent
            blobs (list, optional): Blobs if needed with the query. Defaults to [].

        Raises:
            ConnectionError: Fatal error, connection to server lost

        Returns:
            _type_: _description_
        """
        manage_session = self.should_authenticate and not self._is_authentication_query(
            q)
        query_session = None
        if manage_session:
            self._renew_session()
            query_session = self.authenticate(
                shared_data=self.shared_data,
                user=self.config.username,
                password=self.config.password,
                token=self.token)

        try:
            start = time.time()
            self.response, self.blobs = self._query(
                q, blobs, try_resume=manage_session)
            if manage_session and query_session is not None and \
                    self._is_session_rejected(self.response):
                if self.blobs:
                    self.close()
                    raise ProtocolError(
                        "Session rejection included unexpected output blobs")
                logger.warning(
                    "Session rejected by server. Renewing and retrying query.")
                self._renew_session(rejected_session=query_session)
                start = time.time()
                self.response, self.blobs = self._query(q, blobs)
            self.last_query_time = time.time() - start
            self.last_query_timestamp = datetime.now()
            return self.response, self.blobs
        except BaseException as e:
            logger.critical("Failed to query",
                            exc_info=True, stack_info=True)
            raise

    def _renew_session(self, rejected_session=None):
        count = 0
        while count < RENEW_SESSION_MAX_ATTEMPTS:
            try:
                self._check_session_status(rejected_session)
                break
            except UnauthorizedException as e:
                logger.warning(
                    f"[Attempt {count + 1} of "
                    f"{RENEW_SESSION_MAX_ATTEMPTS}] "
                    "Failed to refresh token.",
                    exc_info=True,
                    stack_info=True)
                count += 1
                if count == RENEW_SESSION_MAX_ATTEMPTS:
                    raise
                time.sleep(RENEW_SESSION_RETRY_INTERVAL_SEC)

    def clone(self) -> Connector:
        """
        Create a new Connector object with the same parameters as the current one.
        This is important in multi-threaded applications, where each thread should have its own Connector object.
        Be cautious when using this method, as it will create a new connection to the database, which will consume resources.
        Ideally, this method should be called once for each thread.

        Returns:
            Connector: Clone of original Connector
        """
        return type(self)(
            shared_data=self.shared_data,
            config=self.config,
            authenticate=self.should_authenticate)

    def create_new_connection(self):
        from aperturedb.CommonLibrary import issue_deprecation_warning
        issue_deprecation_warning(
            "Connector.create_new_connection", "Connector.clone")
        return self.clone()

    def get_last_response_str(self):
        return json.dumps(censor_tokens(self.last_response), indent=4, sort_keys=False)

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

    def check_status(self, json_res: Any) -> int:
        """
        Returns the status of the first negative command response from the server,
        or the status of the first command if all are non-negative.
        Can traverse a JSON recursively to find the statuses.

        Args:
            json_res (Any): The actual response from the server.

        Returns:
            int: The value received from the server, or -2 if not found.
        """
        # Default status is -2, which is an error, but not a server error.
        status = STATUS_ERROR_DEFAULT
        if (isinstance(json_res, dict)):
            if ("status" not in json_res):
                for i, val in enumerate(json_res.values()):
                    st = self.check_status(val)
                    if i == 0:
                        status = st
                    if st < 0:
                        return st
            else:
                status = json_res["status"]
        elif (isinstance(json_res, (tuple, list))):
            for i, res in enumerate(json_res):
                st = self.check_status(res)
                if i == 0:
                    status = st
                if st < 0:
                    return st

        return status
