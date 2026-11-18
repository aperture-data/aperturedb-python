"""Wire and streaming tests that require no database or cloud credentials."""

import base64
import io
import json
import struct
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from aperturedb import queryMessage
from aperturedb.Connector import Connector, Session
from aperturedb.ConnectorRest import ConnectorRest
from aperturedb._query_wire import (
    BufferedInput, HELLO_HEADER, ProtocolError, multipart_response)


def native(metadata, blobs=()):
    result = json.dumps(metadata, ensure_ascii=False).encode() + \
        struct.pack('!I', len(blobs))
    for blob in blobs:
        result += struct.pack('!Q', len(blob)) + blob
    return result


def legacy(metadata, blobs=()):
    message = queryMessage.queryMessage()
    message.json = json.dumps(metadata)
    message.blobs.extend(blobs)
    data = message.SerializeToString()
    return struct.pack('<I', len(data)) + data


def multipart(metadata, blobs=(), boundary=b'example-boundary'):
    result = b'--' + boundary + b'\r\nContent-Type: application/json\r\n\r\n'
    result += json.dumps(metadata).encode()
    for blob in blobs:
        result += b'\r\n--' + boundary + b'\r\nContent-Type: application/octet-stream\r\n'
        result += b'Content-Length: ' + \
            str(len(blob)).encode() + b'\r\n\r\n' + blob
    return result + b'\r\n--' + boundary + b'--\r\n'


class FakeSocket:
    def __init__(self, data=b'', fragment=65536):
        self.data = io.BytesIO(data)
        self.fragment = fragment
        self.sent = []
        self.closed = False
        self.received = 0

    def recv(self, size):
        data = self.data.read(min(size, self.fragment))
        self.received += len(data)
        return data

    def sendall(self, data):
        self.sent.append(bytes(data))

    def close(self):
        self.closed = True

    def setsockopt(self, *_):
        pass

    def settimeout(self, *_):
        pass

    def connect(self, *_):
        pass


class FakeHttpResponse:
    def __init__(self, body, version='2', status=200, fragment=113,
                 content_type='multipart/mixed; boundary="example-boundary"', fail_at_eof=False):
        self.body = body
        self.status_code = status
        self.headers = {'Content-Type': content_type}
        if version is not None:
            self.headers[HELLO_HEADER] = version
        self.fragment = fragment
        self.closed = False
        self.fail_at_eof = fail_at_eof
        self.received = 0

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), min(chunk_size, self.fragment)):
            data = self.body[offset:offset + min(chunk_size, self.fragment)]
            self.received += len(data)
            yield data
        if self.fail_at_eof:
            raise ProtocolError('Incomplete HTTP transfer framing')

    def close(self):
        self.closed = True


class FakeHttpSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return next(self.responses)

    def close(self):
        pass


class QueryTransportTests(unittest.TestCase):
    def setUp(self):
        logger = patch('aperturedb.Connector.logger')
        logger.start()
        self.addCleanup(logger.stop)

    def connector(self, data, version=2, fragment=65536):
        db = Connector(authenticate=False, use_ssl=False, use_keepalive=False)
        db.conn = FakeSocket(data, fragment)
        db.connected = True
        db._hello_version = version
        db._wire_input = BufferedInput(db.conn.recv)
        self.addCleanup(db.close)
        return db

    def test_native_and_rest_clones_preserve_disabled_authentication(self):
        for cls in (Connector, ConnectorRest):
            with self.subTest(connector=cls.__name__):
                source = cls(authenticate=False, use_ssl=False)
                clone = source.clone()
                try:
                    self.assertFalse(clone.should_authenticate)
                    self.assertIs(clone.shared_data, source.shared_data)
                finally:
                    clone.close()
                    source.close()

    def test_native_fragments_token_and_exact_binary_framing(self):
        output = [b'\x00\r\n\xff', b'']
        db = self.connector(
            native([{'FindBlob': {'status': 0}}], output), fragment=1)
        connection = db.conn
        db.shared_data.session = SimpleNamespace(
            session_token='quoted " token')
        response, blobs = db.query(
            [{'FindBlob': {'blobs': True}}], [b'input\x00', b''])
        self.assertEqual(blobs, output)
        self.assertEqual(response[0]['FindBlob']['status'], 0)
        sent = b''.join(connection.sent)
        prefix = json.dumps(
            'quoted " token', ensure_ascii=False).encode() + b' '
        prefix += json.dumps([{'FindBlob': {'blobs': True}}]).encode()
        expected = prefix + \
            struct.pack('!I', 2) + struct.pack('!Q', 6) + \
            b'input\x00' + struct.pack('!Q', 0)
        self.assertEqual(sent, expected)
        self.assertFalse(connection.closed)

    def test_native_retains_following_message_bytes(self):
        db = self.connector(
            native([]) + native([{'GetStatus': {'status': 0}}]))
        self.assertEqual(db.query([]), ([], []))
        self.assertEqual(db.query([{'GetStatus': {}}])[
                         0][0]['GetStatus']['status'], 0)

    def test_legacy_partial_length_prefix_and_stream_interface(self):
        db = self.connector(legacy([{'FindBlob': {'status': 0}}], [
                            b'old', b'']), version=1, fragment=1)
        with db.query_stream([{'FindBlob': {'blobs': True}}]) as response:
            self.assertEqual([blob.read()
                             for blob in response.blobs], [b'old', b''])
            self.assertTrue(response.complete)
        self.assertTrue(db.connected)

    def test_stream_reads_only_current_blob_chunks(self):
        payload = b'x' * (200 * 1024)
        db = self.connector(native([], [payload, b'next']))
        connection = db.conn
        with db.query_stream([]) as response:
            self.assertLessEqual(connection.received, 65536)
            blob = next(response.blobs)
            self.assertEqual(blob.size, len(payload))
            self.assertEqual(blob.read(17), payload[:17])
            with self.assertRaises(RuntimeError):
                db.query([])
            remaining = b''.join(blob.iter_chunks(8192))
            self.assertEqual(remaining, payload[17:])
            self.assertEqual(next(response.blobs).read(), b'next')
            self.assertTrue(response.complete)
        self.assertFalse(connection.closed)

    def test_abandoning_stream_closes_connection(self):
        db = self.connector(native([], [b'a', b'b']))
        connection = db.conn
        with db.query_stream([]) as response:
            self.assertEqual(next(response.blobs).read(), b'a')
        self.assertTrue(connection.closed)
        self.assertFalse(db.connected)

    def test_advancing_past_unread_blob_closes_connection(self):
        db = self.connector(native([], [b'unread', b'next']))
        connection = db.conn
        with self.assertRaises(ProtocolError):
            with db.query_stream([]) as response:
                next(response.blobs)
                next(response.blobs)
        self.assertTrue(connection.closed)

    def test_native_truncation_never_replays_write(self):
        for data in (b'[]', b'[]\0\0', b'[]\0\0\0\1\0',
                     native([], [b'abc'])[:-1]):
            with self.subTest(data=data):
                db = self.connector(data)
                connection = db.conn
                with self.assertRaises(ConnectionError):
                    db.query([{'AddEntity': {'class': 'NoReplay'}}])
                self.assertEqual(
                    b''.join(connection.sent).count(b'AddEntity'), 1)
                self.assertTrue(connection.closed)

    def test_session_rejection_with_blobs_cannot_replay_write(self):
        rejection = {'status': -1, 'code': 'unauthenticated'}
        for version, frame in ((1, legacy), (2, native)):
            with self.subTest(version=version):
                db = self.connector(frame(rejection, [b'']), version=version)
                db.should_authenticate = True
                db.shared_data.session = Session(
                    'token', 'refresh', 3600, 7200, time.time())
                connection = db.conn
                with self.assertRaises(ProtocolError):
                    db.query([{'AddEntity': {'class': 'NoReplay'}}])
                self.assertEqual(
                    b''.join(connection.sent).count(b'AddEntity'), 1)
                self.assertTrue(connection.closed)

    def test_streamed_session_rejection_with_empty_blob_cannot_replay(self):
        rejection = {'status': -1, 'code': 'unauthenticated'}
        for version, frame in ((1, legacy), (2, native)):
            with self.subTest(version=version):
                db = self.connector(frame(rejection, [b'']), version=version)
                db.should_authenticate = True
                db.shared_data.session = Session(
                    'token', 'refresh', 3600, 7200, time.time())
                connection = db.conn
                with self.assertRaises(ProtocolError):
                    with db.query_stream([{'AddEntity': {'class': 'NoReplay'}}]):
                        self.fail(
                            'Malformed rejection must not yield a stream')
                self.assertEqual(
                    b''.join(connection.sent).count(b'AddEntity'), 1)
                self.assertTrue(connection.closed)

    def test_native_rejects_count_bomb_and_excessive_lengths(self):
        for suffix in (struct.pack('!I', 1 << 24), struct.pack('!I', 1) + struct.pack('!Q', 1 << 63)):
            db = self.connector(b'[]' + suffix)
            with self.assertRaises(ProtocolError):
                db.query([])

    def test_native_hello_discovers_old_and_new_without_query(self):
        for version in (1, 2):
            with self.subTest(version=version):
                if version == 1:
                    connections = [FakeSocket(struct.pack('<III', 8, 1, 1), 1)]
                else:
                    connections = [FakeSocket(struct.pack('<III', 8, 2, 0), 1),
                                   FakeSocket(struct.pack('<III', 8, 2, 1), 1)]
                db = Connector(authenticate=False,
                               use_ssl=False, use_keepalive=False)
                with patch('aperturedb.Connector.socket.socket', side_effect=connections):
                    self.assertEqual(db.hello_version, version)
                self.assertEqual(
                    b''.join(connections[0].sent), struct.pack('<III', 8, 1, 1))
                if version == 2:
                    self.assertTrue(connections[0].closed)
                    self.assertEqual(
                        b''.join(connections[1].sent), struct.pack('<III', 8, 2, 1))
                db.close()

    def test_unsupported_hello_is_not_guessed_or_retried(self):
        connection = FakeSocket(struct.pack('<III', 8, 3, 0))
        db = Connector(authenticate=False, use_ssl=False, use_keepalive=False)
        with patch('aperturedb.Connector.socket.socket', return_value=connection) as create:
            with self.assertRaises(ProtocolError):
                _ = db.hello_version
        self.assertEqual(create.call_count, 1)
        self.assertTrue(connection.closed)

    def rest(self, responses):
        db = ConnectorRest(use_ssl=False)
        db.should_authenticate = False
        db.http_session.close()
        db.http_session = FakeHttpSession(responses)
        self.addCleanup(db.close)
        return db

    def test_http_header_discovery_and_raw_multipart(self):
        probe = FakeHttpResponse(multipart({'status': -1}))
        blobs = [b'raw\0\xff', b'', b'\r\n--example-boundary\r\ninside blob']
        answer = FakeHttpResponse(multipart([], blobs), fragment=1)
        db = self.rest([probe, answer])
        session = db.http_session
        self.assertEqual(db.hello_version, 2)
        self.assertEqual(db.query([]), ([], blobs))
        self.assertEqual(len(session.posts), 2)
        self.assertEqual(session.posts[0][1]['files'][0][1][1], '[]')
        self.assertEqual(session.posts[1][1]['headers'][HELLO_HEADER], '2')
        self.assertFalse(session.posts[1][1]['allow_redirects'])
        self.assertEqual(session.posts[1][1]['files'][0][0], 'query')
        self.assertTrue(answer.closed)

    def test_http_blob_upload_preserves_legacy_filename(self):
        db = self.rest([FakeHttpResponse(multipart([])),
                       FakeHttpResponse(multipart([]))])
        session = db.http_session
        db.query([], [b'input'])
        file = session.posts[-1][1]['files'][1]
        self.assertEqual(file[0], 'blobs')
        self.assertEqual(file[1][0], 'blobs')
        self.assertEqual(file[1][1], b'input')

    def test_http_legacy_response_without_hello_header(self):
        def response(metadata, blobs=()):
            body = json.dumps({'json': metadata,
                               'blobs': [base64.b64encode(blob).decode() for blob in blobs]}).encode()
            return FakeHttpResponse(body, version=None, content_type='application/json')
        db = self.rest([response({'status': -1}),
                       response([], [b'legacy', b''])])
        self.assertEqual(db.hello_version, 1)
        self.assertEqual(db.query([]), ([], [b'legacy', b'']))

    def test_http_lazy_stream_and_abandonment(self):
        probe = FakeHttpResponse(multipart([]))
        answer = FakeHttpResponse(multipart([], [b'x' * (200 * 1024)]))
        db = self.rest([probe, answer])
        with db.query_stream([]) as response:
            self.assertLess(answer.received, 200 * 1024)
            self.assertEqual(next(response.blobs).read(11), b'x' * 11)
        self.assertTrue(answer.closed)
        self.assertFalse(db.connected)

    def test_http_json_and_current_blob_do_not_read_ahead(self):
        class DelayedResponse(FakeHttpResponse):
            allow_blob = False
            allow_footer = False

            def iter_content(self, chunk_size):
                yield (b'--example-boundary\r\nContent-Type: application/json\r\n\r\n[]')
                if not self.allow_blob:
                    raise AssertionError(
                        'Read a blob header before JSON was consumed')
                yield (b'\r\n--example-boundary\r\nContent-Type: application/octet-stream\r\n'
                       b'Content-Length: 3\r\n\r\nabc')
                if not self.allow_footer:
                    raise AssertionError(
                        'Read the next boundary before advancing the iterator')
                yield b'\r\n--example-boundary--\r\n'

        delayed = DelayedResponse(b'')
        db = self.rest([FakeHttpResponse(multipart([])), delayed])
        with db.query_stream([]) as response:
            self.assertEqual(response.json, [])
            delayed.allow_blob = True
            self.assertEqual(next(response.blobs).read(), b'abc')
            self.assertFalse(response.complete)
            delayed.allow_footer = True
            self.assertEqual(list(response.blobs), [])
            self.assertTrue(response.complete)

    def test_http_truncation_and_transfer_failure_never_replay(self):
        for answer in (FakeHttpResponse(multipart([], [b'blob'])[:-9]),
                       FakeHttpResponse(
                           multipart([], [b'blob']), fail_at_eof=True),
                       FakeHttpResponse(b'redirect', status=307),
                       FakeHttpResponse(b'error', status=500)):
            with self.subTest(answer=answer):
                db = self.rest([FakeHttpResponse(multipart([])), answer])
                session = db.http_session
                with self.assertRaises(ConnectionError):
                    db.query([{'AddEntity': {'class': 'NoReplay'}}])
                self.assertEqual(len(session.posts), 2)
                self.assertTrue(answer.closed)

    def test_http_close_failure_releases_active_query_guard(self):
        class FailingClose(FakeHttpResponse):
            def close(self):
                self.closed = True
                raise OSError('Unable to close HTTP response')

        db = self.rest([FakeHttpResponse(multipart([])),
                        FailingClose(multipart([], [b'blob'])),
                        FakeHttpResponse(multipart([])),
                        FakeHttpResponse(multipart([]))])
        with self.assertRaises(ConnectionError):
            db.query([])
        self.assertEqual(len(db.http_session.posts), 2)
        self.assertEqual(db.query([]), ([], []))
        self.assertEqual(len(db.http_session.posts), 4)

    def test_http_rejects_invalid_binary_part_size_and_missing_closure(self):
        valid = multipart([], [b'abc'])
        for body in (valid.replace(b'Content-Length: 3', b'Content-Length: -1'),
                     valid.replace(b'Content-Length: 3\r\n', b''),
                     multipart([])[:-8]):
            with self.subTest(body=body):
                source = BufferedInput(io.BytesIO(body).read)
                with self.assertRaises(ProtocolError):
                    with multipart_response(source, 'multipart/mixed; boundary=example-boundary',
                                            1024 * 1024, lambda _: None) as response:
                        list(blob.read() for blob in response.blobs)


if __name__ == '__main__':
    unittest.main()
