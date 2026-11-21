"""Bounded native-v2 and HTTP multipart response readers.

These readers deliberately do not reconnect or replay requests. Once query
bytes have been written, a transport failure leaves its execution uncertain.
"""

import io
import json
import struct
from email.message import Message


HELLO_VERSION = 2
LEGACY_HELLO_VERSION = 1
HELLO_HEADER = "ApertureDB-Hello-Version"
MAX_BLOBS = (1 << 24) - 1
CHUNK_SIZE = 64 * 1024
WHITESPACE = b" \t\r\n"


class ProtocolError(ConnectionError):
    """An invalid or incomplete server response; the connection is unusable."""


class BufferedInput:
    def __init__(self, receive):
        self.receive = receive
        self.buffer = b""
        self.position = 0
        self.consumed = 0

    def _fill(self):
        if self.position == len(self.buffer):
            self.buffer = self.receive(CHUNK_SIZE)
            self.position = 0
        return bool(self.buffer)

    def byte(self, optional=False):
        if not self._fill():
            if optional:
                return None
            raise ProtocolError("Incomplete query response")
        value = self.buffer[self.position]
        self.position += 1
        self.consumed += 1
        return value

    def peek(self):
        if not self._fill():
            raise ProtocolError("Incomplete query response")
        return self.buffer[self.position]

    def exact(self, count):
        output = bytearray()
        while count:
            if not self._fill():
                raise ProtocolError("Incomplete query response")
            size = min(count, len(self.buffer) - self.position)
            output.extend(self.buffer[self.position:self.position + size])
            self.position += size
            self.consumed += size
            count -= size
        return bytes(output)

    def until(self, delimiter, limit):
        output = bytearray()
        while True:
            value = self.byte()
            output.append(value)
            if output.endswith(delimiter):
                return bytes(output[:-len(delimiter)])
            if len(output) > limit + len(delimiter):
                raise ProtocolError("Response metadata exceeds its size limit")

    def whitespace(self, limit=None):
        start = self.consumed
        while self.peek() in WHITESPACE:
            self.byte()
            if limit is not None and self.consumed - start > limit:
                raise ProtocolError("Response exceeds its size limit")

    def all(self, limit):
        output = bytearray()
        while self._fill():
            chunk = self.buffer[self.position:]
            if len(output) + len(chunk) > limit:
                raise ProtocolError("Response exceeds its size limit")
            output.extend(chunk)
            self.position = len(self.buffer)
            self.consumed += len(chunk)
        return bytes(output)

    def json(self, limit):
        raw = bytearray()
        start = self.consumed
        depth = 0
        quoted = escaped = False
        while True:
            value = self.byte()
            if self.consumed - start > limit:
                raise ProtocolError("Response JSON exceeds its size limit")
            if not raw and value in WHITESPACE:
                continue
            if not raw and value not in b"[{":
                raise ProtocolError(
                    "Response must start with a JSON array or object")
            raw.append(value)
            if len(raw) > limit:
                raise ProtocolError("Response JSON exceeds its size limit")
            if quoted:
                if escaped:
                    escaped = False
                elif value == ord("\\"):
                    escaped = True
                elif value == ord('"'):
                    quoted = False
            elif value == ord('"'):
                quoted = True
            elif value in b"[{":
                depth += 1
                if depth > 256:
                    raise ProtocolError(
                        "Response JSON exceeds its nesting limit")
            elif value in b"]}":
                depth -= 1
                if not depth:
                    try:
                        return json.loads(raw)
                    except (ValueError, UnicodeError) as error:
                        raise ProtocolError("Invalid response JSON") from error


class ResponseBlob:
    """One ordered binary response value, read on demand.

    ``read()`` collects the remaining bytes. ``iter_chunks()`` lets application
    processing apply backpressure without materializing the complete blob.
    Consume a blob before requesting the next one.
    """

    def __init__(self, owner, size, read, completed):
        self.size = size
        self.remaining = size
        self._owner = owner
        self._read = read
        self._completed = completed
        if not size:
            self._finish()

    def _finish(self):
        callback, self._completed = self._completed, None
        if callback:
            callback()

    def read(self, size=-1):
        if self._owner.closed and self.remaining:
            raise ProtocolError("The response stream is closed")
        if size is None or size < 0:
            size = self.remaining
        size = min(size, self.remaining)
        try:
            result = self._read(size)
            if len(result) != size:
                raise ProtocolError("Incomplete response blob")
            self.remaining -= size
            if not self.remaining:
                self._finish()
            return result
        except BaseException as error:
            self._owner.abort()
            if isinstance(error, OSError) and not isinstance(error, ConnectionError):
                raise ProtocolError("Interrupted response blob") from error
            raise

    def iter_chunks(self, size=CHUNK_SIZE):
        if size <= 0:
            raise ValueError("Chunk size must be positive")
        while self.remaining:
            yield self.read(min(size, self.remaining))


class QueryResponse:
    """JSON metadata followed by an ordered, lazy iterator of binary blobs."""

    def __init__(self, metadata, release):
        self.json = metadata
        self.blobs = self
        self.complete = False
        self.closed = False
        self._release = release
        self._next_blob = None
        self._active_blob = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        if self.closed and not self.complete:
            raise ProtocolError("The response stream is closed")
        if self._active_blob and self._active_blob.remaining:
            self.abort()
            raise ProtocolError(
                "Consume the current blob before reading the next")
        if self.complete:
            raise StopIteration
        try:
            self._active_blob = self._next_blob()
            return self._active_blob
        except StopIteration:
            self.finish()
            raise
        except BaseException as error:
            self.abort()
            if isinstance(error, OSError) and not isinstance(error, ConnectionError):
                raise ProtocolError("Interrupted response stream") from error
            raise

    def finish(self):
        self.complete = True
        self.close()

    def abort(self):
        if not self.closed:
            self.complete = False
            self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self._release(self.complete)
            except BaseException:
                self.complete = False
                raise


def buffered_response(metadata, blobs, release):
    """Expose already-buffered legacy responses through the streaming API."""
    result = QueryResponse(metadata, release)
    remaining = iter(blobs)
    count = len(blobs)

    def completed():
        nonlocal count
        count -= 1
        if not count:
            result.finish()

    def next_blob():
        data = next(remaining)
        return ResponseBlob(result, len(data), io.BytesIO(data).read, completed)

    result._next_blob = next_blob
    if not blobs:
        result.finish()
    return result


def native_response(source, limit, release):
    start = source.consumed
    metadata = source.json(limit)
    source.whitespace(limit - (source.consumed - start))
    count = struct.unpack("!I", source.exact(4))[0]
    if count > MAX_BLOBS or count * 8 + source.consumed - start > limit:
        raise ProtocolError("Response blob count exceeds its size limit")
    result = QueryResponse(metadata, release)
    remaining = count

    def completed():
        nonlocal remaining
        remaining -= 1
        if not remaining:
            result.finish()

    def next_blob():
        size = struct.unpack("!Q", source.exact(8))[0]
        if size + (remaining - 1) * 8 + source.consumed - start > limit:
            raise ProtocolError("Response blob exceeds its size limit")
        return ResponseBlob(result, size, source.exact, completed)

    result._next_blob = next_blob
    if not count:
        result.finish()
    return result


def multipart_response(source, content_type, limit, release):
    message = Message()
    message["content-type"] = content_type
    boundary = message.get_param("boundary")
    if message.get_content_type() != "multipart/mixed" or not boundary:
        raise ProtocolError("Expected a multipart/mixed query response")
    try:
        boundary = boundary.encode("ascii")
    except UnicodeError as error:
        raise ProtocolError("Invalid multipart boundary") from error
    if len(boundary) > 200 or any(value < 32 or value > 126 for value in boundary):
        raise ProtocolError("Invalid multipart boundary")
    marker = b"--" + boundary
    start = source.consumed
    if source.exact(len(marker) + 2) != marker + b"\r\n":
        raise ProtocolError("Invalid initial multipart boundary")

    def headers():
        block = source.until(b"\r\n\r\n", 16 * 1024)
        values = {}
        for line in block.split(b"\r\n"):
            key, separator, value = line.partition(b":")
            if not separator:
                raise ProtocolError("Invalid multipart header")
            try:
                key = key.decode("ascii").strip().lower()
                value = value.decode("ascii").strip()
            except UnicodeError as error:
                raise ProtocolError("Invalid multipart header") from error
            if key in values:
                raise ProtocolError("Duplicate multipart header")
            values[key] = value
        return values

    first = headers()
    if first.get("content-type", "").split(";", 1)[0].lower() != "application/json":
        raise ProtocolError("The first multipart part must contain JSON")
    metadata = source.json(limit)
    result = QueryResponse(metadata, release)
    next_headers = None
    count = 0
    initial = True
    need_boundary = True

    def boundary_end():
        nonlocal next_headers
        ending = source.exact(2)
        if ending == b"--":
            # Require both MIME closure and completion of HTTP transfer framing.
            # requests raises on an incomplete chunked body while reading EOF.
            tail = bytearray()
            while True:
                value = source.byte(optional=True)
                if value is None:
                    break
                tail.append(value)
                if len(tail) > 1024 or value not in WHITESPACE:
                    raise ProtocolError("Unexpected multipart epilogue")
            if source.consumed - start > limit:
                raise ProtocolError(
                    "Multipart response exceeds its size limit")
            result.finish()
        elif ending == b"\r\n":
            next_headers = headers()
        else:
            raise ProtocolError("Incomplete multipart closing boundary")

    def completed():
        nonlocal need_boundary
        # Reading this blob must not wait for the next one's header/encoding.
        # The following iterator advance validates its boundary instead.
        need_boundary = True

    def next_blob():
        nonlocal next_headers, count, initial, need_boundary
        if need_boundary:
            if initial:
                whitespace = source.until(b"\r\n" + marker,
                                          max(0, limit - (source.consumed - start)))
                if any(value not in WHITESPACE for value in whitespace):
                    raise ProtocolError("Unexpected bytes after response JSON")
                initial = False
            elif source.exact(len(marker) + 2) != b"\r\n" + marker:
                raise ProtocolError("Invalid multipart blob boundary")
            boundary_end()
            need_boundary = False
            if result.complete:
                raise StopIteration
        values, next_headers = next_headers, None
        if values is None:
            raise ProtocolError("Missing multipart blob headers")
        if values.get("content-type", "").lower() != "application/octet-stream":
            raise ProtocolError("Unexpected multipart blob content type")
        length = values.get("content-length", "")
        if len(length) > 20 or not length.isascii() or not length.isdecimal():
            raise ProtocolError("Missing or invalid multipart blob size")
        size = int(length)
        count += 1
        if count > MAX_BLOBS or size + source.consumed - start > limit:
            raise ProtocolError("Multipart response exceeds its size limit")
        return ResponseBlob(result, size, source.exact, completed)

    result._next_blob = next_blob
    return result
