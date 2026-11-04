"""Ports of hyper 1.12.0's HTTP/1 builder and readiness tests."""

import pytest
from _runtime import connected_pair
from conftest import TONIO_OK

from httpunk import H1Connection
from httpunk._backend import Backend
from httpunk._httpunk import H1Codec
from httpunk.exceptions import H1ParseError
from httpunk.h1 import H1Server


async def _h1_connection_is_not_ready_while_a_request_is_in_flight(runtime):
    # hyper tests/h1_ready_in_flight.rs:
    # h1_connection_is_not_ready_while_a_request_is_in_flight.
    # The tokio coop-budget exhaustion is omitted: neither backend uses that budget.
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (stream, peer):
        async with H1Connection(stream, backend=backend) as conn:
            assert conn.busy is False

            async def request():
                await conn.request("GET", "/")
                pytest.fail("the silent peer cannot answer")

            async with backend.scope() as scope:
                scope.spawn(request())
                try:
                    raw = b""
                    while b"\r\n\r\n" not in raw:
                        chunk = await peer.receive_some(65536)
                        assert chunk
                        raw += chunk
                    assert conn.busy is True
                finally:
                    scope.cancel()


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_h1_connection_is_not_ready_while_a_request_is_in_flight_tonio():
    await _h1_connection_is_not_ready_while_a_request_is_in_flight("tonio")


@pytest.mark.asyncio
async def test_h1_connection_is_not_ready_while_a_request_is_in_flight_asyncio():
    await _h1_connection_is_not_ready_while_a_request_is_in_flight("asyncio")


_RESPONSE_WITH_SPACES = b"HTTP/1.1 200 OK\r\nAccess-Control-Allow-Credentials : true\r\n\r\n"
_REQUEST_WITH_SPACES = b"GET  /echo  HTTP/1.1\r\nHost: hyper.rs\r\n\r\n"


def test_parse_allow_response_with_spaces_before_colons():
    # hyper src/proto/h1/role.rs: test_parse_allow_response_with_spaces_before_colons.
    codec = H1Codec(allow_spaces_after_header_name_in_responses=True)
    codec.serialize_request("GET", "/")
    head = codec.receive_head(_RESPONSE_WITH_SPACES)
    assert codec.buffered() == 0
    assert head.status == 200
    assert head.http10 is False
    assert len(head.headers) == 1
    assert head.headers["Access-Control-Allow-Credentials"] == b"true"


def test_parse_reject_response_with_spaces_before_colons():
    # hyper src/proto/h1/role.rs: test_parse_reject_response_with_spaces_before_colons.
    codec = H1Codec()
    codec.serialize_request("GET", "/")
    with pytest.raises(H1ParseError):
        codec.receive_head(_RESPONSE_WITH_SPACES)


def test_parse_allow_request_with_multiple_spaces_in_request_line():
    # hyper src/proto/h1/role.rs: test_parse_allow_request_with_multiple_spaces_in_request_line.
    codec = H1Codec(allow_multiple_spaces_in_request_line_delimiters=True)
    head = codec.receive_request_head(_REQUEST_WITH_SPACES)
    assert codec.buffered() == 0
    assert head.method == "GET"
    assert head.target == "/echo"
    assert head.http10 is False
    assert len(head.headers) == 1
    assert head.headers["Host"] == b"hyper.rs"


def test_parse_reject_request_with_multiple_spaces_in_request_line():
    # hyper src/proto/h1/role.rs: test_parse_reject_request_with_multiple_spaces_in_request_line.
    with pytest.raises(H1ParseError):
        H1Codec().receive_request_head(_REQUEST_WITH_SPACES)


@pytest.mark.parametrize(
    ("max_headers", "count", "accepted"),
    [
        (None, 0, True),
        (None, 1, True),
        (None, 50, True),
        (None, 99, True),
        (None, 100, True),
        (None, 101, False),
        (None, 102, False),
        (None, 200, False),
        (0, 0, True),
        (0, 1, False),
        (0, 100, False),
        (200, 0, True),
        (200, 1, True),
        (200, 100, True),
        (200, 200, True),
        (200, 201, False),
        (200, 210, False),
    ],
)
def test_parse_too_large_headers(max_headers, count, accepted):
    # hyper src/proto/h1/role.rs: parse_too_large_headers (both roles, full matrix).
    fields = b"".join(f"key{i}: val{i}\r\n".encode() for i in range(count)) + b"\r\n"
    for start, parse in [
        (b"GET / HTTP/1.1\r\n", H1Codec(max_headers=max_headers).receive_request_head),
        (b"HTTP/1.1 200 OK\r\n", H1Codec(max_headers=max_headers).receive_head),
    ]:
        if accepted:
            assert parse(start + fields) is not None
        else:
            with pytest.raises(H1ParseError):
                parse(start + fields)


def test_client_max_buf_size_split_header_boundary():
    # Derived from hyper io.rs:204-240: tests/client.rs has no max_buf_size test.
    # Exercise the shared bounded-read rule on a response completed past the cap.
    codec = H1Codec(max_buf_size=8192)
    codec.serialize_request("GET", "/")
    assert codec.receive_head(b"HTTP/1.1 200 OK\r\nX: " + b"a" * 7000) is None
    with pytest.raises(H1ParseError) as caught:
        codec.receive_head(b"a" * 5000 + b"\r\n\r\n")
    assert caught.value.args == ("too_large", "message head is too large")


def test_client_max_buf_size_informational_read_ahead():
    # Derived from hyper role.rs:1039-1230 and io.rs:204-240: consuming a 1xx
    # frees buffer space for the final head; bytes after that head remain body.
    codec = H1Codec(max_buf_size=8192)
    codec.serialize_request("GET", "/")
    informational = b"HTTP/1.1 103 Early Hints\r\nX: " + b"a" * 7000 + b"\r\n\r\n"
    body = b"body" * 3000
    response = b"HTTP/1.1 200 OK\r\nContent-Length: 12000\r\nX: " + b"b" * 7000 + b"\r\n\r\n"
    head = codec.receive_head(informational + response + body)
    assert head.status == 200
    assert head.headers["x"] == b"b" * 7000
    assert codec.take_body() == body


async def _client_builder(runtime, *, title_case):
    # hyper tests/client.rs: client_set_http1_title_case_headers / client_obs_fold_headers.
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (stream, peer):
        received = []
        finished = backend.event()

        async def respond():
            try:
                raw = b""
                while b"\r\n\r\n" not in raw:
                    chunk = await peer.receive_some(65536)
                    assert chunk
                    raw += chunk
                received.append(raw)
                reply = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
                if not title_case:
                    reply += b"Fold: just\r\n some\r\n\t folding\r\n"
                await peer.send_all(reply + b"\r\n")
            finally:
                finished.set()

        async with backend.scope() as scope:
            scope.spawn(respond())
            try:
                async with H1Connection(
                    stream,
                    backend=backend,
                    title_case_headers=title_case,
                    allow_obsolete_multiline_headers_in_responses=not title_case,
                ) as conn:
                    headers = [("x-test-header", "test"), ("host", "example")] if title_case else [("host", "example")]
                    response = await conn.request("GET", "/", headers=headers)
                    assert response.status == 200
                    if not title_case:
                        assert response.headers["fold"] == b"just some folding"
                    assert await response.read() == b""
                await finished.wait()
                assert received == [
                    b"GET / HTTP/1.1\r\nX-Test-Header: test\r\nHost: example\r\n\r\n"
                    if title_case
                    else b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"
                ]
            finally:
                backend.close_transport(stream)
                backend.close_transport(peer)
                scope.cancel()


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_client_set_http1_title_case_headers_tonio():
    await _client_builder("tonio", title_case=True)


@pytest.mark.asyncio
async def test_client_set_http1_title_case_headers_asyncio():
    await _client_builder("asyncio", title_case=True)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_client_obs_fold_headers_tonio():
    await _client_builder("tonio", title_case=False)


@pytest.mark.asyncio
async def test_client_obs_fold_headers_asyncio():
    await _client_builder("asyncio", title_case=False)


async def _max_buf_size_validation(runtime, *, too_small):
    # hyper tests/server.rs: max_buf_size_panic_too_small / max_buf_size_no_panic.
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (peer, stream):
        # Complete socket attachment before the no-I/O builder check and teardown.
        await peer.send_all(b"x")
        assert await stream.receive_some(1) == b"x"
        if too_small:
            with pytest.raises(ValueError, match="max_buf_size"):
                H1Server(stream, backend=backend, max_buf_size=8191)
        else:
            H1Server(stream, backend=backend, max_buf_size=8193)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_buf_size_panic_too_small_tonio():
    await _max_buf_size_validation("tonio", too_small=True)


@pytest.mark.asyncio
async def test_max_buf_size_panic_too_small_asyncio():
    await _max_buf_size_validation("asyncio", too_small=True)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_buf_size_no_panic_tonio():
    await _max_buf_size_validation("tonio", too_small=False)


@pytest.mark.asyncio
async def test_max_buf_size_no_panic_asyncio():
    await _max_buf_size_validation("asyncio", too_small=False)


class _SplitHeadStream:
    def __init__(self, stream, first_size, rest_size, first_parsed):
        self._stream = stream
        self._sizes = iter((first_size, rest_size))
        self._first_returned = False
        self._first_parsed = first_parsed

    async def receive_some(self, max_bytes=65536):
        size = next(self._sizes, None)
        if size is None:
            return await self._stream.receive_some(max_bytes)
        if self._first_returned:
            self._first_parsed.set()
        data = b""
        while len(data) < size:
            chunk = await self._stream.receive_some(min(max_bytes, size - len(data)))
            assert chunk
            data += chunk
        self._first_returned = True
        return data

    def __getattr__(self, name):
        return getattr(self._stream, name)


async def _max_buf_size(runtime, *, split):
    # hyper tests/server.rs:2730,2757: max_buf_size / max_buf_size_split_header_boundary.
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (peer, stream):
        first = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\nX: " + b"a" * 7000
        rest = b"a" * 5000 + b"\r\n\r\n"
        first_parsed = backend.event()
        if split:
            # The next read's entry proves the first, sub-cap part was parsed.
            # Batch the rest into one read so it crosses the cap AND completes the
            # head: uncapped parsing would accept it. No TCP segmentation or sleeps.
            stream = _SplitHeadStream(stream, len(first), len(rest), first_parsed)
        server = H1Server(stream, backend=backend, max_buf_size=8192 if split else 16_000)
        finished = backend.event()
        requests = []

        async def serve():
            try:
                async with server:
                    requests.append(await server.accept())
            finally:
                finished.set()

        async with backend.scope() as scope:
            scope.spawn(serve())
            try:
                if split:
                    await peer.send_all(first)
                    await first_parsed.wait()
                    assert server._conn.codec.buffered() == len(first) < 8192
                    await peer.send_all(rest)
                else:
                    await peer.send_all(b"POST /")
                    await peer.send_all(b"a" * 16_000)
                response = b""
                try:
                    while chunk := await peer.receive_some(65536):
                        response += chunk
                except backend.broken_transport_errors:
                    pass  # dropping unread input can reset the socket instead of sending FIN
                assert response.startswith(b"HTTP/1.1 431 ")
                await finished.wait()
                assert requests == [None]
                assert server._conn.codec.parse_error_status == 431
            finally:
                backend.close_transport(peer)
                backend.close_transport(stream)
                scope.cancel()


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_buf_size_tonio():
    await _max_buf_size("tonio", split=False)


@pytest.mark.asyncio
async def test_max_buf_size_asyncio():
    await _max_buf_size("asyncio", split=False)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_buf_size_split_header_boundary_tonio():
    await _max_buf_size("tonio", split=True)


@pytest.mark.asyncio
async def test_max_buf_size_split_header_boundary_asyncio():
    await _max_buf_size("asyncio", split=True)
