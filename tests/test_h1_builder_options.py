"""Ports of hyper 1.12.0's HTTP/1 builder and readiness tests."""

import pytest
from _net import h1_client_against, read_head, read_to_eof, tcp_pair
from tonio.colored import Event, scope

from httpunk import H1Connection
from httpunk._httpunk import H1Codec
from httpunk.h1 import H1Server


@pytest.mark.tonio
async def test_h1_connection_is_not_ready_while_a_request_is_in_flight():
    # hyper tests/h1_ready_in_flight.rs:
    # h1_connection_is_not_ready_while_a_request_is_in_flight.
    # The tokio coop-budget exhaustion is omitted: tonio does not use that budget.
    async with tcp_pair() as (stream, peer):
        async with H1Connection(stream) as conn:
            assert conn.busy is False

            async def request():
                await conn.request("GET", "/")
                pytest.fail("the silent peer cannot answer")

            async with scope() as s:
                s.spawn(request())
                try:
                    assert await read_head(peer) is not None
                    assert conn.busy is True
                finally:
                    s.cancel()


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


@pytest.mark.tonio
@pytest.mark.parametrize(
    "title_case", [True, False], ids=["client_set_http1_title_case_headers", "client_obs_fold_headers"]
)
async def test_client_builder(title_case):
    # hyper tests/client.rs: client_set_http1_title_case_headers / client_obs_fold_headers.
    received = []

    async def respond(peer):
        raw = await read_head(peer)
        assert raw is not None
        received.append(raw)
        reply = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
        if not title_case:
            reply += b"Fold: just\r\n some\r\n\t folding\r\n"
        await peer.send_all(reply + b"\r\n")

    options = {"title_case_headers": title_case, "allow_obsolete_multiline_headers_in_responses": not title_case}
    async with h1_client_against(respond, **options) as conn:
        headers = [("x-test-header", "test"), ("host", "example")] if title_case else [("host", "example")]
        response = await conn.request("GET", "/", headers=headers)
        assert response.status == 200
        if not title_case:
            assert response.headers["fold"] == b"just some folding"
        assert await response.read() == b""
    assert received == [
        b"GET / HTTP/1.1\r\nX-Test-Header: test\r\nHost: example\r\n\r\n"
        if title_case
        else b"GET / HTTP/1.1\r\nhost: example\r\n\r\n"
    ]


@pytest.mark.tonio
@pytest.mark.parametrize("too_small", [True, False], ids=["max_buf_size_panic_too_small", "max_buf_size_no_panic"])
async def test_max_buf_size_validation(too_small):
    # hyper tests/server.rs: max_buf_size_panic_too_small / max_buf_size_no_panic.
    async with tcp_pair() as (peer, stream):
        # Complete socket attachment before the no-I/O builder check and teardown.
        await peer.send_all(b"x")
        assert await stream.receive_some(1) == b"x"
        if too_small:
            with pytest.raises(ValueError, match="max_buf_size"):
                H1Server(stream, max_buf_size=8191)
        else:
            H1Server(stream, max_buf_size=8193)


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


@pytest.mark.tonio
@pytest.mark.parametrize("split", [False, True], ids=["max_buf_size", "max_buf_size_split_header_boundary"])
async def test_max_buf_size(split):
    # hyper tests/server.rs:2730,2757: max_buf_size / max_buf_size_split_header_boundary.
    async with tcp_pair() as (peer, stream):
        first = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\nX: " + b"a" * 7000
        rest = b"a" * 5000 + b"\r\n\r\n"
        first_parsed = Event()
        if split:
            # The next read's entry proves the first, sub-cap part was parsed.
            # Batch the rest into one read so it crosses the cap AND completes the
            # head: uncapped parsing would accept it. No TCP segmentation or sleeps.
            stream = _SplitHeadStream(stream, len(first), len(rest), first_parsed)
        server = H1Server(stream, max_buf_size=8192 if split else 16_000)
        finished = Event()
        requests = []

        async def serve():
            try:
                async with server:
                    requests.append(await server.accept())
            finally:
                finished.set()

        async with scope() as s:
            s.spawn(serve())
            try:
                if split:
                    await peer.send_all(first)
                    await first_parsed.wait()
                    assert server._conn.codec.buffered() == len(first) < 8192
                    await peer.send_all(rest)
                else:
                    await peer.send_all(b"POST /")
                    await peer.send_all(b"a" * 16_000)
                response = await read_to_eof(peer, allow_reset=True)
                assert response.startswith(b"HTTP/1.1 431 ")
                await finished.wait()
                assert requests == [None]
                assert server._conn.codec.parse_error_status == 431
            finally:
                peer.close()
                stream.close()
                s.cancel()
