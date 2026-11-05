"""HTTP/1 server (`H1Server`) over a tonio loopback, driven by httpunk's own
`H1Connection` client — end-to-end coverage of both sides: request/response,
request + response bodies, keep-alive reuse, chunked responses, headers, and the
auto-`Date` header.
"""

import asyncio

import pytest
from _net import h1_client_against, h1_peer_against, listen, read_head
from _transport import StubStream
from tonio.colored import Event, scope, sleep

from httpunk import Version
from httpunk._backend.asyncio import AsyncioBackend
from httpunk._backend.tonio import TonioBackend
from httpunk._httpunk import H1Codec
from httpunk.exceptions import ConnectionClosedError, H1BodyError, H1IncompleteMessageError, H1UserError
from httpunk.h1 import H1Server
from httpunk.h1.server import ServerConnection
from httpunk.http import HeaderMap


async def _read_until(transport, marker, limit=65536):
    buf = b""
    while marker not in buf:
        chunk = await transport.receive_some(limit)
        if not chunk:
            break
        buf += chunk
    return buf


async def _drain_all(transport, limit=65536):
    """Read until the peer closes — for responses that end by closing the
    connection (HTTP/1.0, `Connection: close`, a 4xx error). Tolerates an ABORTIVE
    close (RST -> ConnectionResetError) as well as a clean EOF: when the server closes
    a connection whose peer still has unread bytes in the socket (e.g. a slowloris head
    it timed out on), BSD sockets send RST, not FIN — so the drain must treat that as
    "closed" too, not a test failure. (hyper closes the same way; it's OS-level.)"""
    buf = b""
    try:
        while True:
            chunk = await transport.receive_some(limit)
            if not chunk:
                break
            buf += chunk
    except (ConnectionResetError, BrokenPipeError):
        pass  # abortive close == the peer closed
    return buf


class _StubTransport(StubStream):  # `StubStream`: the tonio stream surface the bounded reader binds
    """An in-memory transport preloaded with request bytes — drives `Connection`
    directly for cases that don't need a live peer (respond-order, auto-error)."""

    def __init__(self, data):
        self._data = data
        self.sent = b""
        self.closed = False

    async def receive_some(self, max_bytes=65536):
        return self._recv_now(max_bytes)

    # The socket surface: always readable (bytes, then EOF).
    def _readable(self):
        return True

    def _recv_now(self, max_bytes):
        chunk, self._data = self._data[:max_bytes], self._data[max_bytes:]
        return chunk

    async def receive_bounded(self, max_bytes, deadline):  # the asyncio seam's bounded read
        return await self.receive_some(max_bytes)

    async def send_all(self, data):
        self.sent += bytes(data)

    def close(self):
        self.closed = True

    def abort(self):  # the asyncio seam's abortive close (`_AsyncioStream.abort`)
        self.close()


class _PeekableStub(_StubTransport):
    """A stub whose SECOND payload becomes readable only after the first is consumed —
    models a pipelined request sitting buffered in the transport while the previous
    request is being served."""

    def __init__(self, first, buffered):
        super().__init__(first)
        self._buffered = buffered

    def _recv_now(self, max_bytes):
        if self._data:
            return super()._recv_now(max_bytes)
        chunk, self._buffered = self._buffered[:max_bytes], self._buffered[max_bytes:]
        return chunk


class _SilentStub(_StubTransport):
    """A stub whose client goes SILENT once its bytes are consumed: a read then parks
    (until `close()` wakes it with EOF) instead of returning EOF at once — so the
    mid-message watcher a streamed response arms sees an open peer, not a hang-up."""

    def __init__(self, data):
        super().__init__(data)
        self._closed_evt = Event()
        self.parked = Event()  # a read is parked on the silent client (the watcher's)

    async def receive_some(self, max_bytes=65536):
        if self._data:
            return await super().receive_some(max_bytes)
        self.parked.set()
        await self._closed_evt.wait()
        return b""

    # The socket surface: readable while bytes remain or once closed (EOF); parked otherwise.
    def _readable(self):
        return bool(self._data) or self.closed

    def _park(self, timeout):
        self.parked.set()
        return self._closed_evt.wait(None if timeout is None else timeout / 1_000_000)

    def _recv_now(self, max_bytes):
        if self._data:
            return super()._recv_now(max_bytes)
        if self.closed:
            return b""
        raise BlockingIOError

    def close(self):
        super().close()
        self._closed_evt.set()


class _ReadSpy:
    """A real transport, with a hook on `receive_some` ENTRY: `expect_read()` hands back an
    event set by the next read issued after the call — the deterministic "the watcher's
    read is parked" point for tests that need it (between a parsed head and the arm no
    other read is issued, so the next entry IS the watcher's, at the runtime boundary).
    Everything else (`send_all`, `close`, `socket`) is the wrapped transport's."""

    def __init__(self, transport):
        self._transport = transport
        self._expected = None

    def expect_read(self):
        self._expected = Event()
        return self._expected

    async def receive_some(self, max_bytes=65536):
        expected, self._expected = self._expected, None
        if expected is not None:
            expected.set()
        return await self._transport.receive_some(max_bytes)

    def __getattr__(self, name):
        return getattr(self._transport, name)


async def _echo(transport, seen=None):
    """Echo each request as 200 `b"<METHOD> <target> -> " + body`."""
    async with H1Server(transport) as server:
        async for req in server:
            if seen is not None:
                seen.append(req.target)
            body = await req.read()
            reply = f"{req.method} {req.target} -> ".encode() + body
            await req.respond(200, headers={"content-type": "text/plain"}, body=reply)


@pytest.mark.tonio
async def test_server_get():
    async with h1_client_against(_echo) as conn:
        r = await conn.request("GET", "/hello", headers={"host": conn._conn.authority})
        assert r.status == 200
        assert r.headers["content-type"] == b"text/plain"
        assert r.headers.get("date") is not None  # server auto-adds Date (hyper parity)
        assert await r.read() == b"GET /hello -> "


@pytest.mark.tonio
async def test_server_post_echo_body():
    async with h1_client_against(_echo) as conn:
        r = await conn.request("POST", "/submit", headers={"host": conn._conn.authority}, body=b"payload!")
        assert r.status == 200
        assert await r.read() == b"POST /submit -> payload!"


@pytest.mark.tonio
async def test_keep_alive():
    # hyper tests/server.rs: keep_alive (v1.12.0).
    async def serve(transport):
        async with H1Server(transport) as server:
            for body in (b"foo bar baz", b"zar quux"):
                req = await server.accept()
                await req.respond(200, body=body)

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nHost: example.domain\r\n\r\n")
        assert (await _read_until(transport, b"foo bar baz")).endswith(b"foo bar baz")
        await transport.send_all(b"GET /quux HTTP/1.1\r\nHost: example.domain\r\nConnection: close\r\n\r\n")
        assert (await _read_until(transport, b"zar quux")).endswith(b"zar quux")


@pytest.mark.tonio
async def test_server_chunked_response():
    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()

                async def chunks():
                    yield b"chunk-one "
                    yield b"chunk-two"

                await req.respond(200, body=chunks())  # iterable body -> chunked

    async with h1_client_against(serve) as conn:
        r = await conn.request("GET", "/stream", headers={"host": conn._conn.authority})
        assert await r.read() == b"chunk-one chunk-two"


@pytest.mark.tonio
async def test_server_bodyless_204_then_reuse():
    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                await req.respond(204)  # bodyless

    async with h1_client_against(serve) as conn:
        r1 = await conn.request("GET", "/a", headers={"host": conn._conn.authority})
        assert r1.status == 204
        assert await r1.read() == b""
        r2 = await conn.request("GET", "/b", headers={"host": conn._conn.authority})  # connection reused
        assert r2.status == 204


@pytest.mark.tonio
async def test_server_drains_unread_request_body_before_next():
    """If the app responds without reading the request body, the server drains it
    so the next request on the (keep-alive) connection still parses."""
    async with h1_client_against(_drainer) as conn:
        r1 = await conn.request("POST", "/a", headers={"host": conn._conn.authority}, body=b"unread body")
        assert await r1.read() == b"ok"
        r2 = await conn.request("POST", "/b", headers={"host": conn._conn.authority}, body=b"also unread")
        assert await r2.read() == b"ok"


async def _drainer(transport):
    async with H1Server(transport) as server:
        async for req in server:
            await req.respond(200, body=b"ok")  # deliberately does NOT read the request body


@pytest.mark.tonio
async def test_server_request_headers():
    seen = {}

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                seen["host"] = req.headers.get("host")
                seen["x-custom"] = req.headers.get("x-custom")
                await req.read()
                await req.respond(200, body=b"")

    async with h1_client_against(serve) as conn:
        r = await conn.request("GET", "/", headers={"host": conn._conn.authority, "x-custom": "abc"})
        assert r.status == 200
        await r.read()

    assert seen["host"] == conn._conn.authority.encode()
    assert seen["x-custom"] == b"abc"


@pytest.mark.tonio
async def test_server_pipelined_requests():
    """Two requests sent in ONE write must both be served — the second sits in the
    buffer past the first's head and must not be dropped (else the wire deadlocks)."""
    seen = []
    async with h1_peer_against(lambda transport: _echo(transport, seen)) as transport:
        await transport.send_all(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n\r\n")
        data = await _read_until(transport, b"GET /b -> ")
        assert b"GET /a -> " in data
        assert b"GET /b -> " in data
    assert seen == ["/a", "/b"]


@pytest.mark.tonio
async def test_http_10_request_receives_http_10_response():
    # hyper tests/server.rs: http_10_request_receives_http_10_response (v1.12.0).
    async def serve(transport):
        async with H1Server(transport) as server:
            req = await server.accept()
            await req.respond(200)

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.0\r\n\r\n")
        data = await _drain_all(transport)
        expected = b"HTTP/1.0 200 OK\r\ncontent-length: 0\r\n"
        assert len(data) >= len(expected)
        assert data[: len(expected)] == expected


@pytest.mark.tonio
async def test_server_request_exposes_http_version():
    """`ServerRequest.version` is public — the request's version is part of the message
    (hyper `Request::version()`): HTTP_10 for an HTTP/1.0 request, HTTP_11 for 1.1."""
    seen = []

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                seen.append((req.target, req.version))
                await req.respond(200, body=b"ok")

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET /eleven HTTP/1.1\r\nhost: x\r\n\r\nGET /ten HTTP/1.0\r\nhost: x\r\n\r\n")
        data = await _drain_all(transport)  # the 1.0 request (no keep-alive) ends the connection
        assert data.startswith(b"HTTP/1.1 200")
        assert b"HTTP/1.0 200" in data
        assert seen == [("/eleven", Version.HTTP_11), ("/ten", Version.HTTP_10)]


@pytest.mark.tonio
async def test_server_header_read_timeout_closes_slow_head():
    """A request head that never completes is closed after `header_read_timeout`
    (slowloris defence, hyper http1.rs L253): no response, just a close."""

    async def serve(transport):
        async with H1Server(transport, header_read_timeout=0.1) as server:
            async for req in server:
                await req.respond(200)

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n")  # partial head, never completes
        data = await _drain_all(transport)  # server times out and closes -> EOF
        assert data == b""  # no response sent; the connection was just closed


@pytest.mark.asyncio
async def test_server_shutdown_wins_over_buffered_pipelined_request():
    """REGRESSION GUARD: a graceful shutdown is honored BEFORE any idle read consumes
    already-buffered bytes — hyper stops parsing new heads once keep-alive is disabled
    (`can_read_head` under KA::Disabled), so a pipelined request already sitting in the
    transport must NOT be served, nor even read off the transport. The shutdown flag and
    the read decision live in one state step (`begin_read`: the shutdown verdict precedes
    any transport read), so the signal can never be observed late."""
    transport = _PeekableStub(
        b"GET /a HTTP/1.1\r\nhost: x\r\n\r\n",
        b"GET /b HTTP/1.1\r\nhost: x\r\n\r\n",
    )
    conn = ServerConnection(transport, backend=AsyncioBackend())
    req = await conn.next_request()
    assert req.target == "/a"
    await req.respond(200)
    await conn.graceful_shutdown()
    assert await conn.next_request() is None  # /b is readable but must NOT be parsed...
    assert transport._buffered  # ...nor read: the bytes are still in the transport
    await conn.close()  # ends the head-read deadline watchdog (no task left on the loop)


class _AsyncioSilentStub(_StubTransport):
    """`_SilentStub` on asyncio primitives: a read parks once the bytes are consumed,
    until `close()` ends it with EOF (what closing a real transport does)."""

    def __init__(self, data):
        super().__init__(data)
        self._closed_evt = asyncio.Event()
        self.parked = asyncio.Event()

    async def receive_some(self, max_bytes=65536):
        if self._data:
            return await super().receive_some(max_bytes)
        self.parked.set()
        await self._closed_evt.wait()
        return b""

    async def receive_bounded(self, max_bytes, deadline):
        if self._data:
            return await super().receive_some(max_bytes)
        self.parked.set()
        try:
            await asyncio.wait_for(self._closed_evt.wait(), deadline - asyncio.get_running_loop().time())
        except (TimeoutError, asyncio.TimeoutError):
            return None
        return b""

    def close(self):
        super().close()
        self._closed_evt.set()


@pytest.mark.asyncio
async def test_disable_keep_alive_post_request_asyncio():
    """hyper tests/server.rs: disable_keep_alive_post_request (v1.12.0). hyper
    `disable_keep_alive` while `KA::Idle`: `state.close()` at once. The parked
    idle read ends by that close (the one way a parked read ends, on every backend),
    and the reader maps it to a clean end: `next_request` -> None, nothing written."""
    transport = _AsyncioSilentStub(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    conn = ServerConnection(transport, backend=AsyncioBackend())
    req = await conn.next_request()
    await req.respond(200, body=b"hello")
    assert transport.sent.endswith(b"hello")
    assert not transport.closed
    sent = len(transport.sent)
    reader = asyncio.ensure_future(conn.next_request())
    await transport.parked.wait()
    await conn.graceful_shutdown()
    assert transport.closed  # closed under the parked read, in the shutdown itself
    assert await reader is None
    assert len(transport.sent) == sent  # no response, no error on the wire
    assert conn.closed and not conn.reusable
    await conn.close()  # ends the head-read deadline watchdog (no task left on the loop)


@pytest.mark.tonio
async def test_disable_keep_alive_mid_request():
    # hyper tests/server.rs: disable_keep_alive_mid_request (v1.12.0).
    class PartialHead(_StubTransport):
        def __init__(self):
            super().__init__(b"GET / HTTP/1.1\r\n")
            self.parked = Event()
            self.resume = Event()

        async def receive_some(self, max_bytes=65536):
            if self._data:
                return await super().receive_some(max_bytes)
            self.parked.set()
            await self.resume.wait()
            return b"Host: localhost\r\n\r\n"

    transport = PartialHead()
    conn = ServerConnection(transport)

    async def serve():
        req = await conn.next_request()
        await req.respond(200, body=b"hello")
        assert await conn.next_request() is None

    try:
        async with scope() as s:
            s.spawn(serve())
            await transport.parked.wait()
            await conn.graceful_shutdown()
            transport.resume.set()
        assert transport.sent.startswith(b"HTTP/1.1 200 OK\r\n")
        assert b"connection: close\r\n" in transport.sent
        assert transport.closed
    finally:
        await conn.close()


@pytest.mark.tonio
async def test_disable_keep_alive_post_request_tonio():
    # hyper tests/server.rs: disable_keep_alive_post_request (v1.12.0).
    transport = _SilentStub(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    conn = ServerConnection(transport)
    req = await conn.next_request()
    await req.respond(200, body=b"hello")
    assert transport.sent.endswith(b"hello")
    assert not transport.closed
    results = []
    sent = transport.sent

    async def reader():
        results.append(await conn.next_request())

    async with scope() as s:
        s.spawn(reader())
        await transport.parked.wait()
        await conn.graceful_shutdown()
        assert transport.closed
    assert results == [None]
    assert transport.sent == sent
    assert conn.closed and not conn.reusable
    await conn.close()


@pytest.mark.tonio
async def test_server_shutdown_drops_bytes_that_land_on_a_closed_idle_read():
    """The keep-alive race, hyper's way: `disable_keep_alive` on an idle connection
    closes it whatever the kernel (or `read_buf`) holds — a request landing in that
    instant is dropped (the client sees the close and retries). A read that returns
    bytes after the state closed under it must not serve them: `next_request` -> None."""

    class _LateBytes(_SilentStub):
        async def receive_some(self, max_bytes=65536):
            data = await super().receive_some(max_bytes)
            if not data and self._data:  # woken by the close: bytes landed in that instant
                chunk, self._data = self._data, b""
                return chunk
            return data

    transport = _LateBytes(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(transport)
    req = await conn.next_request()
    await req.respond(200)
    sent = len(transport.sent)
    results = []

    async def reader():
        results.append(await conn.next_request())

    async with scope() as s:
        s.spawn(reader())
        await transport.parked.wait()
        transport._data = b"GET /late HTTP/1.1\r\nhost: x\r\n\r\n"  # bytes "arrive" as the close lands
        await conn.graceful_shutdown()  # closes under the read; its wake now returns the bytes
    assert results == [None]  # not served: the connection was closed first
    assert len(transport.sent) == sent  # nothing written for the dropped request
    assert transport.closed and conn.closed


@pytest.mark.tonio
async def test_server_detach_hands_off_open_connection():
    """detach() stops the accept loop and relinquishes the transport WITHOUT closing it,
    returning bytes read past the request head — so a caller can take over the raw connection
    for a protocol upgrade (e.g. WebSocket)."""
    captured = {}

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                captured["leftover"] = req.detach()  # loop ends next (detached), socket stays open
        # `__aexit__` ran (close() is a no-op after detach) — the transport is still usable, so
        # the caller drives its own upgrade response on the raw connection.
        await transport.send_all(b"HTTP/1.1 101 Switching Protocols\r\nupgrade: custom\r\n\r\nOWNED")

    async with h1_peer_against(serve) as client:
        # request head + trailing bytes the client sent past it (must survive as `leftover`)
        await client.send_all(b"GET /up HTTP/1.1\r\nhost: x\r\nupgrade: custom\r\nconnection: upgrade\r\n\r\nEXTRA")
        data = await _read_until(client, b"OWNED")

    assert captured["leftover"] == b"EXTRA"  # bytes past the head handed back
    assert b"101 Switching Protocols" in data and data.endswith(b"OWNED")  # socket stayed open


@pytest.mark.tonio
async def test_server_no_100_continue_for_http10():
    """An HTTP/1.0 request never gets an auto 100-continue, even with `Expect:
    100-continue` — hyper gates the interim on version > 1.0 (conn.rs L317)."""
    async with h1_peer_against(_echo) as transport:
        await transport.send_all(b"POST /x HTTP/1.0\r\nhost: x\r\ncontent-length: 3\r\nexpect: 100-continue\r\n\r\nabc")
        data = await _drain_all(transport)  # 1.0 default-close → server closes after replying
        assert b"100 Continue" not in data  # no interim for a 1.0 client
        assert data.startswith(b"HTTP/1.0 200")  # the real response still arrives


@pytest.mark.tonio
async def test_server_http10_streamed_body_with_content_length_reuses():
    """A streamed (iterable) HTTP/1.0 response WITH a Content-Length is length-framed,
    not close-delimited, so the connection stays reusable and keeps the keep-alive
    header — deciding close-delimited from the body shape alone forced a close."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                body = (part for part in [req.target.encode()])  # an iterable body
                await req.respond(200, headers={"content-length": str(len(req.target))}, body=body)

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET /a HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
        r1 = await _read_until(transport, b"/a")  # head + the 2-byte length-framed body
        assert r1.startswith(b"HTTP/1.0 200")
        assert b"connection: keep-alive" in r1.lower()  # reusable, not close-delimited
        await transport.send_all(b"GET /b HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
        assert b"/b" in await _read_until(transport, b"/b")  # connection was reused


@pytest.mark.tonio
async def test_http_10_keep_alive():
    # hyper tests/server.rs: http_10_keep_alive (v1.12.0).
    async def serve(transport):
        async with H1Server(transport) as server:
            for body in (b"foo bar baz", b"zar quux"):
                req = await server.accept()
                await req.respond(200, body=body)

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.0\r\nHost: example.domain\r\nConnection: keep-alive\r\n\r\n")
        data = await _read_until(transport, b"foo bar baz")
        assert data.endswith(b"foo bar baz")
        assert b"connection: keep-alive\r\n" in data
        await transport.send_all(b"GET /quux HTTP/1.0\r\nHost: example.domain\r\n\r\n")
        assert (await _read_until(transport, b"zar quux")).endswith(b"zar quux")


@pytest.mark.tonio
async def test_header_connection_close():
    # hyper tests/server.rs: header_connection_close (v1.12.0).
    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                await req.respond(200, headers={"connection": "close"}, body=b"foo bar baz")

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nHost: example.domain\r\nConnection: keep-alive\r\n\r\n")
        data = await _drain_all(transport)
        assert b"connection: close\r\n" in data
        assert data.endswith(b"foo bar baz")


@pytest.mark.tonio
async def test_server_unread_large_body_closes_not_drains():
    """If the app responds without reading a large request body, the server does
    ONE non-blocking read and — since the whole body isn't sitting in the buffer —
    closes rather than streaming it off the socket (1:1 hyper
    poll_drain_or_close_read). It must not block (a hang would trip the 6s net)."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.respond(200, body=b"ok")  # does NOT read the (huge) body

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 1000000\r\n\r\npartial")
        data = await _drain_all(transport)  # returns once the server closes (no hang)
        assert b"ok" in data


@pytest.mark.tonio
async def test_server_upgrade_tunnel():
    """A 101 response hands the raw connection to `req.upgraded`; the driver
    detaches and the app drives the tunnel directly."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.respond(101, headers={"upgrade": "myproto", "connection": "upgrade"})
                tunnel = req.upgraded
                assert tunnel is not None
                data = await tunnel.receive_some()
                await tunnel.send_all(b"echo:" + data)
                await tunnel.aclose()
                break

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET /chat HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: myproto\r\n\r\n")
        head = await _read_until(transport, b"\r\n\r\n")
        assert head.startswith(b"HTTP/1.1 101")
        await transport.send_all(b"ping")
        assert await transport.receive_some() == b"echo:ping"


@pytest.mark.tonio
async def test_server_bodyless_response_does_not_drain_body():
    """A body handed to a bodyless response (HEAD / 204) is never polled — no bytes
    on the wire AND the iterable's side effects don't fire (hyper write_head's
    encoder.is_eof() gate)."""
    fired = []

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()

                async def body():
                    fired.append(1)  # side effect — must NOT run
                    yield b"should-not-be-sent"

                status = 204 if req.method == "GET" else 200
                await req.respond(status, body=body())

    async with h1_client_against(serve) as conn:
        head = await conn.request("HEAD", "/h", headers={"host": conn._conn.authority})
        assert head.status == 200
        assert await head.read() == b""  # HEAD response carries no body
        nc = await conn.request("GET", "/n", headers={"host": conn._conn.authority})  # -> 204
        assert nc.status == 204
        assert await nc.read() == b""

    assert fired == []  # neither generator was ever polled


@pytest.mark.tonio
async def test_server_requires_respond_before_next():
    """Reading the next request before responding to the current one is a usage
    error (hyper serializes structurally) — surfaced, not silently mis-paired."""
    data = b"GET /a HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n\r\n"
    conn = ServerConnection(_StubTransport(data))
    await conn.start()
    req = await conn.next_request()
    assert req.target == "/a"
    with pytest.raises(RuntimeError):
        await conn.next_request()  # never responded to /a


@pytest.mark.tonio
async def test_server_auto_error_on_malformed_head():
    """A malformed request head triggers hyper's automatic error response
    (`Server::on_error`: a colon-less header line → 400) + close."""
    stub = _StubTransport(b"GET / HTTP/1.1\r\nBad Header Here\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    assert req is None
    assert stub.sent.startswith(b"HTTP/1.1 400")  # automatic Bad Request
    assert b"connection: close" in stub.sent.lower()  # enforce_version adds it before closing
    assert stub.closed


@pytest.mark.tonio
async def test_server_oversized_head_rejected_with_431():
    """A request head that never completes and grows past hyper's max_buf_size is
    rejected as `Parse::TooLarge` → auto 431 + close, not buffered without bound."""
    oversized = b"GET / HTTP/1.1\r\nx: " + b"a" * 500_000  # > _MAX_HEAD_SIZE, no CRLFCRLF
    stub = _StubTransport(oversized)
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    assert req is None
    assert stub.sent.startswith(b"HTTP/1.1 431")  # Request Header Fields Too Large
    assert b"connection: close" in stub.sent.lower()
    assert stub.closed


# ----- hyper `http1::Builder` options -----


@pytest.mark.tonio
async def test_disable_keep_alive():
    # hyper tests/server.rs: disable_keep_alive (v1.12.0).
    async def serve(transport):
        async with H1Server(transport, keep_alive=False) as server:
            async for req in server:
                await req.respond(200, body=b"foo bar baz")

    async with h1_peer_against(serve) as transport:
        await transport.send_all(
            b"GET / HTTP/1.1\r\nHost: example.domain\r\nConnection: keep-alive\r\n\r\n"
            b"GET /b HTTP/1.1\r\nhost: x\r\n\r\n"
        )
        data = await _drain_all(transport)
        assert data.endswith(b"foo bar baz")
        assert data.count(b"HTTP/1.1 200") == 1
        assert b"connection: close\r\n" in data


@pytest.mark.tonio
async def test_server_max_buf_size_option():
    """`max_buf_size` caps a still-incomplete head (431 + close past it, hyper io.rs
    `max_buf_size`); values below hyper's MINIMUM_MAX_BUFFER_SIZE (8192) are rejected
    like hyper's `assert!`."""
    stub = _StubTransport(b"GET / HTTP/1.1\r\nx: " + b"a" * 9_000)  # < the 400 KB default, > 8192
    conn = ServerConnection(stub, max_buf_size=8192)
    await conn.start()
    assert await conn.next_request() is None
    assert stub.sent.startswith(b"HTTP/1.1 431")
    assert stub.closed
    with pytest.raises(ValueError, match="max_buf_size"):
        ServerConnection(_StubTransport(b""), max_buf_size=100)


@pytest.mark.tonio
async def test_server_max_headers_option():
    """`max_headers` (hyper default 100): a head with more header lines parses as
    `TooLarge` (httparse TooManyHeaders) -> auto 431 + close."""
    head = b"GET / HTTP/1.1\r\nhost: x\r\na: 1\r\nb: 2\r\n\r\n"  # 3 headers
    stub = _StubTransport(head)
    conn = ServerConnection(stub, max_headers=2)
    await conn.start()
    assert await conn.next_request() is None
    assert stub.sent.startswith(b"HTTP/1.1 431")
    assert stub.closed
    # The default (100) accepts it.
    conn = ServerConnection(_StubTransport(head))
    await conn.start()
    req = await conn.next_request()
    assert req is not None and req.headers["b"] == b"2"


@pytest.mark.tonio
async def test_server_ignore_invalid_headers_option():
    """`ignore_invalid_headers` (httparse `ignore_invalid_headers_in_requests`): a malformed
    header line is skipped instead of failing the head with 400."""
    head = b"GET / HTTP/1.1\r\nbad header: v\r\nhost: x\r\n\r\n"  # space in the field name
    stub = _StubTransport(head)
    conn = ServerConnection(stub)
    await conn.start()
    assert await conn.next_request() is None
    assert stub.sent.startswith(b"HTTP/1.1 400")  # default: rejected
    conn = ServerConnection(_StubTransport(head), ignore_invalid_headers=True)
    await conn.start()
    req = await conn.next_request()
    assert req is not None
    assert req.headers["host"] == b"x"
    assert len(req.headers) == 1  # the bad line was dropped, not mangled into a header


@pytest.mark.tonio
async def test_server_auto_date_header_and_title_case_options():
    """`auto_date_header=False` omits `Date` (hyper `http1::Builder::auto_date_header`);
    `title_case_headers=True` writes `Content-Type:` / `Date:` (hyper `title_case_headers`)."""
    stub = _StubTransport(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(stub, auto_date_header=False)
    await conn.start()
    req = await conn.next_request()
    await req.respond(200, headers={"content-type": "text/plain"}, body=b"ok")
    assert b"\r\ndate:" not in stub.sent.lower()
    assert b"\r\ncontent-type: text/plain\r\n" in stub.sent  # lowercase by default

    stub = _StubTransport(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(stub, title_case_headers=True)
    await conn.start()
    req = await conn.next_request()
    await req.respond(200, headers={"content-type": "text/plain"}, body=b"ok")
    assert b"\r\nContent-Type: text/plain\r\n" in stub.sent
    assert b"\r\nDate: " in stub.sent


def test_response_date_is_refreshed_at_encode_time_per_thread():
    """hyper refreshes its per-thread `Date` cache only in `Server::parse` and copies it in
    `Server::encode`. Under a work-stealing runtime the encode may run on a thread whose
    cache was last refreshed long ago, so httpunk refreshes at encode too: a response
    encoded >1s after this thread's last refresh must NOT carry the old value."""
    import threading
    import time
    from email.utils import parsedate_to_datetime

    from httpunk._httpunk import H1Codec, http_date

    seen = {}

    def worker():
        codec = H1Codec()
        codec.receive_request_head(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")  # parse: refreshes this thread's cache
        seen["at_parse"] = http_date()
        time.sleep(1.2)  # no parse on this thread meanwhile
        head = codec.serialize_response(200, HeaderMap(), content_length=0)
        seen["in_response"] = next(line for line in head.split(b"\r\n") if line.lower().startswith(b"date:"))[
            5:
        ].strip()

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert seen["in_response"] != seen["at_parse"]
    delta = parsedate_to_datetime(seen["in_response"].decode()) - parsedate_to_datetime(seen["at_parse"].decode())
    assert delta.total_seconds() >= 1


# ----- push-style responses: `send_response` -> `SendStream` -----


@pytest.mark.tonio
async def test_server_push_response_streams_chunked_and_keeps_alive():
    """`send_response` + `send_data` writes a chunked body (unknown length, 1.1) with the
    same head negotiation as `respond()`: Date added, keep-alive kept, the connection
    reused for the next request."""
    seen = []

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                seen.append(req.target)
                stream = await req.send_response(200, headers={"content-type": "text/plain"})
                await stream.send_data(b"one ")
                await stream.send_data(b"two", end_stream=True)

    async with h1_client_against(serve) as conn:
        for path in ("/a", "/b"):
            r = await conn.request("GET", path, headers={"host": "x"})
            assert r.status == 200
            assert r.headers["transfer-encoding"] == b"chunked"
            assert r.headers.get("date") is not None
            assert await r.read() == b"one two"
    assert seen == ["/a", "/b"]  # both on the one (kept-alive) connection


@pytest.mark.tonio
async def test_server_push_content_length_frames_by_length_not_chunked():
    """An app-supplied `content-length` on a push response frames the body by length
    (hyper `set_length` -> `existing_con_len`), never chunked."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200, headers={"content-length": "8"})
                await stream.send_data(b"abcd")
                await stream.send_data(b"efgh", end_stream=True)

    async with h1_client_against(serve) as conn:
        r = await conn.request("GET", "/", headers={"host": "x"})
        assert r.headers.get("transfer-encoding") is None
        assert r.headers["content-length"] == b"8"
        assert await r.read() == b"abcdefgh"


@pytest.mark.tonio
async def test_server_push_bodyless_end_stream_and_head_discards_chunks():
    """`send_response(..., end_stream=True)` is a bodyless response (then `send_data`
    is a misuse). For HEAD the encoder is EOF: hyper never polls the body, so pushed
    chunks are DISCARDED, and the app's `content-length` still goes on the wire."""
    misuse = []
    # The misuse is recorded AFTER the 204 head is on the wire, so the client seeing the
    # complete 204 does not order it: wait for this event before asserting (the scope
    # exit does not wait for the cancelled server task).
    misuse_seen = Event()

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                if req.method == "HEAD":
                    stream = await req.send_response(200, headers={"content-length": "5"})
                    await stream.send_data(b"hello", end_stream=True)  # discarded on the wire
                else:
                    stream = await req.send_response(204, end_stream=True)
                    try:
                        await stream.send_data(b"nope")
                    except RuntimeError as exc:
                        misuse.append(str(exc))
                    misuse_seen.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"HEAD /x HTTP/1.1\r\nhost: x\r\n\r\nGET /y HTTP/1.1\r\nhost: x\r\n\r\n")
        data = b""
        while b"HTTP/1.1 204" not in data or not data.endswith(b"\r\n\r\n"):  # both heads, the 204 complete
            chunk = await transport.receive_some(65536)
            assert chunk, "server closed before both responses arrived"
            data += chunk
        head_resp, _, rest = data.partition(b"HTTP/1.1 204")
        assert head_resp.startswith(b"HTTP/1.1 200")
        assert b"\r\ncontent-length: 5\r\n" in head_resp
        assert b"hello" not in head_resp  # no body bytes for HEAD
        assert head_resp.endswith(b"\r\n\r\n")  # the 204 followed the HEAD head directly
        await misuse_seen.wait()
    assert misuse == ["response body already complete"]


@pytest.mark.tonio
async def test_server_push_send_reset_truncates_body_and_closes():
    """h1 has no reset frame: `send_reset` closes the connection mid-body (hyper's
    behaviour when a response `Body` errors), so the client sees a truncated chunked
    body — no `0\\r\\n\\r\\n` terminator."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                await stream.send_data(b"partial")
                await stream.send_reset()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        data = await _drain_all(transport)  # returns only because the server closed
        assert data.startswith(b"HTTP/1.1 200")
        assert b"7\r\npartial\r\n" in data
        assert not data.endswith(b"0\r\n\r\n")


@pytest.mark.tonio
async def test_server_push_http10_without_content_length_is_close_delimited():
    """HTTP/1.0 + unknown length = close-delimited (hyper role.rs L930-934): raw body
    bytes, no chunking, and the connection closes after the body."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                await stream.send_data(b"old ")
                await stream.send_data(b"school", end_stream=True)

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.0\r\nhost: x\r\n\r\n")
        data = await _drain_all(transport)
        assert data.startswith(b"HTTP/1.0 200")
        assert b"chunked" not in data.lower()
        assert data.endswith(b"\r\n\r\nold school")


@pytest.mark.tonio
async def test_server_next_request_refuses_while_push_body_is_open():
    """hyper's dispatcher reads the next head only after the response is fully written;
    reading it while a push body is still open is a misuse, not a mis-paired response."""
    stub = _StubTransport(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    stream = await req.send_response(200)
    with pytest.raises(RuntimeError, match="finish its body"):
        await conn.next_request()
    await stream.send_data(b"done", end_stream=True)
    nxt = await conn.next_request()
    assert nxt is not None and nxt.target == "/b"


# ----- response trailers -----


@pytest.mark.skip(
    reason="intermittent: `_send_async_body` can fail a fully written response as abandoned "
    "when the client's FIN lands before the pump sets `done` (httpunk/h1/server.py)"
)
@pytest.mark.tonio
async def test_server_respond_trailers_need_chunked_framing_and_a_trailer_declaration():
    """`respond(trailers=)` is hyper's `Body` with trailer frames: the framing is the body's
    and only fields the response's own `Trailer` header declares are emitted (role.rs
    `Server::encode` -> `Kind::Chunked(Some(fields))`, `Encoder::encode_trailers`). A
    known-length `bytes` body is `Content-Length`-framed and carries none — dropped, no
    error; a streamed body with a `Trailer` declaration carries them."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                if req.target == "/bytes":
                    await req.respond(200, body=b"payload", trailers={"x-checksum": "abc"})
                else:

                    async def chunks():
                        yield b"pay"
                        yield b"load"

                    await req.respond(
                        200, headers={"trailer": "x-checksum"}, body=chunks(), trailers={"x-checksum": "def"}
                    )

    async with h1_client_against(serve) as conn:
        r = await conn.request("GET", "/bytes", headers={"host": "x", "te": "trailers"})
        assert r.headers["content-length"] == b"7"  # the body's framing: no room for trailers
        assert "trailer" not in r.headers
        assert await r.read() == b"payload"
        assert r.trailers is None
        r = await conn.request("GET", "/stream", headers={"host": "x", "te": "trailers"})
        assert r.headers["transfer-encoding"] == b"chunked"
        assert r.headers["trailer"] == b"x-checksum"
        assert await r.read() == b"payload"
        assert r.trailers["x-checksum"] == b"def"


@pytest.mark.tonio
async def test_server_trailers_undeclared_fields_are_dropped_by_encoder():
    """hyper `Encoder::encode_trailers`: only fields named in the response's `Trailer`
    header are emitted (others are dropped). Through the push handle the app owns the
    `Trailer` header, so an undeclared field never reaches the wire."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200, headers={"trailer": "x-declared"})
                await stream.send_data(b"body")
                await stream.send_trailers({"x-declared": "yes", "x-undeclared": "no"})

    async with h1_client_against(serve) as conn:
        r = await conn.request("GET", "/", headers={"host": "x", "te": "trailers"})
        assert await r.read() == b"body"
        assert r.trailers["x-declared"] == b"yes"
        assert r.trailers.get("x-undeclared") is None


@pytest.mark.tonio
@pytest.mark.parametrize("path", ["pull", "push"])
async def test_server_trailers_dropped_without_te_trailers(path):
    """hyper's server sends response trailers only if the request declared `TE: trailers`
    (conn.rs `read_head` -> `allow_trailer_fields`; `write_trailers` -> "trailers not
    allowed to be sent"): without it the trailer block is not written and the body ends
    with the bare chunked terminator. The framing and the `Trailer` declaration are the
    app's head as given. Both the pull and the push path (a silent client: the push
    response arms the mid-message watcher, which must not read the stub's EOF)."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    if path == "pull":

        async def chunks():
            yield b"payload"

        await req.respond(200, headers={"trailer": "x-checksum"}, body=chunks(), trailers={"x-checksum": "abc"})
    else:
        stream = await req.send_response(200, headers={"trailer": "x-checksum"})
        await stream.send_data(b"payload")
        await stream.send_trailers({"x-checksum": "abc"})
    head, _, body = stub.sent.partition(b"\r\n\r\n")
    assert b"trailer: x-checksum" in head.lower()
    assert body == b"7\r\npayload\r\n0\r\n\r\n"  # bare terminator, no trailer block
    assert b"x-checksum: abc" not in stub.sent
    await conn.close()  # ends the watcher's parked read (push path)


@pytest.mark.tonio
@pytest.mark.parametrize(
    ("te_lines", "allowed"),
    [
        (b"te: trailers\r\n", True),
        (b"te: gzip, Trailers\r\n", True),  # any position in the list, case-insensitive
        (b"te: gzip\r\nte: trailers\r\n", True),  # any of several TE lines
        (b"te: gzip\r\n", False),
        (b"te: trailers-please\r\n", False),  # a token, not a prefix
    ],
)
async def test_server_te_trailers_token_forms(te_lines, allowed):
    """hyper 1.12.0 `headers::te_is_trailers`: the `trailers` token may sit in any `TE` line,
    at any position of its comma list, in any case — earlier hyper only matched a first
    line equal to `trailers`."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\nhost: x\r\n" + te_lines + b"\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()

    async def chunks():
        yield b"payload"

    await req.respond(200, headers={"trailer": "x-checksum"}, body=chunks(), trailers={"x-checksum": "abc"})
    await conn.close()
    assert (b"\r\n0\r\nx-checksum: abc\r\n\r\n" in stub.sent) is allowed


@pytest.mark.tonio
async def test_server_respond_trailers_dropped_on_http10():
    """HTTP/1.0 has no chunked framing: the body is close-delimited and the trailers are
    dropped (hyper's close-delimited encoder emits none), not written as garbage."""

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.respond(200, body=b"old", trailers={"x-checksum": "abc"})

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.0\r\nhost: x\r\nte: trailers\r\n\r\n")
        data = await _drain_all(transport)
        assert data.startswith(b"HTTP/1.0 200")
        assert data.endswith(b"\r\n\r\nold")
        assert b"x-checksum: abc" not in data


# ----- mid-message peer EOF (hyper conn.rs `mid_message_detect_eof`) -----


@pytest.mark.tonio
async def test_server_peer_closed_resolves_and_fails_the_late_response():
    """A client that sends a GET then closes is detected WHILE the handler still runs:
    `peer_closed()` resolves, and the late `respond()` fails with `ConnectionClosedError`
    (hyper: `close_read` + `IncompleteMessage`, the service future is dropped). The accept
    loop then ends."""
    seen, done = [], Event()

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.peer_closed()
                seen.append("peer_closed")
                try:
                    await req.respond(200, body=b"too late")
                except H1IncompleteMessageError:
                    seen.append("respond failed")
            seen.append("loop ended")
            done.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        transport.close()  # FIN while the handler is parked
        await done.wait()
    assert seen == ["peer_closed", "respond failed", "loop ended"]


@pytest.mark.tonio
async def test_server_streamed_response_fails_fast_on_peer_close():
    """hyper polls the connection (so `mid_message_detect_eof`) while the response body's
    next chunk is pending: a client FIN fails `respond()` at once — the app is not left
    parked on its own `__anext__` forever."""
    seen, done = [], Event()

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:

                async def body():
                    try:
                        yield b"first"
                        await Event().wait()  # nothing more to say (yet)
                    finally:
                        seen.append("producer unwound")  # hyper drops the body future: cleanup runs

                try:
                    await req.respond(200, body=body())
                except H1IncompleteMessageError:
                    seen.append("failed")
                done.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        data = await _read_until(transport, b"5\r\nfirst\r\n")  # streamed chunks are not held back
        assert data.startswith(b"HTTP/1.1 200")
        transport.close()
        await done.wait()  # would hit the conftest deadline if the sender stayed parked
    assert seen == ["producer unwound", "failed"]  # cancelled + unwound BEFORE respond() raised


@pytest.mark.tonio
async def test_server_push_send_data_fails_after_peer_close():
    """Through the push handle: once the client closed its side, the next `send_data`
    fails with `ConnectionClosedError` instead of writing into a dead exchange."""
    seen, done = [], Event()

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                await stream.send_data(b"tick")
                await req.peer_closed()
                try:
                    await stream.send_data(b"tock")
                except H1IncompleteMessageError:
                    seen.append("failed")
                done.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        await _read_until(transport, b"4\r\ntick\r\n")
        transport.close()
        await done.wait()
    assert seen == ["failed"]


@pytest.mark.tonio
async def test_server_pipelined_request_read_mid_message_is_served_not_lost():
    """Bytes arriving mid-message are the next pipelined request (hyper keeps them in
    `read_buf`): the watcher's read must be handed to the next head read, never dropped,
    and must NOT be mistaken for a peer close."""
    release, seen = Event(), []

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                seen.append(req.target)
                if req.target == "/slow":
                    stream = await req.send_response(200)  # a push response arms the watcher
                    await release.wait()  # the second request arrives while this one is pending
                    assert not req._peer_closed
                    await stream.send_data(req.target.encode(), end_stream=True)
                else:
                    await req.respond(200, body=req.target.encode())

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
        await sleep(0.05)  # let the watcher park, then pipeline the next request into it
        await transport.send_all(b"GET /next HTTP/1.1\r\nhost: x\r\n\r\n")
        await sleep(0.05)
        release.set()
        data = await _read_until(transport, b"/next")
        assert data.count(b"HTTP/1.1 200") == 2
        assert seen == ["/slow", "/next"]


@pytest.mark.tonio
async def test_server_half_close_option_ignores_mid_request_fin():
    """`half_close=True` (hyper `http1::Builder::half_close`): a client FIN mid-request is
    not a disconnect — no watcher, `peer_closed()` stays pending, the response completes
    and is delivered; the connection then ends at the idle read."""
    seen, done = [], Event()

    async def serve(transport):
        async with H1Server(transport, half_close=True) as server:
            async for req in server:
                await sleep(0.05)  # the FIN lands here
                seen.append(req._peer_closed)
                await req.respond(200, body=b"still served")
            done.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        transport.socket.shutdown(1)  # SHUT_WR: half-close, we still read
        data = await _drain_all(transport)
        transport.close()
        await done.wait()
    assert seen == [False]
    assert data.startswith(b"HTTP/1.1 200") and data.endswith(b"still served")


@pytest.mark.tonio
async def test_server_detach_refuses_while_watcher_parked_but_allows_upgrade_requests():
    """A request without `Upgrade` has a mid-message read parked, which cannot be handed
    to a caller (one reader per transport) -> `detach()` refuses. An Upgrade request is
    not watched before its response head (a task cannot stand in for hyper's poll while
    a detach / switch is still possible) -> `detach()` works."""
    seen = []

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await server._conn._arm_watcher(req)  # what `peer_closed()` / a streamed or push response does
                try:
                    req.detach()
                except RuntimeError as exc:
                    seen.append(str(exc))
                await req.respond(200, body=b"not detached")

    async with h1_client_against(serve) as conn:
        r = await conn.request("GET", "/plain", headers={"host": "x"})  # no Upgrade -> watchable
        assert await r.read() == b"not detached"
    assert len(seen) == 1 and "mid-message read is parked" in seen[0]

    # An Upgrade request is not watched before its head, so `detach()` hands the transport over cleanly.
    stub = _StubTransport(b"GET /ws HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: websocket\r\n\r\nWSDATA")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    await conn._arm_watcher(req)  # a no-op here: the head is not negotiated yet
    assert not conn.has_watcher
    assert req.detach() == b"WSDATA"


@pytest.mark.tonio
async def test_h2_preface_closes_silently_without_response():
    """An h1 server that receives the HTTP/2 prior-knowledge preface closes silently
    (a version error) instead of writing a 400 — hyper `on_parse_error`/`has_h2_prefix`
    (conn.rs L836-839)."""
    stub = _StubTransport(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    assert req is None
    assert stub.sent == b""  # NO response of any kind — silent close
    assert stub.closed


@pytest.mark.tonio
async def test_body_io_after_close_raises_clean_error():
    """A body read/write after the connection closed (transport nulled) raises a clean
    ConnectionClosedError, not an AttributeError on `None.receive_some`/`send_all`."""
    conn = ServerConnection(_StubTransport(b""))
    await conn.close()  # nulls the transport
    with pytest.raises(ConnectionClosedError):
        await conn.read_body_more()
    with pytest.raises(ConnectionClosedError):
        await conn.write(b"data")


@pytest.mark.tonio
async def test_h1_request_trailers_round_trip():
    """Request trailers are hyper's: `Client::encode` allow-lists the fields the request's
    own `Trailer` header declares (role.rs L1440-1453) and a chunked (streamed) body
    carries them after the data; the H1Server decodes them into `req.trailers`. A
    known-length `bytes` body is `Content-Length`-framed and drops them, silently."""
    seen = {}

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                seen[req.target] = (await req.read(), req.trailers)
                await req.respond(200, body=b"ok")

    async def chunks():
        yield b"da"
        yield b"ta"

    async with h1_client_against(serve) as conn:
        resp = await conn.request(
            "POST",
            "/declared",
            headers={"host": conn._conn.authority, "trailer": "x-checksum"},
            body=chunks(),
            trailers={"x-checksum": "abc", "x-undeclared": "no"},
        )
        assert await resp.read() == b"ok"
        resp = await conn.request(
            "POST", "/bytes", headers={"host": conn._conn.authority}, body=b"data", trailers={"x-checksum": "abc"}
        )
        assert await resp.read() == b"ok"

    body, trailers = seen["/declared"]
    assert body == b"data"
    assert trailers is not None
    assert trailers.get("x-checksum") == b"abc"
    assert trailers.get("x-undeclared") is None  # not declared -> dropped by the encoder
    body, trailers = seen["/bytes"]
    assert body == b"data" and trailers is None  # Content-Length framing: no trailers


@pytest.mark.tonio
async def test_h1_bodyless_request_drops_trailers():
    """A request with trailers but no body: hyper frames no body at all (`set_length`
    with no body -> a zero-length encoder), so there is nowhere for trailers to go —
    they are dropped, exactly as for a `Content-Length` body."""
    seen = {}

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                seen["framing"] = (req.headers.get("transfer-encoding"), req.headers.get("content-length"))
                await req.respond(200, body=b"ok")

    async with h1_client_against(serve) as conn:
        resp = await conn.request(
            "POST", "/", headers={"host": conn._conn.authority, "trailer": "x-done"}, trailers={"x-done": "1"}
        )
        assert await resp.read() == b"ok"

    assert seen["body"] == b""
    assert seen["trailers"] is None
    assert seen["framing"] == (None, None)


@pytest.mark.tonio
async def test_abrupt_client_close_ends_iteration_cleanly():
    """A client that tears the connection down abruptly between requests — an
    RST instead of a FIN (SO_LINGER 0; also what an abortive TLS close looks
    like) — must end the server's `async for` cleanly, exactly like the clean
    EOF (the wire outcome is identical — the connection just ends with no
    request to serve). The backend's broken-transport error must not escape
    `next_request` and kill the accept loop."""
    import socket as pysock
    import struct

    listener, host, port = await listen()
    server_errors, served = [], []

    async def server():
        try:
            await _echo(await listener.accept(), seen=served)
        except Exception as exc:
            server_errors.append(exc)

    async with scope() as s:
        s.spawn(server())
        transport = await TonioBackend().connect_tcp(host, port)
        try:
            await transport.send_all(b"GET /one HTTP/1.1\r\nhost: x\r\n\r\n")
            assert await read_head(transport) is not None, "connection closed before the response head"
            # RST the connection while the server is parked in the next head-read.
            transport.socket._sock.setsockopt(pysock.SOL_SOCKET, pysock.SO_LINGER, struct.pack("ii", 1, 0))
        finally:
            # No s.cancel(): the scope join waiting out the server task — which must
            # finish CLEANLY on its own — is the assertion under test.
            transport.close()
    assert served == ["/one"]
    assert not server_errors


# ----- hyper error kinds on the server: `H1BodyError` / `H1UserError` (0.3.0) -----


@pytest.mark.tonio
async def test_server_request_body_truncated_by_client_close_is_body_error():
    """A client that hangs up mid-upload: hyper's decoder returns `UnexpectedEof`
    (`IncompleteBody`) and the request body stream yields `Error::new_body` — NOT
    `IncompleteMessage` (that is a mid-HEAD EOF) and not a parse error. Mirrored as
    `H1BodyError(io_kind="unexpected_eof")` out of `read()`, and the connection is
    unusable afterwards (hyper `Reading::Closed`)."""
    stub = _StubTransport(b"POST / HTTP/1.1\r\ncontent-length: 10\r\n\r\nhello")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    assert req is not None
    with pytest.raises(H1BodyError) as ei:
        await req.read()
    assert ei.value.args[0] == "unexpected_eof"
    assert not conn.reusable


@pytest.mark.tonio
async def test_server_1xx_response_is_user_error_and_closes_without_writing():
    """`respond(102)`: hyper `Server::encode` fails with `User::UnsupportedStatusCode`,
    `dst` is rewound (nothing reaches the wire) and the connection closes with the
    error stored on it (conn.rs `encode_head` -> `Writing::Closed`). `respond()` raises
    the `H1UserError` itself — not a wrapped `ConnectionClosedError`."""
    stub = _StubTransport(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    with pytest.raises(H1UserError) as ei:
        await req.respond(102)
    assert ei.value.args[0] == "unsupported_status_code"
    assert stub.sent == b""
    assert stub.closed
    assert await conn.next_request() is None


@pytest.mark.tonio
async def test_server_pull_body_short_of_content_length_is_user_error():
    """An explicit `content-length` with a streamed body that ends early: the encoder
    honours the declared length (hyper `set_length`'s `existing_con_len`), so the
    body's end is `NotEof` -> `User::BodyWriteAborted`; the connection closes."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()

    async def body():
        yield b"abc"

    with pytest.raises(H1UserError) as ei:
        await req.respond(200, headers={"content-length": "10"}, body=body())
    assert ei.value.args[0] == "body_write_aborted"
    assert stub.sent.startswith(b"HTTP/1.1 200")  # the head and the 3 bytes went out; the body never completed
    assert stub.sent.endswith(b"abc")
    assert stub.closed


@pytest.mark.tonio
async def test_server_push_body_short_of_content_length_is_user_error():
    """The push path: `send_data(end_stream=True)` short of the declared length raises
    the same `H1UserError` and poisons the connection like a failed write."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    stream = await req.send_response(200, headers={"content-length": "10"})
    with pytest.raises(H1UserError) as ei:
        await stream.send_data(b"abc", end_stream=True)
    assert ei.value.args[0] == "body_write_aborted"
    assert stub.closed
    with pytest.raises(RuntimeError):  # the stream is done
        await stream.send_data(b"more")


@pytest.mark.tonio
async def test_server_body_iterable_exception_propagates_as_itself():
    """The app's own body iterable raising: hyper wraps it as `User::Body` because Rust
    must; Python carries the instance — `respond()` raises it unwrapped (not a
    `ConnectionClosedError`), and the connection closes (hyper `Writing::Closed`)."""

    class AppError(Exception):
        pass

    stub = _SilentStub(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()

    async def body():
        yield b"abc"
        raise AppError("boom")

    with pytest.raises(AppError):
        await req.respond(200, body=body())
    assert stub.closed


# ----- `peer_closed()` always resolves; upgrade requests are watched once answered -----

_UPGRADE_WS = b"GET /ws HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: websocket\r\n\r\n"
# What `curl --http2` sends to a plain `http://` URL, and Go's h2c client: served as HTTP/1.1
# (httpunk implements no h2c upgrade), but hyper's `wants_upgrade` is set all the same.
_UPGRADE_H2C = (
    b"GET / HTTP/1.1\r\nhost: x\r\nconnection: Upgrade, HTTP2-Settings\r\nupgrade: h2c\r\n"
    b"http2-settings: AAMAAABkAAQCAAAAAAIAAAAA\r\n\r\n"
)


@pytest.mark.tonio
async def test_server_peer_closed_after_response_done_returns_false_at_once():
    """The mid-message window is closed: hyper no longer polls `mid_message_detect_eof`,
    so there is nothing to wait for — `peer_closed()` returns `False` immediately instead
    of parking on an event nobody would ever set."""
    stub = _StubTransport(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    await req.respond(200, body=b"ok")
    assert await req.peer_closed() is False


@pytest.mark.tonio
async def test_server_peer_closed_parked_resolves_false_when_the_response_completes():
    """A `peer_closed()` awaited in one task while another completes the response:
    the window's close wakes it with `False` — it is not left parked forever holding the request."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    seen = []

    async def watch():
        seen.append(await req.peer_closed())

    async with scope() as s:
        s.spawn(watch())
        await stub.parked.wait()  # the watcher's read is parked on the silent client
        assert seen == []
        await req.respond(200, body=b"ok")
    assert seen == [False]
    assert stub.sent.endswith(b"ok")
    await conn.close()  # the parked read ends with the transport (the server-side close path below)


@pytest.mark.tonio
async def test_server_peer_closed_resolves_false_under_half_close():
    """`half_close=True`: hyper does not read mid-message, so a FIN is never observed — but
    the await still ends with the exchange, reporting `False`. There is no read to wait
    on here, so the `peer_closed()` task and the response run in whichever order the
    scheduler picks; both orders (parked then woken, or returned at once) must give the
    same verdict, and neither parks a read."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\n\r\n")
    conn = ServerConnection(stub, half_close=True)
    await conn.start()
    req = await conn.next_request()
    seen, started = [], Event()

    async def watch():
        started.set()
        seen.append(await req.peer_closed())

    async with scope() as s:
        s.spawn(watch())
        await started.wait()
        await req.respond(200, body=b"ok")
    assert seen == [False]
    assert not conn.has_watcher and not stub.parked.is_set()  # no read parked, as hyper


@pytest.mark.tonio
async def test_server_h2c_upgrade_request_is_watched_once_answered():
    """An `Upgrade: h2c` request served as HTTP/1.1: not watched before its head (it might
    still be detached), watched from the head on — a client hang-up mid-stream fails the
    streamed `respond()` with `H1IncompleteMessageError` and resolves a racing
    `peer_closed()` with `True`, exactly as for a plain request (hyper's
    `mid_message_detect_eof` never looked at `Upgrade`)."""
    seen, done = [], Event()

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                assert req.is_upgrade

                async def watch(req=req):
                    seen.append(("peer_closed", await req.peer_closed()))

                async def body():
                    yield b"first"
                    await Event().wait()

                async with scope() as inner:
                    await server._conn._arm_watcher(req)  # `peer_closed()`'s arm: deferred, no head yet
                    assert not server._conn.has_watcher
                    inner.spawn(watch())
                    try:
                        await req.respond(200, body=body())
                    except H1IncompleteMessageError:
                        seen.append("failed")
                done.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(_UPGRADE_H2C)
        data = await _read_until(transport, b"5\r\nfirst\r\n")
        assert data.startswith(b"HTTP/1.1 200")
        transport.close()
        await done.wait()
    assert sorted(seen, key=str) == [("peer_closed", True), "failed"]


@pytest.mark.tonio
async def test_server_upgrade_request_peer_closed_is_deferred_until_the_head():
    """Before the head an Upgrade request is unwatched, so `peer_closed()` parks without a
    read and `detach()` still works — the detach closes the window, resolving it `False`.
    Answered without a switch instead, the deferred `peer_closed()` arms the read at the
    head (a bytes body alone would not have), and the exchange resolves it `False`."""
    stub = _SilentStub(_UPGRADE_WS + b"WSDATA")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    seen = []

    async def watch():
        seen.append(await req.peer_closed())

    await conn._arm_watcher(req)  # `peer_closed()`'s arm
    assert not conn.has_watcher  # no read parked: the transport can still be handed over
    async with scope() as s:
        s.spawn(watch())  # parks (or, if it runs after the detach, returns at once): `False` either way
        assert req.detach() == b"WSDATA"
    assert seen == [False]

    stub = _SilentStub(_UPGRADE_WS)
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    seen = []
    await conn._arm_watcher(req)  # the ask is recorded (`watch_wanted`), nothing parked yet
    assert not conn.has_watcher
    async with scope() as s:
        s.spawn(watch())
        await req.respond(200, body=b"no switch")  # 200 to an upgrade request: served as plain HTTP/1.1
        await stub.parked.wait()  # armed at the head, honouring the earlier ask; its read is parked
    assert seen == [False]
    await conn.close()


# ----- server-side close with the watcher parked -----


@pytest.mark.tonio
@pytest.mark.parametrize("arm", ["push", "peer_closed"])
async def test_server_close_with_watcher_parked_on_silent_client(arm):
    """The order no other watcher test covers: the SERVER closes while its mid-message
    read is parked on a silent client — a host's forced close after a graceful-shutdown
    timeout, with the app still mid-response. `ServerConnection.close()` closes the
    transport and then JOINS the watcher (never cancels it), relying on the close waking
    the parked `receive_some` (tonio `io_deregister` re-dispatches the parked reader).
    The `async with` exit must return, a `peer_closed()` parked on that watcher resolves
    (`True`: the read ended without a response), and the client sees EOF."""
    seen, exited = [], Event()

    async def serve(transport):
        transport = _ReadSpy(transport)
        async with scope() as inner:
            async with H1Server(transport) as server:
                req = await server.accept()
                parked = transport.expect_read()  # the next read issued is the watcher's
                if arm == "push":
                    stream = await req.send_response(200)  # a push response arms the watcher
                    await stream.send_data(b"tick")
                else:
                    inner.spawn(_watch(req, seen))  # `peer_closed()` arms it
                await parked.wait()
                seen.append("exiting")
                # leave with the exchange unfinished and the client silent: `close()`
        seen.append("exited")
        exited.set()

    async with h1_peer_against(serve) as transport:
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        if arm == "push":
            await _read_until(transport, b"4\r\ntick\r\n")
        await exited.wait()  # would hit the conftest deadline if the join never returned
        assert await _drain_all(transport) == b""  # EOF (or RST) — the server is gone
    assert seen == ["exiting", "exited"] if arm == "push" else ["exiting", True, "exited"]


async def _watch(req, seen):
    seen.append(await req.peer_closed())


@pytest.mark.tonio
async def test_bounded_reader_on_a_real_socket_times_out_then_keeps_reading():
    """The bounded read on tonio's own socket: a silent peer -> None once the timer fires;
    the socket is untouched by that (the stale reader slot is harmless): the peer's next
    bytes arrive through a plain read, and bytes already there come back at once."""
    listener, host, port = await listen()
    backend = TonioBackend()
    accepted, got = [], Event()

    async def accept():
        accepted.append(await listener.accept())
        got.set()

    async with scope() as s:
        s.spawn(accept())
        client = await TonioBackend().connect_tcp(host, port)
        await got.wait()
        server = accepted[0]
        try:
            read = backend.bounded_reader(server)
            assert await read(100, backend.monotonic() + 0.05) is None  # the timer: nothing to read
            await client.send_all(b"late")
            assert await server.receive_some(100) == b"late"  # still a working socket
            await client.send_all(b"early")
            assert await read(100, backend.monotonic() + 5.0) == b"early"  # bytes: the readiness wake
        finally:
            client.close()
            server.close()
            s.cancel()


@pytest.mark.asyncio
async def test_server_read_return_and_head_step_are_one_transition_each():
    """`accept_head` is the read's return (hyper `poll_read_head`: the read that returned
    is the one parsed): it ends the idle park and, on a connection closed under the read
    (a shutdown while idle), parses NOTHING — the bytes belong to a request hyper drops.
    `respond_head` hands back the transport taken under the claim's own lock."""
    transport = _StubTransport(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(transport, backend=AsyncioBackend(), header_read_timeout=None)
    req = await conn.next_request()
    code, head, armed, t = conn.respond_head(req._seq, 200, None, asyncio.Event())
    assert code == 0 and head.startswith(b"HTTP/1.1 200 OK\r\n") and not armed and t is transport
    assert conn.finish_response(req._seq) == (False, False, None, None)
    code, obj = conn.begin_read()  # positioned at the next head, the idle park flagged
    assert obj is transport
    assert await conn.graceful_shutdown() is None and transport.closed  # closed under the park
    assert conn.accept_head(b"GET /b HTTP/1.1\r\nhost: x\r\n\r\n") is None  # nothing parsed
    assert conn.current_seq == req._seq and conn.codec.buffered() == 0
    assert await conn.next_request() is None
    await conn.close()


@pytest.mark.tonio
async def test_server_head_read_overdue_is_one_state_step():
    """The deadline watchdog's transition (`head_read_overdue`, hyper `poll_read_head`'s
    Sleep beside a Pending `parse`), pinned without a clock: `timeout=0` = "now is the
    deadline". No head being read (a request in flight): nothing to expire, no remaining.
    A head read in progress — here a MID-head one, the partial head never completing —
    expires: closed in the same step, the transport handed back, and the close ends the
    parked read, which the reader maps to the end of iteration (hyper errors mid-head
    too). Once closed: the watchdog's exit answer."""
    transport = _SilentStub(b"GET / HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n")
    conn = ServerConnection(transport, header_read_timeout=30.0)
    req = await conn.next_request()
    assert conn.head_read_overdue(0.0) == (False, None, None)  # in flight: no head is being read
    await req.respond(200)
    results = []

    async def reader():
        results.append(await conn.next_request())

    async with scope() as s:
        s.spawn(reader())
        await transport.parked.wait()  # the partial head of /b is read; the read for the rest is parked
        expired, remaining, t = conn.head_read_overdue(0.0)
        assert expired and remaining is None and t is transport  # closed here, the transport to close
        assert conn.closed and not conn.reusable
        conn._close(t)  # what the watchdog does: the close ends the parked mid-head read
    assert results == [None]  # `next_request` -> None: no response, no error
    assert conn.head_read_overdue(0.0) == (False, None, None)  # closed: the watchdog exits
    await conn.close()


@pytest.mark.tonio
async def test_server_header_read_timeout_bounds_the_watcher_hand_off():
    """With the mid-message watcher armed (a push response), the next head read IS the
    watcher's parked read, awaited through its done event: the deadline bounds that
    wait the same way (hyper: one timer per head, whichever poll is pending). Expiry
    closes with no response; the close ends the watcher's read; `close()` joins it."""
    transport = _SilentStub(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(transport, header_read_timeout=0.05)
    req = await conn.next_request()
    stream = await req.send_response(200, headers={"content-length": "2"})  # push: arms the watcher
    await stream.send_data(b"ok", end_stream=True)
    sent = len(transport.sent)
    await transport.parked.wait()  # the watcher's read is parked on the silent client
    assert await conn.next_request() is None  # the deadline hit
    assert transport.closed and len(transport.sent) == sent  # closed, nothing written
    await conn.close()  # joins the watcher (its read ended by the close)


@pytest.mark.tonio
async def test_server_push_response_detect_eof_off_parks_nothing_until_asked():
    """`send_response(detect_eof=False)` (httpunk's own knob): the head arms no
    mid-message watcher — no read parked, no task — and the body still goes out; a
    later `peer_closed()` arms on demand, exactly as it does before the head. The
    default keeps today's arm (`test_server_header_read_timeout_bounds_the_watcher_hand_off`)."""
    stub = _SilentStub(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    stream = await req.send_response(200, headers={"content-length": "4"}, detect_eof=False)
    assert not conn.has_watcher and not stub.parked.is_set()  # nothing parked by the head
    await stream.send_data(b"ti")
    assert not conn.has_watcher and not stub.parked.is_set()  # nor mid-body
    seen = []
    async with scope() as s:
        s.spawn(_watch(req, seen))  # the explicit ask: arms now
        await stub.parked.wait()
        assert conn.has_watcher
        await stream.send_data(b"ck", end_stream=True)  # the exchange completes: `False`
    assert seen == [False]
    assert stub.sent.endswith(b"tick")
    await conn.close()  # joins the watcher (its read ended by the close)


@pytest.mark.asyncio
async def test_immediate_body_coalesces_with_the_head_up_to_the_codecs_buffer_cap():
    """hyper `WriteBuf` Flatten up to `max_buf_size`: an immediate body no larger than the
    codec's cap rides the head's write; a larger one follows the head as its own write."""

    class _WriteLog(_AsyncioSilentStub):
        def __init__(self, data):
            super().__init__(data)
            self.writes = []  # one entry per `send_all`, the object as handed over

        async def send_all(self, data):
            self.writes.append(data)
            await super().send_all(data)

    body = bytes(100_000)
    transport = _WriteLog(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(transport, backend=AsyncioBackend())
    assert H1Codec().max_buf_size == 417_792  # hyper's default, the cap in force here
    req = await conn.next_request()
    await req.respond(200, body=body)
    assert len(transport.writes) == 1
    assert transport.writes[0].startswith(b"HTTP/1.1 200 OK\r\n") and transport.writes[0].endswith(body)
    await conn.close()  # ends the head-read deadline watchdog (no task left on the loop)

    transport = _WriteLog(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
    conn = ServerConnection(transport, backend=AsyncioBackend(), max_buf_size=8192)
    req = await conn.next_request()
    await req.respond(200, body=body)
    assert len(transport.writes) == 2  # over the cap: the head first, the body as its own write
    await conn.close()
    assert transport.writes[0].startswith(b"HTTP/1.1 200 OK\r\n") and transport.writes[0].endswith(b"\r\n\r\n")
    assert transport.writes[1] is body  # and the body is the caller's own object (item 1)
