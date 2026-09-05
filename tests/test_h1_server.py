"""HTTP/1 server (`H1Server`) over a tonio loopback, driven by httpunk's own
`H1Connection` client — end-to-end coverage of both sides: request/response,
request + response bodies, keep-alive reuse, chunked responses, headers, and the
auto-`Date` header.
"""

import pytest
from _client import open_h1
from tonio.colored import Event, scope, sleep
from tonio.colored.net import open_tcp_listeners

from httpunk import Version
from httpunk._backend.asyncio import AsyncioBackend
from httpunk._backend.tonio import TonioBackend
from httpunk.exceptions import ConnectionClosedError
from httpunk.h1 import H1Server
from httpunk.h1.server import ServerConnection
from httpunk.http import HeaderMap


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


async def _raw_client(host, port):
    """A byte-level transport — for wire-fidelity cases the (sequential) httpunk
    client can't drive: pipelining, HTTP/1.0, upgrades, malformed heads.

    Callers must run their test body in `try/finally: transport.close(); s.cancel()`:
    an assertion that fires while the transport is still open leaves the spawned
    server parked in a read only our EOF can end (tonio's optimistic cancellation
    cannot land on its pre-existing waiter), and the scope join then waits it out
    forever — the conftest deadline turns the real failure into an opaque timeout.
    Both calls are idempotent, so a body that already closed/cancelled is fine."""
    return await TonioBackend().connect_tcp(host, port)


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


class _StubTransport:
    """An in-memory transport preloaded with request bytes — drives `Connection`
    directly for cases that don't need a live peer (respond-order, auto-error)."""

    def __init__(self, data):
        self._data = data
        self.sent = b""
        self.closed = False

    async def receive_some(self, max_bytes=65536):
        chunk, self._data = self._data[:max_bytes], self._data[max_bytes:]
        return chunk

    async def send_all(self, data):
        self.sent += bytes(data)

    def close(self):
        self.closed = True


class _PeekableStub(_StubTransport):
    """A stub whose SECOND payload becomes readable only after the first is consumed —
    models a pipelined request sitting buffered in the transport while the previous
    request is being served."""

    def __init__(self, first, buffered):
        super().__init__(first)
        self._buffered = buffered

    async def receive_some(self, max_bytes=65536):
        if self._data:
            return await super().receive_some(max_bytes)
        chunk, self._buffered = self._buffered[:max_bytes], self._buffered[max_bytes:]
        return chunk


async def _echo_server(listener, seen=None):
    """Accept one connection; echo each request as 200 `b"<METHOD> <target> -> " + body`."""
    transport = await listener.accept()
    async with H1Server(transport) as server:
        async for req in server:
            if seen is not None:
                seen.append(req.target)
            body = await req.read()
            reply = f"{req.method} {req.target} -> ".encode() + body
            await req.respond(200, headers={"content-type": "text/plain"}, body=reply)


@pytest.mark.tonio
async def test_server_get():
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/hello", headers={"host": f"{host}:{port}"})
            assert r.status == 200
            assert r.headers["content-type"] == b"text/plain"
            assert r.headers.get("date") is not None  # server auto-adds Date (hyper parity)
            assert await r.read() == b"GET /hello -> "
        s.cancel()


@pytest.mark.tonio
async def test_server_post_echo_body():
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        async with open_h1(host, port) as conn:
            r = await conn.request("POST", "/submit", headers={"host": f"{host}:{port}"}, body=b"payload!")
            assert r.status == 200
            assert await r.read() == b"POST /submit -> payload!"
        s.cancel()


@pytest.mark.tonio
async def test_server_keep_alive_two_requests():
    listener, host, port = await _listener()
    seen = []
    async with scope() as s:
        s.spawn(_echo_server(listener, seen))
        async with open_h1(host, port) as conn:
            assert await (await conn.request("GET", "/a", headers={"host": f"{host}:{port}"})).read() == b"GET /a -> "
            assert await (await conn.request("GET", "/b", headers={"host": f"{host}:{port}"})).read() == b"GET /b -> "
        s.cancel()
    assert seen == ["/a", "/b"]  # both served on the one (reused) connection


@pytest.mark.tonio
async def test_server_chunked_response():
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()

                async def chunks():
                    yield b"chunk-one "
                    yield b"chunk-two"

                await req.respond(200, body=chunks())  # iterable body -> chunked

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/stream", headers={"host": f"{host}:{port}"})
            assert await r.read() == b"chunk-one chunk-two"
        s.cancel()


@pytest.mark.tonio
async def test_server_bodyless_204_then_reuse():
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                await req.respond(204)  # bodyless

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r1 = await conn.request("GET", "/a", headers={"host": f"{host}:{port}"})
            assert r1.status == 204
            assert await r1.read() == b""
            r2 = await conn.request("GET", "/b", headers={"host": f"{host}:{port}"})  # connection reused
            assert r2.status == 204
        s.cancel()


@pytest.mark.tonio
async def test_server_drains_unread_request_body_before_next():
    """If the app responds without reading the request body, the server drains it
    so the next request on the (keep-alive) connection still parses."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_drainer(listener))
        async with open_h1(host, port) as conn:
            r1 = await conn.request("POST", "/a", headers={"host": f"{host}:{port}"}, body=b"unread body")
            assert await r1.read() == b"ok"
            r2 = await conn.request("POST", "/b", headers={"host": f"{host}:{port}"}, body=b"also unread")
            assert await r2.read() == b"ok"
        s.cancel()


async def _drainer(listener):
    transport = await listener.accept()
    async with H1Server(transport) as server:
        async for req in server:
            await req.respond(200, body=b"ok")  # deliberately does NOT read the request body


@pytest.mark.tonio
async def test_server_request_headers():
    listener, host, port = await _listener()
    seen = {}

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                seen["host"] = req.headers.get("host")
                seen["x-custom"] = req.headers.get("x-custom")
                await req.read()
                await req.respond(200, body=b"")

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/", headers={"host": f"{host}:{port}", "x-custom": "abc"})
            assert r.status == 200
            await r.read()
        s.cancel()

    assert seen["host"] == f"{host}:{port}".encode()
    assert seen["x-custom"] == b"abc"


@pytest.mark.tonio
async def test_server_pipelined_requests():
    """Two requests sent in ONE write must both be served — the second sits in the
    buffer past the first's head and must not be dropped (else the wire deadlocks)."""
    listener, host, port = await _listener()
    seen = []
    async with scope() as s:
        s.spawn(_echo_server(listener, seen))
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n\r\n")
            data = await _read_until(transport, b"GET /b -> ")
            assert b"GET /a -> " in data
            assert b"GET /b -> " in data
        finally:
            transport.close()
            s.cancel()
    assert seen == ["/a", "/b"]


@pytest.mark.tonio
async def test_server_http10_response_version_and_close():
    """An HTTP/1.0 request gets an `HTTP/1.0` status line and (no keep-alive) the
    server closes the connection."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /old HTTP/1.0\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)  # 1.0 default-close → server closes after replying
            assert data.startswith(b"HTTP/1.0 200")
            assert b"GET /old -> " in data
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_request_exposes_http_version():
    """`ServerRequest.version` is public — the request's version is part of the message
    (hyper `Request::version()`): HTTP_10 for an HTTP/1.0 request, HTTP_11 for 1.1."""
    listener, host, port = await _listener()
    seen = []

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                seen.append((req.target, req.version))
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /eleven HTTP/1.1\r\nhost: x\r\n\r\nGET /ten HTTP/1.0\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)  # the 1.0 request (no keep-alive) ends the connection
            assert data.startswith(b"HTTP/1.1 200")
            assert b"HTTP/1.0 200" in data
            assert seen == [("/eleven", Version.HTTP_11), ("/ten", Version.HTTP_10)]
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_header_read_timeout_closes_slow_head():
    """A request head that never completes is closed after `header_read_timeout`
    (slowloris defence, hyper http1.rs L249): no response, just a close (F30)."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport, header_read_timeout=0.1) as server:
            async for req in server:
                await req.respond(200)

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n")  # partial head, never completes
            data = await _drain_all(transport)  # server times out and closes -> EOF
            assert data == b""  # no response sent; the connection was just closed
        finally:
            transport.close()
            s.cancel()


@pytest.mark.asyncio
async def test_server_shutdown_wins_over_buffered_pipelined_request():
    """REGRESSION GUARD: a graceful-shutdown signal is honored BEFORE any idle read consumes
    already-buffered bytes — hyper stops parsing new heads once keep-alive is disabled
    (`can_read_head` under KA::Disabled), so a pipelined request already sitting in the
    transport must NOT be served. Whitebox: the shutdown event is set directly (without
    `graceful_shutdown()`'s `_reusable = False`, which would end the accept loop before the
    read even starts) to pin the in-loop ordering: shutdown check -> read."""
    transport = _PeekableStub(
        b"GET /a HTTP/1.1\r\nhost: x\r\n\r\n",
        b"GET /b HTTP/1.1\r\nhost: x\r\n\r\n",
    )
    conn = ServerConnection(transport, backend=AsyncioBackend())
    req = await conn.next_request()
    assert req.target == "/a"
    await req.respond(200)
    conn._shutdown_evt.set()  # signal only (whitebox) — see docstring
    assert await conn.next_request() is None  # /b is readable but must NOT be parsed


@pytest.mark.tonio
async def test_server_detach_hands_off_open_connection():
    """detach() stops the accept loop and relinquishes the transport WITHOUT closing it,
    returning bytes read past the request head — so a caller can take over the raw connection
    for a protocol upgrade (e.g. WebSocket)."""
    listener, host, port = await _listener()
    captured = {}

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                captured["leftover"] = req.detach()  # loop ends next (detached), socket stays open
        # `__aexit__` ran (close() is a no-op after detach) — the transport is still usable, so
        # the caller drives its own upgrade response on the raw connection.
        await transport.send_all(b"HTTP/1.1 101 Switching Protocols\r\nupgrade: custom\r\n\r\nOWNED")

    async with scope() as s:
        s.spawn(serve())
        client = await _raw_client(host, port)
        try:
            # request head + trailing bytes the client sent past it (must survive as `leftover`)
            await client.send_all(b"GET /up HTTP/1.1\r\nhost: x\r\nupgrade: custom\r\nconnection: upgrade\r\n\r\nEXTRA")
            data = await _read_until(client, b"OWNED")
        finally:
            client.close()
            s.cancel()

    assert captured["leftover"] == b"EXTRA"  # bytes past the head handed back
    assert b"101 Switching Protocols" in data and data.endswith(b"OWNED")  # socket stayed open


@pytest.mark.tonio
async def test_server_no_100_continue_for_http10():
    """An HTTP/1.0 request never gets an auto 100-continue, even with `Expect:
    100-continue` — hyper gates the interim on version > 1.0 (conn.rs L311). F16."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(
                b"POST /x HTTP/1.0\r\nhost: x\r\ncontent-length: 3\r\nexpect: 100-continue\r\n\r\nabc"
            )
            data = await _drain_all(transport)  # 1.0 default-close → server closes after replying
            assert b"100 Continue" not in data  # F16: no interim for a 1.0 client
            assert data.startswith(b"HTTP/1.0 200")  # the real response still arrives
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_http10_streamed_body_with_content_length_reuses():
    """A streamed (iterable) HTTP/1.0 response WITH a Content-Length is length-framed,
    not close-delimited, so the connection stays reusable and keeps the keep-alive
    header (F27) — deciding close-delimited from the body shape alone forced a close."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                body = (part for part in [req.target.encode()])  # an iterable body
                await req.respond(200, headers={"content-length": str(len(req.target))}, body=body)

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /a HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
            r1 = await _read_until(transport, b"/a")  # head + the 2-byte length-framed body
            assert r1.startswith(b"HTTP/1.0 200")
            assert b"connection: keep-alive" in r1.lower()  # reusable, not close-delimited
            await transport.send_all(b"GET /b HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
            assert b"/b" in await _read_until(transport, b"/b")  # connection was reused
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_http10_keep_alive_header():
    """HTTP/1.0 + `Connection: keep-alive` → the response must echo `Connection:
    keep-alive` (hyper fix_keep_alive) so the 1.0 client keeps the connection."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /a HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
            data = await _read_until(transport, b"\r\n\r\n")
            assert data.startswith(b"HTTP/1.0 200")
            assert b"connection: keep-alive" in data.lower()
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_response_connection_close():
    """A response `Connection: close` (on a keep-alive request) closes the
    connection and appears on the wire."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                await req.respond(200, headers={"connection": "close"}, body=b"bye")

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)  # server closes despite the keep-alive request
            assert b"connection: close" in data.lower()
            assert b"bye" in data
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_unread_large_body_closes_not_drains():
    """If the app responds without reading a large request body, the server does
    ONE non-blocking read and — since the whole body isn't sitting in the buffer —
    closes rather than streaming it off the socket (1:1 hyper
    poll_drain_or_close_read). It must not block (a hang would trip the 6s net)."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.respond(200, body=b"ok")  # does NOT read the (huge) body

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 1000000\r\n\r\npartial")
            data = await _drain_all(transport)  # returns once the server closes (no hang)
            assert b"ok" in data
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_upgrade_tunnel():
    """A 101 response hands the raw connection to `req.upgraded`; the driver
    detaches and the app drives the tunnel directly."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.respond(101, headers={"upgrade": "myproto", "connection": "upgrade"})
                tunnel = req.upgraded
                assert tunnel is not None
                data = await tunnel.receive_some()
                await tunnel.send_all(b"echo:" + data)
                await tunnel.aclose()
                break

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(
                b"GET /chat HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: myproto\r\n\r\n"
            )
            head = await _read_until(transport, b"\r\n\r\n")
            assert head.startswith(b"HTTP/1.1 101")
            await transport.send_all(b"ping")
            assert await transport.receive_some() == b"echo:ping"
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_bodyless_response_does_not_drain_body():
    """A body handed to a bodyless response (HEAD / 204) is never polled — no bytes
    on the wire AND the iterable's side effects don't fire (G37, hyper write_head's
    encoder.is_eof() gate)."""
    listener, host, port = await _listener()
    fired = []

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()

                async def body():
                    fired.append(1)  # side effect — must NOT run
                    yield b"should-not-be-sent"

                status = 204 if req.method == "GET" else 200
                await req.respond(status, body=body())

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            head = await conn.request("HEAD", "/h", headers={"host": f"{host}:{port}"})
            assert head.status == 200
            assert await head.read() == b""  # HEAD response carries no body
            nc = await conn.request("GET", "/n", headers={"host": f"{host}:{port}"})  # -> 204
            assert nc.status == 204
            assert await nc.read() == b""
        s.cancel()

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
    assert b"connection: close" in stub.sent.lower()  # F29: enforce_version adds it before closing
    assert stub.closed


@pytest.mark.tonio
async def test_server_oversized_head_rejected_with_431():
    """A request head that never completes and grows past hyper's max_buf_size is
    rejected as `Parse::TooLarge` → auto 431 + close, not buffered without bound (F14)."""
    oversized = b"GET / HTTP/1.1\r\nx: " + b"a" * 500_000  # > _MAX_HEAD_SIZE, no CRLFCRLF
    stub = _StubTransport(oversized)
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    assert req is None
    assert stub.sent.startswith(b"HTTP/1.1 431")  # Request Header Fields Too Large
    assert b"connection: close" in stub.sent.lower()
    assert stub.closed


# ----- hyper `http1::Builder` options (FR-5 parity pass) -----


@pytest.mark.tonio
async def test_server_keep_alive_false_answers_one_request_with_close():
    """`keep_alive=False` = hyper `http1::Builder::keep_alive(false)` -> `disable_keep_alive`
    on the still-Busy connection (`KA::Disabled`): the first response is `Connection: close`
    and the connection closes after it; a pipelined second request is never answered."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport, keep_alive=False) as server:
            async for req in server:
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)  # the server closes after the first response
            assert data.count(b"HTTP/1.1 200") == 1
            assert b"connection: close" in data.lower()
        finally:
            transport.close()
            s.cancel()


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


# ----- push-style responses: `send_response` -> `SendStream` (FR-3) -----


@pytest.mark.tonio
async def test_server_push_response_streams_chunked_and_keeps_alive():
    """`send_response` + `send_data` writes a chunked body (unknown length, 1.1) with the
    same head negotiation as `respond()`: Date added, keep-alive kept, the connection
    reused for the next request."""
    listener, host, port = await _listener()
    seen = []

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                seen.append(req.target)
                stream = await req.send_response(200, headers={"content-type": "text/plain"})
                await stream.send_data(b"one ")
                await stream.send_data(b"two", end_stream=True)

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            for path in ("/a", "/b"):
                r = await conn.request("GET", path, headers={"host": "x"})
                assert r.status == 200
                assert r.headers["transfer-encoding"] == b"chunked"
                assert r.headers.get("date") is not None
                assert await r.read() == b"one two"
        s.cancel()
    assert seen == ["/a", "/b"]  # both on the one (kept-alive) connection


@pytest.mark.tonio
async def test_server_push_content_length_frames_by_length_not_chunked():
    """An app-supplied `content-length` on a push response frames the body by length
    (hyper `set_length` -> `existing_con_len`), never chunked."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200, headers={"content-length": "8"})
                await stream.send_data(b"abcd")
                await stream.send_data(b"efgh", end_stream=True)

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/", headers={"host": "x"})
            assert r.headers.get("transfer-encoding") is None
            assert r.headers["content-length"] == b"8"
            assert await r.read() == b"abcdefgh"
        s.cancel()


@pytest.mark.tonio
async def test_server_push_bodyless_end_stream_and_head_discards_chunks():
    """`send_response(..., end_stream=True)` is a bodyless response (then `send_data`
    is a misuse). For HEAD the encoder is EOF: hyper never polls the body, so pushed
    chunks are DISCARDED, and the app's `content-length` still goes on the wire."""
    listener, host, port = await _listener()
    misuse = []

    async def serve():
        transport = await listener.accept()
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

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
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
        finally:
            transport.close()
            s.cancel()
    assert misuse == ["response body already complete"]


@pytest.mark.tonio
async def test_server_push_send_reset_truncates_body_and_closes():
    """h1 has no reset frame: `send_reset` closes the connection mid-body (hyper's
    behaviour when a response `Body` errors), so the client sees a truncated chunked
    body — no `0\\r\\n\\r\\n` terminator."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                await stream.send_data(b"partial")
                await stream.send_reset()

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)  # returns only because the server closed
            assert data.startswith(b"HTTP/1.1 200")
            assert b"7\r\npartial\r\n" in data
            assert not data.endswith(b"0\r\n\r\n")
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_push_http10_without_content_length_is_close_delimited():
    """HTTP/1.0 + unknown length = close-delimited (hyper role.rs L907-910): raw body
    bytes, no chunking, and the connection closes after the body."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                await stream.send_data(b"old ")
                await stream.send_data(b"school", end_stream=True)

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET / HTTP/1.0\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)
            assert data.startswith(b"HTTP/1.0 200")
            assert b"chunked" not in data.lower()
            assert data.endswith(b"\r\n\r\nold school")
        finally:
            transport.close()
            s.cancel()


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


# ----- response trailers (FR-6) -----


@pytest.mark.tonio
async def test_server_respond_trailers_force_chunked_and_declare_trailer_header():
    """`respond(trailers=)` mirrors the client's `Request.trailers` (F45): even a `bytes`
    body is framed chunked, a `Trailer` header declares the fields, and the trailers
    arrive after the body. Same for a streamed body; an app-set `Trailer` header is kept."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
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

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/bytes", headers={"host": "x"})
            assert r.headers["transfer-encoding"] == b"chunked"  # forced chunked for a bytes body
            assert r.headers["trailer"] == b"x-checksum"  # declared for the app
            assert await r.read() == b"payload"
            assert r.trailers["x-checksum"] == b"abc"
            r = await conn.request("GET", "/stream", headers={"host": "x"})
            assert r.headers["trailer"] == b"x-checksum"
            assert await r.read() == b"payload"
            assert r.trailers["x-checksum"] == b"def"
        s.cancel()


@pytest.mark.tonio
async def test_server_trailers_undeclared_fields_are_dropped_by_encoder():
    """hyper `Encoder::encode_trailers`: only fields named in the response's `Trailer`
    header are emitted (others are dropped). Through the push handle the app owns the
    `Trailer` header, so an undeclared field never reaches the wire."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200, headers={"trailer": "x-declared"})
                await stream.send_data(b"body")
                await stream.send_trailers({"x-declared": "yes", "x-undeclared": "no"})

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/", headers={"host": "x"})
            assert await r.read() == b"body"
            assert r.trailers["x-declared"] == b"yes"
            assert r.trailers.get("x-undeclared") is None
        s.cancel()


@pytest.mark.tonio
async def test_server_respond_trailers_dropped_on_http10():
    """HTTP/1.0 has no chunked framing: the body is close-delimited and the trailers are
    dropped (hyper's close-delimited encoder emits none), not written as garbage."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.respond(200, body=b"old", trailers={"x-checksum": "abc"})

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET / HTTP/1.0\r\nhost: x\r\n\r\n")
            data = await _drain_all(transport)
            assert data.startswith(b"HTTP/1.0 200")
            assert data.endswith(b"\r\n\r\nold")
            assert b"x-checksum: abc" not in data
        finally:
            transport.close()
            s.cancel()


# ----- mid-message peer EOF (FR-2; hyper conn.rs `mid_message_detect_eof`) -----


@pytest.mark.tonio
async def test_server_peer_closed_resolves_and_fails_the_late_response():
    """A client that sends a GET then closes is detected WHILE the handler still runs:
    `peer_closed()` resolves, and the late `respond()` fails with `ConnectionClosedError`
    (hyper: `close_read` + `IncompleteMessage`, the service future is dropped). The accept
    loop then ends."""
    listener, host, port = await _listener()
    seen, done = [], Event()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                await req.peer_closed()
                seen.append("peer_closed")
                try:
                    await req.respond(200, body=b"too late")
                except ConnectionClosedError:
                    seen.append("respond failed")
            seen.append("loop ended")
            done.set()

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        transport.close()  # FIN while the handler is parked
        await done.wait()
        s.cancel()
    assert seen == ["peer_closed", "respond failed", "loop ended"]


@pytest.mark.tonio
async def test_server_streamed_response_fails_fast_on_peer_close():
    """hyper polls the connection (so `mid_message_detect_eof`) while the response body's
    next chunk is pending: a client FIN fails `respond()` at once — the app is not left
    parked on its own `__anext__` forever."""
    listener, host, port = await _listener()
    seen, done = [], Event()

    async def serve():
        transport = await listener.accept()
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
                except ConnectionClosedError:
                    seen.append("failed")
                done.set()

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        data = await _read_until(transport, b"5\r\nfirst\r\n")  # streamed chunks are not held back
        assert data.startswith(b"HTTP/1.1 200")
        transport.close()
        await done.wait()  # would hit the conftest deadline if the sender stayed parked
        s.cancel()
    assert seen == ["producer unwound", "failed"]  # cancelled + unwound BEFORE respond() raised


@pytest.mark.tonio
async def test_server_push_send_data_fails_after_peer_close():
    """Through the push handle: once the client closed its side, the next `send_data`
    fails with `ConnectionClosedError` instead of writing into a dead exchange."""
    listener, host, port = await _listener()
    seen, done = [], Event()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                await stream.send_data(b"tick")
                await req.peer_closed()
                try:
                    await stream.send_data(b"tock")
                except ConnectionClosedError:
                    seen.append("failed")
                done.set()

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        await _read_until(transport, b"4\r\ntick\r\n")
        transport.close()
        await done.wait()
        s.cancel()
    assert seen == ["failed"]


@pytest.mark.tonio
async def test_server_pipelined_request_read_mid_message_is_served_not_lost():
    """Bytes arriving mid-message are the next pipelined request (hyper keeps them in
    `read_buf`): the watcher's read must be handed to the next head read, never dropped,
    and must NOT be mistaken for a peer close."""
    listener, host, port = await _listener()
    release, seen = Event(), []

    async def serve():
        transport = await listener.accept()
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

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
            await sleep(0.05)  # let the watcher park, then pipeline the next request into it
            await transport.send_all(b"GET /next HTTP/1.1\r\nhost: x\r\n\r\n")
            await sleep(0.05)
            release.set()
            data = await _read_until(transport, b"/next")
            assert data.count(b"HTTP/1.1 200") == 2
            assert seen == ["/slow", "/next"]
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_half_close_option_ignores_mid_request_fin():
    """`half_close=True` (hyper `http1::Builder::half_close`): a client FIN mid-request is
    not a disconnect — no watcher, `peer_closed()` stays pending, the response completes
    and is delivered; the connection then ends at the idle read."""
    listener, host, port = await _listener()
    seen, done = [], Event()

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport, half_close=True) as server:
            async for req in server:
                await sleep(0.05)  # the FIN lands here
                seen.append(req._peer_closed)
                await req.respond(200, body=b"still served")
            done.set()

    async with scope() as s:
        s.spawn(serve())
        transport = await _raw_client(host, port)
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        transport.socket.shutdown(1)  # SHUT_WR: half-close, we still read
        data = await _drain_all(transport)
        transport.close()
        await done.wait()
        s.cancel()
    assert seen == [False]
    assert data.startswith(b"HTTP/1.1 200") and data.endswith(b"still served")


@pytest.mark.tonio
async def test_server_detach_refuses_while_watcher_parked_but_allows_upgrade_requests():
    """A request without `Upgrade` has a mid-message read parked, which cannot be handed
    to a caller (one reader per transport) -> `detach()` refuses. An Upgrade request is
    never watched (a task cannot stand in for hyper's poll there) -> `detach()` works."""
    listener, host, port = await _listener()
    seen = []

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                server._conn._arm_watcher(req)  # what `peer_closed()` / a streamed or push response does
                try:
                    req.detach()
                except RuntimeError as exc:
                    seen.append(str(exc))
                await req.respond(200, body=b"not detached")

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/plain", headers={"host": "x"})  # no Upgrade -> watchable
            assert await r.read() == b"not detached"
        s.cancel()
    assert len(seen) == 1 and "mid-message read is parked" in seen[0]

    # An Upgrade request is never watched, so `detach()` hands the transport over cleanly.
    stub = _StubTransport(b"GET /ws HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: websocket\r\n\r\nWSDATA")
    conn = ServerConnection(stub)
    await conn.start()
    req = await conn.next_request()
    assert conn._watcher_handle is None  # upgrade requests are not watched
    assert req.detach() == b"WSDATA"


@pytest.mark.tonio
async def test_h2_preface_closes_silently_without_response():
    """An h1 server that receives the HTTP/2 prior-knowledge preface closes silently
    (a version error) instead of writing a 400 — hyper `on_parse_error`/`has_h2_prefix`
    (conn.rs L809-812) (F49)."""
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
    ConnectionClosedError, not an AttributeError on `None.receive_some`/`send_all` (F59)."""
    conn = ServerConnection(_StubTransport(b""))
    await conn.close()  # nulls the transport
    with pytest.raises(ConnectionClosedError):
        await conn.read_body_more()
    with pytest.raises(ConnectionClosedError):
        await conn.write(b"data")


def test_negotiate_connection_header_replaces_not_appends():
    """The wire `Connection` header REPLACES a user-set value (hyper `insert`), never
    appending a second/contradictory token (F48)."""
    closing = HeaderMap()
    closing.add("connection", "keep-alive")  # user asked keep-alive, but we must close
    out = ServerConnection._negotiate_connection_header(closing, keep_alive=False, http10=False, resp_close=False)
    assert out.get_all("connection") == [b"close"]  # replaced, not [keep-alive, close]

    keeping = HeaderMap()
    keeping.add("connection", "x-foo")  # a custom token on a 1.0 keep-alive response
    out2 = ServerConnection._negotiate_connection_header(keeping, keep_alive=True, http10=True, resp_close=False)
    assert out2.get_all("connection") == [b"keep-alive"]  # replaced x-foo


@pytest.mark.tonio
async def test_h1_request_trailers_round_trip():
    """Request trailers (F45): the client sends them as chunked trailers after the body
    (forcing chunked framing + a `Trailer` header); the H1Server decodes them into
    `req.trailers`."""
    listener, host, port = await _listener()
    seen = {}

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            resp = await conn.request(
                "POST", "/", headers={"host": f"{host}:{port}"}, body=b"data", trailers={"x-checksum": "abc"}
            )
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["body"] == b"data"
    assert seen["trailers"] is not None
    assert seen["trailers"].get("x-checksum") == b"abc"


@pytest.mark.tonio
async def test_h1_bodyless_request_with_trailers():
    """A request with trailers but no body still sends a (chunked, empty) body + trailer
    block rather than a bodyless framing (F45)."""
    listener, host, port = await _listener()
    seen = {}

    async def serve():
        transport = await listener.accept()
        async with H1Server(transport) as server:
            async for req in server:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            resp = await conn.request("POST", "/", headers={"host": f"{host}:{port}"}, trailers={"x-done": "1"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["body"] == b""
    assert seen["trailers"].get("x-done") == b"1"


@pytest.mark.tonio
async def test_abrupt_client_close_ends_iteration_cleanly():
    """A client that tears the connection down abruptly between requests — an
    RST instead of a FIN (SO_LINGER 0; also what an abortive TLS close looks
    like) — must end the server's `async for` cleanly, exactly like the clean
    EOF (F47: the wire outcome is identical — the connection just ends with no
    request to serve). The backend's broken-transport error must not escape
    `next_request` and kill the accept loop."""
    import socket as pysock
    import struct

    listener, host, port = await _listener()
    server_errors, served = [], []

    async def server():
        try:
            await _echo_server(listener, seen=served)
        except Exception as exc:
            server_errors.append(exc)

    async with scope() as s:
        s.spawn(server())
        transport = await _raw_client(host, port)
        try:
            await transport.send_all(b"GET /one HTTP/1.1\r\nhost: x\r\n\r\n")
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = await transport.receive_some(65536)
                assert chunk, "connection closed before the response head"
                resp += chunk
            # RST the connection while the server is parked in the next head-read.
            transport.socket._sock.setsockopt(pysock.SOL_SOCKET, pysock.SO_LINGER, struct.pack("ii", 1, 0))
        finally:
            # No s.cancel(): the scope join waiting out the server task — which must
            # finish CLEANLY on its own — is the assertion under test.
            transport.close()
    assert served == ["/one"]
    assert not server_errors
