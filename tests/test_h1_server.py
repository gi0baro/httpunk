"""HTTP/1 server (`H1Server`) over a tonio loopback, driven by httpunk's own
`H1Connection` client — end-to-end coverage of both sides: request/response,
request + response bodies, keep-alive reuse, chunked responses, headers, and the
auto-`Date` header.
"""

import pytest
from _client import open_h1
from tonio.colored import scope
from tonio.colored.net import open_tcp_listeners

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
    client can't drive: pipelining, HTTP/1.0, upgrades, malformed heads."""
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
            r = await conn.get("/hello", headers={"host": f"{host}:{port}"})
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
            assert await (await conn.get("/a", headers={"host": f"{host}:{port}"})).read() == b"GET /a -> "
            assert await (await conn.get("/b", headers={"host": f"{host}:{port}"})).read() == b"GET /b -> "
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
            r = await conn.get("/stream", headers={"host": f"{host}:{port}"})
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
            r1 = await conn.get("/a", headers={"host": f"{host}:{port}"})
            assert r1.status == 204
            assert await r1.read() == b""
            r2 = await conn.get("/b", headers={"host": f"{host}:{port}"})  # connection reused
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
            r = await conn.get("/", headers={"host": f"{host}:{port}", "x-custom": "abc"})
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
        await transport.send_all(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\nGET /b HTTP/1.1\r\nhost: x\r\n\r\n")
        data = await _read_until(transport, b"GET /b -> ")
        assert b"GET /a -> " in data
        assert b"GET /b -> " in data
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
        await transport.send_all(b"GET /old HTTP/1.0\r\nhost: x\r\n\r\n")
        data = await _drain_all(transport)  # 1.0 default-close → server closes after replying
        assert data.startswith(b"HTTP/1.0 200")
        assert b"GET /old -> " in data
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
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n")  # partial head, never completes
        data = await _drain_all(transport)  # server times out and closes -> EOF
        assert data == b""  # no response sent; the connection was just closed
        s.cancel()


@pytest.mark.tonio
async def test_server_no_100_continue_for_http10():
    """An HTTP/1.0 request never gets an auto 100-continue, even with `Expect:
    100-continue` — hyper gates the interim on version > 1.0 (conn.rs L311). F16."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        transport = await _raw_client(host, port)
        await transport.send_all(b"POST /x HTTP/1.0\r\nhost: x\r\ncontent-length: 3\r\nexpect: 100-continue\r\n\r\nabc")
        data = await _drain_all(transport)  # 1.0 default-close → server closes after replying
        assert b"100 Continue" not in data  # F16: no interim for a 1.0 client
        assert data.startswith(b"HTTP/1.0 200")  # the real response still arrives
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
        await transport.send_all(b"GET /a HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
        r1 = await _read_until(transport, b"/a")  # head + the 2-byte length-framed body
        assert r1.startswith(b"HTTP/1.0 200")
        assert b"connection: keep-alive" in r1.lower()  # reusable, not close-delimited
        await transport.send_all(b"GET /b HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
        assert b"/b" in await _read_until(transport, b"/b")  # connection was reused
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
        await transport.send_all(b"GET /a HTTP/1.0\r\nhost: x\r\nconnection: keep-alive\r\n\r\n")
        data = await _read_until(transport, b"\r\n\r\n")
        assert data.startswith(b"HTTP/1.0 200")
        assert b"connection: keep-alive" in data.lower()
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
        await transport.send_all(b"GET /a HTTP/1.1\r\nhost: x\r\n\r\n")
        data = await _drain_all(transport)  # server closes despite the keep-alive request
        assert b"connection: close" in data.lower()
        assert b"bye" in data
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
        await transport.send_all(b"POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 1000000\r\n\r\npartial")
        data = await _drain_all(transport)  # returns once the server closes (no hang)
        assert b"ok" in data
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
        await transport.send_all(b"GET /chat HTTP/1.1\r\nhost: x\r\nconnection: upgrade\r\nupgrade: myproto\r\n\r\n")
        head = await _read_until(transport, b"\r\n\r\n")
        assert head.startswith(b"HTTP/1.1 101")
        await transport.send_all(b"ping")
        assert await transport.receive_some() == b"echo:ping"
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
            nc = await conn.get("/n", headers={"host": f"{host}:{port}"})  # -> 204
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
