"""Step-3 e2e smokes: the REAL h1/h2/util drivers driven on `AsyncioBackend` over
loopback. The 162 tonio tests are the driver-fidelity gate; these prove the asyncio
backend *conforms* — deliberately covering the teardown/cancel-sensitive paths
(connection close, graceful shutdown, the h1 read-race, TLS/ALPN) where emergent
bugs hide. No harness abstraction — plain asyncio primitives + `backend=`.
"""

import asyncio
import ssl
import sys

import pytest
import trustme

from httpunk import H1Connection, H2Connection, HTTPunkError
from httpunk._backend.asyncio import AsyncioBackend, _AsyncioStream
from httpunk.h1.server import H1Server
from httpunk.h2.server import H2Server
from httpunk.util import auto, connect
from httpunk.util.graceful import GracefulShutdown


# These use `asyncio.TaskGroup` (3.11+) in the harness; the full stack on the asyncio
# backend is also covered by test_asyncio_protocol.py, which runs on 3.10.
pytestmark = pytest.mark.skipif(sys.version_info < (3, 11), reason="asyncio.TaskGroup is 3.11+")


async def _listen(*, ssl_ctx=None, stream_cls=_AsyncioStream):
    """Listen on loopback; hand back the first accepted connection as our
    `_AsyncioStream` (via a capturing `create_server` factory). Returns
    `(host, port, accept_coro, listener)`. The stream is enqueued from
    `connection_made` — asyncio schedules that AFTER the factory returns, so a
    factory-time enqueue would race the driver's first send (this is also the
    Phase 6b pattern: drive from `connection_made`)."""
    loop = asyncio.get_running_loop()
    incoming = asyncio.Queue()

    class _Captured(stream_cls):
        def connection_made(self, transport):
            super().connection_made(transport)
            incoming.put_nowait(self)

    listener = await loop.create_server(_Captured, "127.0.0.1", 0, ssl=ssl_ctx)
    host, port = listener.sockets[0].getsockname()[:2]
    return host, port, incoming.get, listener


async def _run_server(handler, make_server, *, ssl_ctx=None):
    """Accept ONE connection, run `make_server(stream)` over it, calling
    `handler(req)` per request. Returns `(host, port, serve_coro)`."""
    host, port, accept, listener = await _listen(ssl_ctx=ssl_ctx)

    async def serve():
        try:
            server = await make_server(await accept())
            async with server:
                async for req in server:
                    await handler(req)
        finally:
            listener.close()

    return host, port, serve


class _ReadSpyStream(_AsyncioStream):
    """`_AsyncioStream` with a hook on `receive_some` ENTRY: `expect_read()` hands back an
    event set by the next read issued after the call — the deterministic "the watcher's
    read is parked" point (see test_h1_server.py's `_ReadSpy`)."""

    _expected = None

    def expect_read(self):
        self._expected = asyncio.Event()
        return self._expected

    async def receive_some(self, max_bytes=65536):
        expected, self._expected = self._expected, None
        if expected is not None:
            expected.set()
        return await super().receive_some(max_bytes)


async def _echo_path(req):
    await req.read()
    await req.respond(200, body=b"ok:" + req.path.encode())


@pytest.mark.asyncio
async def test_h2_get_roundtrip():
    backend = AsyncioBackend()
    host, port, serve = await _run_server(_echo_path, lambda s: _h2(s, backend))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/x")
            assert await resp.read() == b"ok:/x"


async def _h2(stream, backend):
    return H2Server(stream, backend=backend)


async def _h1(stream, backend):
    return H1Server(stream, backend=backend)


@pytest.mark.asyncio
async def test_h1_get_roundtrip():
    backend = AsyncioBackend()
    host, port, serve = await _run_server(_echo_path, lambda s: _h1(s, backend))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        async with H1Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/y", headers={"host": host})
            assert await resp.read() == b"ok:/y"


@pytest.mark.asyncio
async def test_h1_idle_fin_closes_connection_promptly():
    """The idle watcher on the asyncio backend (a single parked read over
    `_AsyncioStream`): a server FIN on a PARKED keep-alive connection flips
    `closed` promptly with NO send and no error (hyper's clean idle close,
    conn.rs L471-481), and the next send_request raises with `request_unsent`
    (client/conn/http1.rs L247-263)."""
    backend = AsyncioBackend()
    host, port, accept, listener = await _listen()
    r1_read = asyncio.Event()

    async def serve():
        try:
            stream = await accept()
            buf = b""
            while b"\r\n\r\n" not in buf:
                buf += await stream.receive_some(65536)
            await stream.send_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            await r1_read.wait()  # the client consumed r1 — the connection is parked
            stream.close()  # FIN into the idle connection
        finally:
            listener.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        async with H1Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/a", headers={"host": host})
            assert await resp.read() == b"ok"
            r1_read.set()
            for _ in range(400):  # bounded wait — the watcher must see the FIN unprompted
                if conn.closed:
                    break
                await asyncio.sleep(0.005)
            assert conn.closed
            assert conn._conn.error is None  # clean close, not an error
            with pytest.raises(HTTPunkError) as excinfo:
                await conn.request("GET", "/b", headers={"host": host})
            assert getattr(excinfo.value, "request_unsent", False) is True


@pytest.mark.asyncio
async def test_h1_server_close_with_watcher_parked_on_silent_client():
    """The asyncio twin of test_h1_server.py's server-side close: the server exits its
    `async with` while the mid-message watcher (armed by a push response) is parked on a
    silent client. `ServerConnection.close()` closes the transport then joins the
    watcher — the local close must wake the parked `_AsyncioStream` read — and the
    client sees EOF."""
    backend = AsyncioBackend()
    host, port, accept, listener = await _listen(stream_cls=_ReadSpyStream)
    exited = asyncio.Event()

    async def serve():
        try:
            transport = await accept()
            server = H1Server(transport, backend=backend)
            async with server:
                req = await server.accept()
                parked = transport.expect_read()  # the next read issued is the watcher's
                stream = await req.send_response(200)  # arms the watcher
                await stream.send_data(b"tick")
                await parked.wait()
            exited.set()
        finally:
            listener.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        await transport.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        buf = b""
        while b"4\r\ntick\r\n" not in buf:
            buf += await transport.receive_some(65536)
        await asyncio.wait_for(exited.wait(), 5)  # the exit returned: the join ended
        rest = b""
        try:
            while chunk := await transport.receive_some(65536):
                rest += chunk
        except ConnectionError:
            pass  # an abortive close (RST) counts as closed too
        assert rest == b""
        transport.close()


@pytest.mark.asyncio
async def test_h2_multiplexed_requests():
    backend = AsyncioBackend()
    host, port, serve = await _run_server(_echo_path, lambda s: _h2(s, backend))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            r1, r2 = await asyncio.gather(
                conn.request("GET", "/a"), conn.request("GET", "/b")
            )  # two concurrent streams
            b1, b2 = await asyncio.gather(r1.read(), r2.read())
            assert {b1, b2} == {b"ok:/a", b"ok:/b"}


@pytest.mark.asyncio
async def test_h2_streaming_response_body():
    backend = AsyncioBackend()

    async def handler(req):
        await req.read()

        async def chunks():
            for i in range(3):
                yield f"chunk{i}".encode()

        await req.respond(200, body=chunks())

    host, port, serve = await _run_server(handler, lambda s: _h2(s, backend))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/s")
            assert await resp.read() == b"chunk0chunk1chunk2"


# ----- TLS + ALPN (exercises connect_tls end-to-end on asyncio) -----


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


def _server_ctx(ca):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(ctx)
    ctx.set_alpn_protocols(["h2", "http/1.1"])
    return ctx


def _client_ctx(ca):
    # A caller-supplied context is never mutated by `connect()`: it carries its own ALPN offer.
    ctx = ssl.create_default_context()
    ca.configure_trust(ctx)
    ctx.set_alpn_protocols(["h2", "http/1.1"])
    return ctx


@pytest.mark.asyncio
async def test_tls_alpn_negotiates_h2(ca):
    backend = AsyncioBackend()

    async def handler(req):
        await req.read()
        await req.respond(200, body=b"tls:" + req.path.encode())

    host, port, serve = await _run_server(handler, lambda s: auto.serve(s, backend=backend), ssl_ctx=_server_ctx(ca))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        conn = await connect(f"https://127.0.0.1:{port}/", backend=backend, ssl_context=_client_ctx(ca))
        assert isinstance(conn, H2Connection)  # ALPN chose h2 over TLS
        async with conn:
            resp = await conn.request("GET", "/x")
            assert await resp.read() == b"tls:/x"


# ----- TLS over an existing stream: `wrap_tls` on a CONNECT tunnel -----

_CONNECT_OK = b"HTTP/1.1 200 Connection Established\r\n\r\n"


def _h1_ctx(ca):
    ctx = ssl.create_default_context()
    ca.configure_trust(ctx)
    ctx.set_alpn_protocols(["http/1.1"])
    return ctx


async def _read_head(stream):
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = await stream.receive_some(65536)
        if not chunk:
            return None
        buf += chunk
    return buf


async def _answer_connect(stream):
    head = await _read_head(stream)
    assert head is not None and head.startswith(b"CONNECT 127.0.0.1:443 HTTP/1.1\r\n")
    await stream.send_all(_CONNECT_OK)


async def _server_tls(stream, server_ctx):
    """The origin's side of the tunnel: server TLS on the SAME connection the CONNECT
    came on — `start_tls` over the accepted stream's transport, the stream re-pointed at
    the TLS transport (the mirror of what `wrap_tls` does on the client side)."""
    loop = asyncio.get_running_loop()
    tls_transport = await loop.start_tls(stream._transport, stream, server_ctx, server_side=True)
    stream.connection_made(tls_transport)
    return stream


async def _tunnel_origin(accept, listener, server_ctx, backend, errors):
    """A CONNECT proxy and the `https` origin behind it, in one (see the tonio twin in
    test_util_tls.py). Errors are recorded, never swallowed."""
    try:
        stream = await accept()
        await _answer_connect(stream)
        server = await auto.serve(await _server_tls(stream, server_ctx), backend=backend)
        async with server:
            async for req in server:
                body = await req.read()
                await req.respond(200, body=b"tls:" + body)
    except Exception as exc:
        errors.append(exc)
    finally:
        listener.close()


async def _connect_tunnel(backend, host, port, *, ssl_context=None):
    """Dial the proxy (over TLS if `ssl_context`), CONNECT, take the tunnel's IO back out."""
    if ssl_context is not None:
        transport, _ = await backend.connect_tls(host, port, ssl_context=ssl_context)
    else:
        transport = await backend.connect_tcp(host, port)
    async with H1Connection(transport, authority=f"{host}:{port}", backend=backend) as proxy:
        resp = await proxy.request("CONNECT", "127.0.0.1:443", headers={"host": "127.0.0.1:443"})
        assert resp.status == 200 and resp.is_upgrade
        return resp.upgraded.downcast()


@pytest.mark.asyncio
async def test_wrap_tls_tunnel_alpn_h2(ca):
    backend = AsyncioBackend()
    errors = []
    host, port, accept, listener = await _listen()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_tunnel_origin(accept, listener, _server_ctx(ca), backend, errors))
        io, read_buf = await _connect_tunnel(backend, host, port)
        assert read_buf == b""
        stream, selected = await backend.wrap_tls(
            io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca), prefix=read_buf
        )
        assert stream is io  # the same stream object, re-pointed at the TLS transport
        assert selected == "h2"
        async with H2Connection(stream, authority="127.0.0.1:443", scheme="https", backend=backend) as conn:
            resp = await conn.request("POST", "/", body=b"hi")
            assert await resp.read() == b"tls:hi"
    assert not errors


@pytest.mark.asyncio
async def test_wrap_tls_tunnel_h1_keep_alive_and_abort(ca):
    """h1 over the tunnel: keep-alive reuse (the send-time peek before the second request
    runs on the wrapped stream), then the abortive `close_transport` — `abort()` on the
    TLS transport, the path punkreq's own wrapper lacked — ends the origin's loop."""
    backend = AsyncioBackend()
    errors = []
    host, port, accept, listener = await _listen()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_tunnel_origin(accept, listener, _server_ctx(ca), backend, errors))
        io, read_buf = await _connect_tunnel(backend, host, port)
        stream, selected = await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=_h1_ctx(ca))
        assert selected == "http/1.1"
        conn = H1Connection(stream, authority="127.0.0.1:443", backend=backend)
        await conn.__aenter__()
        for body in (b"one", b"two"):
            resp = await conn.request("POST", "/", headers={"host": "127.0.0.1:443"}, body=body)
            assert await resp.read() == b"tls:" + body
        backend.close_transport(stream)  # no close_notify: the origin's read breaks or ends
    assert not errors  # the h1 server maps the broken transport at the head boundary to a clean end (F47)


@pytest.mark.asyncio
async def test_wrap_tls_tunnel_orderly_close_sends_close_notify(ca):
    backend = AsyncioBackend()
    errors = []
    host, port, accept, listener = await _listen()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_tunnel_origin(accept, listener, _server_ctx(ca), backend, errors))
        io, _ = await _connect_tunnel(backend, host, port)
        stream, _ = await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=_h1_ctx(ca))
        async with H1Connection(stream, authority="127.0.0.1:443", backend=backend) as conn:
            resp = await conn.request("GET", "/", headers={"host": "127.0.0.1:443"})
            assert await resp.read() == b"tls:"
    assert not errors


@pytest.mark.asyncio
async def test_wrap_tls_over_tls_https_proxy(ca):
    """A tunnel through an `https://` proxy: TLS to the proxy, CONNECT inside it, then the
    origin's TLS inside the tunnel — TLS over TLS. sslproto stacks and cascades both
    ends, so on asyncio this simply works (tonio refuses it at setup)."""
    backend = AsyncioBackend()
    errors = []
    host, port, accept, listener = await _listen(ssl_ctx=_server_ctx(ca))  # the proxy speaks TLS
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_tunnel_origin(accept, listener, _server_ctx(ca), backend, errors))
        io, read_buf = await _connect_tunnel(backend, host, port, ssl_context=_h1_ctx(ca))
        stream, selected = await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca))
        assert selected == "h2"
        async with H2Connection(stream, authority="127.0.0.1:443", scheme="https", backend=backend) as conn:
            resp = await conn.request("POST", "/", body=b"nested")
            assert await resp.read() == b"tls:nested"
    assert not errors  # the orderly close cascaded through both TLS layers


@pytest.mark.asyncio
async def test_wrap_tls_rejects_bytes_before_the_handshake(ca):
    """asyncio's runtime limitation: bytes the peer sent before the ClientHello — a
    non-empty `prefix`, or bytes the eager drain already buffered on the stream — cannot
    be fed to the SSL layer, and mean the peer is no TLS server anyway: `ssl.SSLError`
    up front, the stream aborted (the proxy's read ends)."""
    backend = AsyncioBackend()
    ended = []
    host, port, accept, listener = await _listen()

    async def proxy():
        try:
            stream = await accept()
            await _answer_connect(stream)
            try:
                ended.append(await stream.receive_some())
            except ConnectionError as exc:
                ended.append(exc)
        finally:
            listener.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(proxy())
        io, read_buf = await _connect_tunnel(backend, host, port)
        assert read_buf == b""
        with pytest.raises(ssl.SSLError, match="before the TLS handshake"):
            await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca), prefix=b"junk")
    assert ended and (ended[0] == b"" or isinstance(ended[0], ConnectionError))


@pytest.mark.asyncio
async def test_wrap_tls_rejects_buffered_bytes(ca):
    """The other way the peer can speak first on asyncio: bytes sitting in the stream's
    own buffer (the eager drain) at wrap time — the same up-front `ssl.SSLError`."""
    backend = AsyncioBackend()
    io = _AsyncioStream()
    io.connection_made(None)
    io.data_received(b"junk")
    with pytest.raises(ssl.SSLError, match="before the TLS handshake"):
        await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca))


@pytest.mark.asyncio
async def test_wrap_tls_dead_stream_fails_fast(ca):
    """A stream whose peer already closed: no handshake can complete, and `start_tls`
    would silently drop the ClientHello and sit until its own timeout — fail up front
    with a `ConnectionError` instead (the F32 rule `send_all` follows)."""
    backend = AsyncioBackend()
    host, port, accept, listener = await _listen()

    async def proxy():
        try:
            stream = await accept()
            await _answer_connect(stream)
            stream.close()  # a FIN right after the 200
        finally:
            listener.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(proxy())
        io, _ = await _connect_tunnel(backend, host, port)
        assert await io.receive_some() == b""  # the EOF has arrived: deterministic, no timing
        with pytest.raises(ConnectionError):
            await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca))


@pytest.mark.asyncio
async def test_wrap_tls_handshake_failure_closes_the_socket(ca):
    """A refused certificate: `ssl.SSLCertVerificationError` out of `wrap_tls`, and the
    socket is already closed by then (sslproto force-closes it): the origin's own
    handshake fails, then its raw read ends — nothing for the caller to tear down."""
    backend = AsyncioBackend()
    ends = []
    host, port, accept, listener = await _listen()

    async def origin():
        try:
            stream = await accept()
            await _answer_connect(stream)
            try:
                await _server_tls(stream, _server_ctx(ca))
            except Exception as exc:
                ends.append(exc)
            try:
                ends.append(await stream.receive_some())
            except Exception as exc:
                ends.append(exc)
        finally:
            listener.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(origin())
        io, _ = await _connect_tunnel(backend, host, port)
        untrusting = ssl.create_default_context()  # no `ca.configure_trust`: verification fails
        with pytest.raises(ssl.SSLCertVerificationError):
            await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=untrusting)
        assert io._transport.is_closing()  # sslproto force-closed the socket: nothing to clean up
    assert len(ends) == 2 and not isinstance(ends[0], bytes)  # the origin's handshake failed, then its read ended


# ----- graceful shutdown (teardown + the h1 read-race via backend.select) -----


@pytest.mark.asyncio
async def test_h1_graceful_releases_idle_connection():
    backend = AsyncioBackend()
    graceful = GracefulShutdown(backend=backend)
    served = asyncio.Event()
    host, port, accept, listener = await _listen()

    async def serve(server):
        async with server:
            async for req in server:
                await req.read()
                await req.respond(200, body=b"ok")
                served.set()

    async with asyncio.TaskGroup() as tg:

        async def server_side():
            server = H1Server(await accept(), backend=backend)
            await graceful.watch(server, serve)
            listener.close()

        tg.create_task(server_side())

        transport = await backend.connect_tcp(host, port)
        conn = H1Connection(transport, authority=f"{host}:{port}", backend=backend)
        await conn.__aenter__()
        resp = await conn.request("GET", "/", headers={"host": host})
        assert await resp.read() == b"ok"
        await served.wait()
        assert graceful.count() == 1
        # The server is now idle, parked in next_request's head-read racing the
        # shutdown event via backend.select; shutdown must release it and close.
        await graceful.shutdown()
        assert graceful.count() == 0
        await conn.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_h2_graceful_drains_and_refuses_new():
    backend = AsyncioBackend()
    graceful = GracefulShutdown(backend=backend)
    host, port, accept, listener = await _listen()

    async def serve(server):
        async with server:
            async for req in server:
                await req.read()
                await req.respond(200, body=b"ok")

    async with asyncio.TaskGroup() as tg:

        async def server_side():
            server = H2Server(await accept(), backend=backend)
            await graceful.watch(server, serve)
            listener.close()

        tg.create_task(server_side())

        transport = await backend.connect_tcp(host, port)
        conn = H2Connection(transport, authority=f"{host}:{port}", backend=backend)
        await conn.__aenter__()
        resp = await conn.request("GET", "/")
        assert await resp.read() == b"ok"
        assert graceful.count() == 1
        await graceful.shutdown()  # GOAWAY + drain (idle) + close
        assert graceful.count() == 0
        with pytest.raises(HTTPunkError):  # new work refused after shutdown (GoAway or ConnClosed)
            await conn.request("GET", "/")
        await conn.__aexit__(None, None, None)
