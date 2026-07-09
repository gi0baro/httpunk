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

from httpunk import H1Connection, H2Connection, H2Error
from httpunk._backend.asyncio import AsyncioBackend, _AsyncioStream
from httpunk.h1.server import H1Server
from httpunk.h2.server import H2Server
from httpunk.util import auto, connect
from httpunk.util.graceful import GracefulShutdown


# These use `asyncio.TaskGroup` (3.11+) in the harness; the full stack on the asyncio
# backend is also covered by test_asyncio_protocol.py, which runs on 3.10.
pytestmark = pytest.mark.skipif(sys.version_info < (3, 11), reason="asyncio.TaskGroup is 3.11+")


async def _listen(*, ssl_ctx=None):
    """Listen on loopback; hand back the first accepted connection as our
    `_AsyncioStream` (via a capturing `create_server` factory). Returns
    `(host, port, accept_coro, listener)`. The stream is enqueued from
    `connection_made` — asyncio schedules that AFTER the factory returns, so a
    factory-time enqueue would race the driver's first send (this is also the
    Phase 6b pattern: drive from `connection_made`)."""
    loop = asyncio.get_running_loop()
    incoming = asyncio.Queue()

    class _Captured(_AsyncioStream):
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
async def test_h2_multiplexed_requests():
    backend = AsyncioBackend()
    host, port, serve = await _run_server(_echo_path, lambda s: _h2(s, backend))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve())
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            r1, r2 = await asyncio.gather(conn.request("GET", "/a"), conn.request("GET", "/b"))  # two concurrent streams
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
    ctx = ssl.create_default_context()
    ca.configure_trust(ctx)
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
        conn = await connect(
            f"https://127.0.0.1:{port}/", backend=backend, ssl_context=_client_ctx(ca), alpn=("h2", "http/1.1")
        )
        assert isinstance(conn, H2Connection)  # ALPN chose h2 over TLS
        async with conn:
            resp = await conn.request("GET", "/x")
            assert await resp.read() == b"tls:/x"


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
        with pytest.raises(H2Error):  # new work refused after shutdown
            await conn.request("GET", "/")
        await conn.__aexit__(None, None, None)
