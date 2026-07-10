"""Phase 6b: the reusable asyncio server protocols (`httpunk.asyncio.{H1,H2,Auto}
Protocol`). A host embeds httpunk by `loop.create_server(MyProtocol)` and implements
`handle(request)`; the protocol runs the real httpunk server driver over itself.
Here the "host" is the test loop and the client is httpunk's own client on the
asyncio backend.
"""

import asyncio

import pytest

from httpunk import H1Connection, H2Connection, H2Error, HTTPunkError
from httpunk._backend.asyncio import AsyncioBackend
from httpunk.asyncio import AutoServerProtocol, H1ServerProtocol, H2ServerProtocol, ServerConnections


class _EchoH2(H2ServerProtocol):
    async def handle(self, request):
        body = await request.read()
        await request.respond(200, body=b"h2:" + request.path.encode() + b":" + body)


class _EchoH1(H1ServerProtocol):
    async def handle(self, request):
        body = await request.read()
        await request.respond(200, body=b"h1:" + body)


class _EchoAuto(AutoServerProtocol):
    async def handle(self, request):
        await request.read()
        await request.respond(200, body=b"auto:" + (request.path or request.target).encode())


async def _serve(protocol_cls):
    """`create_server(protocol_cls)` on loopback, capturing the per-connection
    protocol instances so a test can await their serve task (a host would track
    these; here we just join for a clean teardown). Returns `(server, host, port,
    protocols)`."""
    loop = asyncio.get_running_loop()
    protocols = []

    def factory():
        p = protocol_cls()
        protocols.append(p)
        return p

    server = await loop.create_server(factory, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    return server, host, port, protocols


@pytest.mark.asyncio
async def test_h2_protocol_roundtrip():
    server, host, port, protocols = await _serve(_EchoH2)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("POST", "/x", body=b"hi")
            assert await resp.read() == b"h2:/x:hi"
        await protocols[0]._serve_task  # join the connection's serve loop (clean teardown)


@pytest.mark.asyncio
async def test_h2_protocol_handles_multiplexed_concurrently():
    server, host, port, protocols = await _serve(_EchoH2)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            r1, r2 = await asyncio.gather(conn.request("GET", "/a"), conn.request("GET", "/b"))
            b1, b2 = await asyncio.gather(r1.read(), r2.read())
            assert {b1, b2} == {b"h2:/a:", b"h2:/b:"}
        await protocols[0]._serve_task


class _BoomH2(H2ServerProtocol):
    async def handle(self, request):
        await request.read()
        if request.path == "/boom":
            raise RuntimeError("handler failed")  # never responds
        await request.respond(200, body=b"ok")


@pytest.mark.asyncio
async def test_h2_protocol_handler_error_resets_stream_not_connection():
    """An h2 handler that raises resets ITS stream (the client sees an error) instead of
    the exception vanishing silently and hanging the client (F34); the connection and
    its other streams keep running."""
    server, host, port, protocols = await _serve(_BoomH2)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            with pytest.raises(H2Error):  # the failed handler reset the stream
                await conn.request("GET", "/boom")
            assert await (await conn.request("GET", "/ok")).read() == b"ok"  # connection survived
        await protocols[0]._serve_task


@pytest.mark.asyncio
async def test_h1_protocol_roundtrip():
    server, host, port, protocols = await _serve(_EchoH1)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H1Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("POST", "/y", headers={"host": host}, body=b"hey")
            assert await resp.read() == b"h1:hey"
        await protocols[0]._serve_task


@pytest.mark.asyncio
async def test_auto_protocol_serves_h2_client():
    server, host, port, protocols = await _serve(_EchoAuto)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H2Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/h2path")
            assert await resp.read() == b"auto:/h2path"
        await protocols[0]._serve_task


@pytest.mark.asyncio
async def test_auto_protocol_serves_h1_client():
    server, host, port, protocols = await _serve(_EchoAuto)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H1Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/h1path", headers={"host": host})
            assert await resp.read() == b"auto:/h1path"
        await protocols[0]._serve_task


# ----- host-coordinated graceful shutdown -----


@pytest.mark.asyncio
async def test_h2_protocol_graceful_shutdown():
    # A host tracks its protocols (here, `protocols`), signals graceful shutdown on
    # each, and awaits wait_closed(). In-flight work finishes; new work is refused.
    server, host, port, protocols = await _serve(_EchoH2)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        conn = H2Connection(transport, authority=f"{host}:{port}", backend=backend)
        await conn.__aenter__()
        resp = await conn.request("GET", "/")
        assert await resp.read() == b"h2:/:"  # full round-trip; connection open

        proto = protocols[0]
        await proto.graceful_shutdown()  # GOAWAY + refuse-new
        await proto.wait_closed()  # drains (idle) + closes -> resolves
        with pytest.raises(HTTPunkError):  # new work refused after the GOAWAY (GoAway or ConnClosed)
            await conn.request("GET", "/again")
        await conn.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_h1_protocol_graceful_shutdown_releases_idle():
    # h1 graceful releases the idle head-read (via backend.select) and closes; the
    # protocol's wait_closed() resolves once the connection has drained.
    server, host, port, protocols = await _serve(_EchoH1)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        async with H1Connection(transport, authority=f"{host}:{port}", backend=backend) as conn:
            resp = await conn.request("GET", "/", headers={"host": host})
            assert await resp.read() == b"h1:"
            proto = protocols[0]
            await proto.graceful_shutdown()
            await proto.wait_closed()  # idle read released, connection closed -> resolves


# ----- ServerConnections registry (host-facing shutdown convenience) -----


@pytest.mark.asyncio
async def test_server_connections_tracks_and_gracefully_shuts_down():
    conns = ServerConnections()
    loop = asyncio.get_running_loop()
    server = await loop.create_server(conns.track(_EchoH2), "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        conn = H2Connection(transport, authority=f"{host}:{port}", backend=backend)
        await conn.__aenter__()
        assert await (await conn.request("GET", "/r")).read() == b"h2:/r:"
        assert conns.count() == 1  # the live connection is tracked
        await conns.shutdown(timeout=5)  # graceful; the idle connection drains + closes
        assert conns.count() == 0  # deregistered when its serve task finished
        with pytest.raises(HTTPunkError):
            await conn.request("GET", "/again")  # refused after the GOAWAY (GoAway or ConnClosed)
        await conn.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_server_connections_shutdown_force_closes_past_timeout():
    hang = asyncio.Event()  # never set -> the handler never returns

    class _Hang(H2ServerProtocol):
        async def handle(self, request):
            await request.read()
            await hang.wait()

    conns = ServerConnections()
    loop = asyncio.get_running_loop()
    server = await loop.create_server(conns.track(_Hang), "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)
        conn = H2Connection(transport, authority=f"{host}:{port}", backend=backend)
        await conn.__aenter__()
        pending_req = asyncio.ensure_future(conn.request("GET", "/hang"))  # handler will hang
        await asyncio.sleep(0.02)  # let the request reach the server + the handler start
        assert conns.count() == 1
        await conns.shutdown(timeout=0.05)  # won't drain -> force-close; must still return
        assert conns.count() == 0
        pending_req.cancel()
        await conn.__aexit__(None, None, None)


class _FakeProto:
    """A stand-in protocol for ServerConnections.shutdown: `graceful_shutdown` may
    raise; `wait_closed`/`close` are inert. Lets us drive the shutdown fan-out
    without a live connection."""

    def __init__(self, *, boom=False):
        self._boom = boom
        self.drained = False
        self._serve_task = None

    async def graceful_shutdown(self):
        if self._boom:
            raise RuntimeError("graceful_shutdown blew up")
        self.drained = True

    async def wait_closed(self):
        pass

    def close(self):
        pass


@pytest.mark.asyncio
async def test_server_connections_shutdown_survives_one_bad_connection():
    """One connection whose graceful_shutdown() raises must not abort the whole
    shutdown — every other connection is still drained and shutdown() returns (F54)."""
    conns = ServerConnections()
    good, bad = _FakeProto(), _FakeProto(boom=True)
    conns._live.update({good, bad})
    await conns.shutdown(timeout=0.05)  # must NOT propagate bad's exception (returns cleanly)
    assert good.drained  # the healthy connection was still drained


@pytest.mark.asyncio
async def test_auto_graceful_cancels_silent_preface_sniff():
    """A graceful shutdown of an AutoServerProtocol whose client connected but sent
    nothing interrupts the parked preface sniff and finishes promptly, instead of
    lingering until the host's force-close timeout (F36)."""
    server, host, port, protocols = await _serve(_EchoAuto)
    backend = AsyncioBackend()
    async with server:
        transport = await backend.connect_tcp(host, port)  # connect, then send NOTHING
        await asyncio.sleep(0.05)  # let the server accept + park in the preface sniff
        assert len(protocols) == 1
        proto = protocols[0]
        await proto.graceful_shutdown()  # must cancel the parked sniff
        # With the fix the serve task ends promptly; without it the sniff stays parked
        # and this wait_for times out (the whole point of F36).
        await asyncio.wait_for(proto.wait_closed(), timeout=3.0)
        transport.close()
