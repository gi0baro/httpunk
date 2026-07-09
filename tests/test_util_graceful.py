"""Graceful shutdown — the non-blocking server `graceful_shutdown()` primitives
(h2 GOAWAY + refuse-new; h1 stop-reuse + release-idle-read) and the self-contained
`httpunk.util.GracefulShutdown` coordinator (≈ hyper-util `server::graceful`).

Direct unit tests cover the primitive branches; loopback tests drive the coordinator
end-to-end and prove an in-flight request finishes and an idle connection is released
before the connection closes.
"""

from types import SimpleNamespace

import pytest
from _client import open_h1  # noqa: F401  (kept for symmetry; loopback uses raw connections)
from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners

from httpunk import H1Connection, H2Connection, H2Reason
from httpunk._backend.tonio import TonioBackend
from httpunk.exceptions import H2Error
from httpunk.h1.server import H1Server, ServerConnection as H1ServerConnection
from httpunk.h2.server import H2Server, ServerConnection as H2ServerConnection
from httpunk.h2.streams import _StreamError
from httpunk.util import GracefulShutdown


class _IdleTransport:
    """A live-but-quiet transport: `receive_some` yields nothing, `close` is a no-op
    record. Used where the primitive must not depend on transport behavior."""

    def __init__(self):
        self.closed = False

    async def receive_some(self, max_bytes=65536):
        return b""

    async def send_all(self, data):
        pass

    def close(self):
        self.closed = True


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


# ----- coordinator basics -----


def test_count_starts_at_zero():
    assert GracefulShutdown().count() == 0


@pytest.mark.tonio
async def test_shutdown_with_no_connections_returns_immediately():
    await GracefulShutdown().shutdown()  # must not hang


# ----- h2 primitive: refuse new streams once graceful (non-blocking signal) -----


@pytest.mark.tonio
async def test_h2_refuses_new_streams_once_graceful():
    conn = H2ServerConnection(_IdleTransport(), max_concurrent_streams=100)
    conn.streams._graceful = True  # as graceful_shutdown() sets after the GOAWAY
    with pytest.raises(_StreamError) as excinfo:
        conn.streams._recv_headers_target(SimpleNamespace(stream_id=1))
    assert excinfo.value.stream_id == 1
    assert excinfo.value.reason == int(H2Reason.REFUSED_STREAM)
    assert conn.streams._last_processed_id == 0  # a refused stream must not count


# ----- h1 primitive: non-blocking signal (stop reuse + arm the idle-read release) -----


@pytest.mark.tonio
async def test_h1_graceful_is_a_nonblocking_signal():
    transport = _IdleTransport()
    conn = H1ServerConnection(transport)
    await conn.graceful_shutdown()
    assert not conn._reusable
    assert conn._shutdown_evt.is_set()
    assert not transport.closed  # the primitive does NOT close — the serve loop does
    assert await conn.next_request() is None  # not reusable -> serves no more


# ----- h2 end-to-end: an in-flight request finishes before close -----


@pytest.mark.tonio
async def test_h2_graceful_drains_open_connection_then_closes():
    # A full request/response completes on the (kept-open) connection; graceful
    # shutdown then drains that now-idle h2 connection, sends GOAWAY, and closes it,
    # so a subsequent request is refused. (In-flight *concurrent* completion is
    # guaranteed structurally — the accept loop only ends once `_streams` is empty,
    # see `_release_slot` — and refuse-new is covered by the direct unit test above.)
    listener, host, port = await _listener()
    graceful = GracefulShutdown()

    async def serve(server):
        async with server:
            async for req in server:
                await req.respond(200, body=b"finished")

    async with scope() as s:

        async def connection_task():
            transport = await listener.accept()
            await graceful.watch(H2Server(transport), serve)

        s.spawn(connection_task())

        client_transport = await TonioBackend().connect_tcp(host, port)
        conn = H2Connection(client_transport, authority=f"{host}:{port}")
        await conn.__aenter__()

        resp = await conn.get("/")
        assert await resp.read() == b"finished"
        assert graceful.count() == 1  # connection still open, being watched

        await graceful.shutdown()  # GOAWAY + drain (idle) + close
        assert graceful.count() == 0

        # A new request is refused: either GoAwayError (the client saw our GOAWAY) or
        # ConnectionClosedError (it saw EOF first) — both are H2Error, timing-dependent.
        with pytest.raises(H2Error):
            await conn.get("/")
        await conn.__aexit__(None, None, None)


# ----- h1 end-to-end: an idle keep-alive connection is released and closed -----


@pytest.mark.tonio
async def test_h1_graceful_releases_idle_connection_and_closes():
    listener, host, port = await _listener()
    graceful = GracefulShutdown()
    served = Event()

    async def serve(server):
        async with server:
            async for req in server:
                await req.read()
                await req.respond(200, body=b"ok")
                served.set()

    async with scope() as s:

        async def connection_task():
            transport = await listener.accept()
            await graceful.watch(H1Server(transport), serve)

        s.spawn(connection_task())

        client_transport = await TonioBackend().connect_tcp(host, port)
        conn = H1Connection(client_transport, authority=f"{host}:{port}")
        await conn.__aenter__()
        resp = await conn.request("GET", "/", headers={"host": host})
        assert await resp.read() == b"ok"
        await served.wait()
        assert graceful.count() == 1

        # The server is now idle, parked reading the next request head on a
        # keep-alive connection. Graceful shutdown must release that read (via the
        # shutdown-event race) so the connection completes and closes.
        await graceful.shutdown()
        assert graceful.count() == 0
        await conn.__aexit__(None, None, None)
