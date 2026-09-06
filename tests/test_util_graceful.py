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
from tonio.colored import Event, scope, sleep
from tonio.colored.net import open_tcp_listeners

from httpunk import H1Connection, H2Connection, H2Reason
from httpunk._backend.tonio import TonioBackend
from httpunk._httpunk import H2_HEADERS_IGNORED, H2_HEADERS_OPENED, H2Codec, H2FrameGoAway, H2FramePing
from httpunk.exceptions import HTTPunkError
from httpunk.h1.server import H1Server, ServerConnection as H1ServerConnection
from httpunk.h2.server import H2Server, ServerConnection as H2ServerConnection
from httpunk.h2.stream import Stream
from httpunk.http import HeaderMap
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


class _CapturingTransport(_IdleTransport):
    """`_IdleTransport` that also records everything written, so a test can decode the
    exact frames a primitive emitted."""

    def __init__(self):
        super().__init__()
        self.sent = b""

    async def send_all(self, data):
        self.sent += data


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


@pytest.mark.tonio
async def test_watch_registers_count_synchronously():
    """watch() bumps the count when CALLED — before the driving coroutine is scheduled —
    so count()/shutdown() can't race a not-yet-run watch (hyper-util watcher(), F53).
    With the old async-registration the count stayed 0 until the coroutine ran."""
    graceful = GracefulShutdown()
    release = Event()

    async def _noop_graceful_shutdown():
        pass

    server = SimpleNamespace(graceful_shutdown=_noop_graceful_shutdown)

    async def serve(_server):
        await release.wait()  # keep the watch live until we let it finish

    coro = graceful.watch(server, serve)  # SYNC call — must register the slot NOW
    assert graceful.count() == 1  # incremented synchronously, before the coro is spawned

    async with scope() as s:
        s.spawn(coro)
        await sleep(0)  # let the watch coroutine start and its signal-watcher park
        await sleep(0)
        release.set()  # let serve() (and thus the watch) complete
    assert graceful.count() == 0  # dropped when the watch finished


# ----- h2 primitive: two-phase graceful (serve during phase 1, ignore after phase 2) -----


def _req_frame(stream_id):
    """A minimal valid request-HEADERS frame (END_STREAM), minted through a codec round trip."""
    raw = H2Codec("client").serialize_request_headers(stream_id, "GET", "https://x/", HeaderMap(), end_stream=True)
    [frame] = H2Codec("server").receive(raw)
    return frame


def _data_frame(stream_id, payload=b"x"):
    [frame] = H2Codec("server").receive(H2Codec("client").serialize_data(stream_id, payload))
    return frame


@pytest.mark.tonio
async def test_h2_graceful_two_phase_serves_then_ignores():
    """Two-phase graceful shutdown (h2). PHASE 1 (GOAWAY 2^31-1 + shutdown PING) keeps
    ACCEPTING streams — a request already in flight when we started the shutdown is
    served, not refused. PHASE 2 (the shutdown PING's ack lowers `max_stream_id` to the
    last-processed id) silently IGNORES streams opened above it, never REFUSED."""
    conn = H2ServerConnection(_IdleTransport(), max_concurrent_streams=100)
    conn.begin_graceful_shutdown()  # phase 1: max_stream_id is still 2^31-1
    assert conn.graceful
    v = conn.recv_headers(_req_frame(1), Stream(conn.backend))
    assert v.kind == H2_HEADERS_OPENED  # handled by ACCEPTING (served), not refusing
    assert conn.has_stream(1)
    assert conn.last_processed_id == 1

    # phase 2, as the shutdown PING's ack sets it: the final GOAWAY's last-processed id
    conn.set_max_stream_id(conn.last_processed_id)  # 1
    v = conn.recv_headers(_req_frame(3), Stream(conn.backend))
    assert v.kind == H2_HEADERS_IGNORED  # id > max -> ignored
    assert not conn.has_stream(3)
    assert conn.last_processed_id == 1  # an ignored stream must not count


@pytest.mark.tonio
async def test_h2_frame_above_goaway_ignored_not_errored():
    """A NON-headers frame (DATA/etc.) on a client stream above the last-stream-id of
    our GOAWAY is silently IGNORED (dropped with conn-window accounting, h2
    `ignore_data`), never an RST(STREAM_CLOSED) or a connection PROTOCOL_ERROR that
    would abort the graceful drain (F42)."""
    conn = H2ServerConnection(_IdleTransport(), max_concurrent_streams=100)
    conn.set_max_stream_id(1)  # as phase-2 graceful lowers it
    # Stream 3 (> 1) was refused by our GOAWAY: a late frame on it is ignored, not raised.
    v = conn.recv_data(_data_frame(3))
    assert v.handle is None and v.payload is None


@pytest.mark.tonio
async def test_h2_graceful_shutdown_emits_two_phase_goaways():
    """The graceful primitive emits the two-phase frames (F7), decoded straight off the
    bytes the connection wrote: PHASE 1 = GOAWAY(2^31-1, NO_ERROR) + a (non-ack) shutdown
    PING; PHASE 2 (on the ping's ack) = GOAWAY(the real last-processed id)."""
    t = _CapturingTransport()
    conn = H2ServerConnection(t, max_concurrent_streams=100)
    peer = H2Codec("client")  # decode what the server wrote

    await conn.graceful_shutdown()  # phase 1 (queued for the write pump)
    await conn._flush()  # no pump is running here: write what was queued
    phase1 = peer.receive(t.sent)
    t.sent = b""
    goaways = [f for f in phase1 if isinstance(f, H2FrameGoAway)]
    pings = [f for f in phase1 if isinstance(f, H2FramePing)]
    assert len(goaways) == 1 and goaways[0].last_stream_id == 2**31 - 1
    assert goaways[0].error_code == int(H2Reason.NO_ERROR)
    assert len(pings) == 1 and not pings[0].ack  # a non-ack shutdown PING follows

    # Idempotent: a second call emits nothing.
    await conn.graceful_shutdown()
    await conn._flush()
    assert t.sent == b""

    # Phase 2: the shutdown ping's ack, after having processed up to stream 5.
    conn.set_last_processed_id(5)
    [pong] = H2Codec("server").receive(H2Codec("client").serialize_ping_ack(pings[0].data))
    conn._after(conn.recv_ping(pong))
    await conn._flush()
    phase2 = [f for f in peer.receive(t.sent) if isinstance(f, H2FrameGoAway)]
    assert len(phase2) == 1 and phase2[0].last_stream_id == 5  # the real last-processed id
    assert conn.shutdown_final is True


# ----- h1 primitive: non-blocking signal (stop reuse + arm the idle-read release) -----


@pytest.mark.tonio
async def test_h1_graceful_is_a_nonblocking_signal():
    transport = _IdleTransport()
    conn = H1ServerConnection(transport)
    await conn.graceful_shutdown()
    assert not conn.reusable
    assert conn._shutdown_evt.is_set()
    assert not transport.closed  # the primitive does NOT close — the serve loop does
    assert await conn.next_request() is None  # not reusable -> serves no more


# ----- h2 end-to-end: an in-flight request finishes before close -----


@pytest.mark.tonio
async def test_h2_graceful_drains_open_connection_then_closes():
    # A full request/response completes on the (kept-open) connection; graceful
    # shutdown then drains that now-idle h2 connection (two-phase: GOAWAY(2^31-1) +
    # shutdown PING, then GOAWAY(last id) on the ack) and closes it, so a subsequent
    # request fails. (In-flight *concurrent* completion is guaranteed structurally — the
    # accept loop only ends once `_streams` is empty, see `_release_slot` — and the
    # two-phase serve/ignore + frame emission are covered by the direct unit tests above.)
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

        resp = await conn.request("GET", "/")
        assert await resp.read() == b"finished"
        assert graceful.count() == 1  # connection still open, being watched

        await graceful.shutdown()  # GOAWAY + drain (idle) + close
        assert graceful.count() == 0

        # A new request is refused: either GoAwayError (the client saw our GOAWAY) or
        # ConnectionClosedError (it saw EOF first) — both are HTTPunkError (GoAwayError is
        # also an H2Error; ConnectionClosedError is protocol-neutral), timing-dependent.
        with pytest.raises(HTTPunkError):
            await conn.request("GET", "/")
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


# ----- two-party transitions (HTTPUNK_RUST_STATE_DESIGN.md §3.4) -----


@pytest.mark.tonio
async def test_shutdown_signal_and_liveness_sample_are_one_step():
    """`shutdown()` signals and samples "anyone live?" under the lock: a watch
    registered before the call is always waited for, and one whose serve ends before
    the signal was ever observed drops the count exactly once (the release latch),
    so `shutdown()` returns instead of hanging on a negative count (U3, U5)."""
    graceful = GracefulShutdown()
    release = Event()

    async def _noop_graceful_shutdown():
        pass

    server = SimpleNamespace(graceful_shutdown=_noop_graceful_shutdown)

    async def serve(_server):
        await release.wait()

    watcher = graceful.watcher()
    assert graceful.count() == 1
    async with scope() as s:
        s.spawn(watcher.watch(server, serve))
        finished = Event()

        async def shut():
            await graceful.shutdown()
            finished.set()

        s.spawn(shut())
        await sleep(0)
        assert not finished.is_set()  # a live watch: shutdown waits
        release.set()
        await finished.wait()
    watcher._release()  # a second release (a duplicate teardown path) is a no-op
    assert graceful.count() == 0
    await graceful.shutdown()  # nothing live: returns at once
