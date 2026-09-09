"""Minimal step-2 smoke for the asyncio backend — proves `_AsyncioStream`'s buffer
+ a real loopback `connect_tcp` round-trip work. The full contract + e2e suite
(scope teardown, select, TLS/ALPN, driver round-trips) is step 3.
"""

import asyncio

import pytest

from httpunk._backend.asyncio import AsyncioBackend, _AsyncioStream


@pytest.mark.asyncio
async def test_stream_buffer_receive_peek_and_eof():
    s = _AsyncioStream()
    s.connection_made(None)  # no transport needed while under the read high-water mark
    s.data_received(b"hello")
    assert s.read_nowait() == b"hello"  # sync peek drains the buffer
    assert s.read_nowait() is None  # empty now: nothing ready (NOT EOF)
    s.data_received(b"world")
    assert await s.receive_some(3) == b"wor"  # partial read honors max_bytes
    assert await s.receive_some() == b"ld"
    s.eof_received()
    assert s.read_nowait() == b""  # EOF -> b"" from the peek too (the h1 server's probe relies on it)
    assert await s.receive_some() == b""  # EOF -> b""


@pytest.mark.asyncio
async def test_receive_some_blocks_until_data():
    s = _AsyncioStream()
    s.connection_made(None)

    async def feed_later():
        await asyncio.sleep(0.01)
        s.data_received(b"late")

    asyncio.ensure_future(feed_later())
    assert await s.receive_some() == b"late"  # parked until data arrives


@pytest.mark.asyncio
async def test_connect_tcp_roundtrip_over_loopback():
    async def echo(reader, writer):
        data = await reader.read(1024)
        writer.write(b"echo:" + data)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    async with server:
        stream = await AsyncioBackend().connect_tcp(host, port)
        await stream.send_all(b"ping")
        got = b""
        while True:
            chunk = await stream.receive_some()
            if not chunk:
                break
            got += chunk
        stream.close()
    assert got == b"echo:ping"


# ----- contract tests for the risky seam bits (in isolation) -----


class _FakeTransport:
    """Records pause/resume_reading for the backpressure contract test."""

    def __init__(self):
        self.reading_paused = False

    def pause_reading(self):
        self.reading_paused = True

    def resume_reading(self):
        self.reading_paused = False


@pytest.mark.asyncio
async def test_read_backpressure_pauses_and_resumes():
    s = _AsyncioStream()
    s.connection_made(_FakeTransport())
    s.data_received(b"x" * (2**16))  # at/over the high-water mark -> pause reading
    assert s._transport.reading_paused
    assert await s.receive_some(2**16) == b"x" * (2**16)  # drain -> resume
    assert not s._transport.reading_paused


@pytest.mark.asyncio
async def test_select_returns_winner_and_cancels_loser():
    backend = AsyncioBackend()
    loser_finished = False

    async def winner():
        return "win"

    async def loser():
        nonlocal loser_finished
        await asyncio.sleep(10)
        loser_finished = True

    assert await backend.select(winner(), loser()) == "win"
    assert not loser_finished  # the loser was cancelled, not left running


@pytest.mark.asyncio
async def test_send_all_raises_after_connection_lost():
    # A write to a dead socket must FAIL — asyncio silently discards writes after
    # connection_lost, but tonio (like a raw socket) raises, and drivers detect a
    # dead peer that way (F32). Surface the real error, or EPIPE on a clean FIN.
    s = _AsyncioStream()
    s.connection_made(_FakeTransport())
    s.connection_lost(ConnectionResetError("peer reset"))
    with pytest.raises(ConnectionResetError):
        await s.send_all(b"data")

    clean = _AsyncioStream()
    clean.connection_made(_FakeTransport())
    clean.connection_lost(None)  # a clean peer close carries no error
    with pytest.raises(BrokenPipeError):
        await clean.send_all(b"data")


class _FakeCloseTransport:
    """Records abort() vs close() and reports whether it is a TLS transport."""

    def __init__(self, *, ssl):
        self._ssl = ssl
        self.aborted = False
        self.closed = False

    def get_extra_info(self, name):
        return object() if (name == "ssl_object" and self._ssl) else None

    def abort(self):
        self.aborted = True

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_close_is_orderly_and_abort_is_abortive():
    # hyper's two ends. `close()` = the `Connection` future completing -> `poll_shutdown`
    # on the IO: asyncio's `transport.close()` runs the TLS close_notify exchange (and is
    # a plain FIN over TCP). `abort()` = the IO dropped without `poll_shutdown`: over TLS
    # `transport.abort()` (no close_notify); plain TCP has nothing to skip -> `close()`.
    tls = _AsyncioStream()
    tls.connection_made(_FakeCloseTransport(ssl=True))
    tls.close()
    assert tls._transport.closed and not tls._transport.aborted

    tls = _AsyncioStream()
    tls.connection_made(_FakeCloseTransport(ssl=True))
    tls.abort()
    assert tls._transport.aborted and not tls._transport.closed

    plain = _AsyncioStream()
    plain.connection_made(_FakeCloseTransport(ssl=False))
    plain.abort()
    assert plain._transport.closed and not plain._transport.aborted


@pytest.mark.asyncio
async def test_backend_close_transport_aborts_and_shutdown_transport_closes():
    backend = AsyncioBackend()
    tls = _AsyncioStream()
    tls.connection_made(_FakeCloseTransport(ssl=True))
    backend.close_transport(tls)  # the abortive end
    assert tls._transport.aborted and not tls._transport.closed

    tls = _AsyncioStream()
    tls.connection_made(_FakeCloseTransport(ssl=True))
    await backend.shutdown_transport(tls)  # the orderly end
    assert tls._transport.closed and not tls._transport.aborted


@pytest.mark.asyncio
async def test_second_concurrent_receive_some_raises():
    # The single-reader contract: a second receive_some while one is parked must fail
    # loudly rather than clobber the first's waiter and hang it forever (F55).
    s = _AsyncioStream()
    s.connection_made(None)
    first = asyncio.ensure_future(s.receive_some())  # parks: buffer empty, no EOF
    await asyncio.sleep(0)  # let it register its read waiter
    with pytest.raises(RuntimeError, match="single-reader"):
        await s.receive_some()  # a concurrent second reader
    first.cancel()


@pytest.mark.asyncio
async def test_select_prefers_argument_order_when_both_ready():
    # Among racers ready in the same wakeup the winner is the FIRST argument, matching
    # tonio's spawn-in-order select (asyncio.wait's `done` set is unordered) — F33c.
    backend = AsyncioBackend()

    async def a():
        return "a"

    async def b():
        return "b"

    for _ in range(20):
        assert await backend.select(a(), b()) == "a"


@pytest.mark.asyncio
async def test_select_discards_losing_exception():
    # A loser that raises (here, on cancellation) must NOT surface out of select —
    # only the winner's outcome propagates (F33c). The old drain only caught
    # CancelledError, so a loser's other exception leaked.
    backend = AsyncioBackend()

    async def winner():
        return "win"

    async def loser():
        try:
            await asyncio.sleep(10)
        finally:
            raise RuntimeError("loser boom")

    assert await backend.select(winner(), loser()) == "win"


@pytest.mark.asyncio
async def test_select_cancels_racers_when_itself_cancelled():
    # If select is cancelled its racers are cancelled too — no orphans (F33c).
    backend = AsyncioBackend()
    cancelled = []

    async def racer(tag):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(tag)
            raise

    sel = asyncio.ensure_future(backend.select(racer("x"), racer("y")))
    for _ in range(3):
        await asyncio.sleep(0)  # let both racers park in sleep()
    sel.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sel
    assert set(cancelled) == {"x", "y"}


@pytest.mark.asyncio
async def test_scope_spawn_cancel_and_join():
    backend = AsyncioBackend()
    ran = []

    async def work(tag, delay):
        try:
            await asyncio.sleep(delay)
            ran.append(tag)
        except asyncio.CancelledError:
            ran.append(f"{tag}-cancelled")
            raise

    scope = backend.scope()
    await scope.__aenter__()
    scope.spawn(work("fast", 0))
    scope.spawn(work("slow", 10))
    await asyncio.sleep(0.01)  # let "fast" finish
    scope.cancel()
    await scope.__aexit__(None, None, None)  # joins; swallows "slow"'s CancelledError
    assert "fast" in ran
    assert "slow-cancelled" in ran


@pytest.mark.asyncio
async def test_queue_send_and_receive_fifo():
    sender, receiver = AsyncioBackend().queue()
    sender.send(1)
    sender.send(2)
    assert await receiver.receive() == 1
    assert await receiver.receive() == 2


# ----- receive_bounded / bounded_reader: the head-read deadline on the read future (no task, nothing cancelled) -----


def _stream():
    s = _AsyncioStream()
    s.connection_made(_FakeCloseTransport(ssl=False))
    return s


def _at(seconds):
    return asyncio.get_running_loop().time() + seconds


@pytest.mark.asyncio
async def test_receive_bounded_buffered_bytes_return_at_once():
    s = _stream()
    s.data_received(b"hi")
    assert await s.receive_bounded(100, _at(5.0)) == b"hi"


@pytest.mark.asyncio
async def test_receive_bounded_data_wins_and_the_timer_is_cancelled():
    s = _stream()
    asyncio.get_running_loop().call_soon(s.data_received, b"soon")
    assert await s.receive_bounded(100, _at(5.0)) == b"soon"
    assert s._read_waiter is None


@pytest.mark.asyncio
async def test_receive_bounded_expires_to_none_and_the_stream_keeps_working():
    s = _stream()
    assert await s.receive_bounded(100, _at(0.01)) is None
    s.data_received(b"later")
    assert await s.receive_some(100) == b"later"


@pytest.mark.asyncio
async def test_receive_bounded_eof_is_empty_bytes():
    s = _stream()
    asyncio.get_running_loop().call_soon(s.eof_received)
    assert await s.receive_bounded(100, _at(5.0)) == b""


@pytest.mark.asyncio
async def test_backend_bounded_reader_and_timed_event_wait():
    backend = AsyncioBackend()
    s = _stream()
    s.data_received(b"own")
    assert await backend.bounded_reader(s)(100, _at(5.0)) == b"own"  # the seam's stream bounds itself
    # The backend's Event carries tonio's `wait(timeout)`: no verdict, `is_set()` is the answer.
    assert backend.event is asyncio.Event  # plain events carry no wrapper
    evt = backend.timed_event()
    await evt.wait(0.01)
    assert not evt.is_set()
    evt.set()
    await evt.wait(0.01)
    assert evt.is_set()


@pytest.mark.asyncio
async def test_received_objects_are_handed_through_without_copying():
    # BOUNDARY_NOTES S2: a read that covers a whole received object gets that object.
    s = _AsyncioStream()
    first, second = b"first-segment", b"second"
    s.data_received(first)
    s.data_received(second)
    assert await s.receive_some() is first
    assert s.read_nowait() is second
    assert s.read_nowait() is None


@pytest.mark.asyncio
async def test_partial_reads_walk_one_object_then_the_next():
    s = _AsyncioStream()
    s.data_received(b"abcdef")
    s.data_received(b"gh")
    assert await s.receive_some(2) == b"ab"
    assert await s.receive_some(3) == b"cde"
    assert await s.receive_some(10) == b"f"  # the rest of the first object, never merged with the next
    assert await s.receive_some(10) == b"gh"
    assert s.read_nowait() is None


@pytest.mark.asyncio
async def test_backpressure_counts_unread_bytes_across_objects():
    s = _AsyncioStream()
    s.connection_made(_FakeTransport())
    for _ in range(4):
        s.data_received(b"x" * (2**14))  # four objects reach the 64 KiB watermark together
    assert s._transport.reading_paused
    await s.receive_some(2**14)  # one object out: 48 KiB left, below the mark
    assert not s._transport.reading_paused
