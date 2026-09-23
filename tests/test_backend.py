"""TonioBackend glue: the seam's no-wait peek (`receive_nowait`, the h1 server's
unread-body drain and the client's send-time check), the bounded reader (the h1 head-read
deadline riding the read), and the two closes. The peek and the reader are tonio's own
public stream API (`receive_some_nowait`, `waiter_readable` / `watch_readable`), so they
are exercised on tonio's real streams — a loopback pair, and a TLS pair minted by trustme —
plus, for the reader's loop itself, a scripted stream presenting that same surface."""

import ssl

import pytest
import trustme
from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners
from tonio.colored.net.tls import TLSStream, open_tls_over_tcp_listeners
from tonio.colored.time import time as _now
from tonio.exceptions import CancelledError, ResourceBroken

from httpunk._backend.tonio import TonioBackend


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


# ----- close_transport / shutdown_transport: dispatch on the stream's kind -----


class _FakeUnderlyingSocket:
    """The plain socket stream beneath a `TLSStream` (`TLSStream.transport`): its
    `close()` is synchronous."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeTLSStream(TLSStream):
    """A stand-in for tonio's `TLSStream` — a subclass, since the backend dispatches on
    the type: the underlying `.transport` socket, and an own `close()` that is a
    coroutine (the TLS `close_notify` dance), which a sync caller must NOT invoke."""

    def __init__(self):  # noqa: PLW0231 - the real __init__ needs a socket + context
        self.transport = _FakeUnderlyingSocket()
        self.close_coro_called = False

    async def close(self):
        self.close_coro_called = True


class _FakePlainStream:
    """A stand-in for tonio's `SocketStream`: `close()` is synchronous."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_close_transport_plain_socket_calls_close():
    stream = _FakePlainStream()
    TonioBackend().close_transport(stream)
    assert stream.closed


def test_close_transport_tls_closes_underlying_socket_synchronously():
    # The ABORTIVE end (hyper dropping the IO): a TLSStream's own close() is the
    # close_notify coroutine, which this end must NOT run; the underlying socket is
    # closed directly so the peer's read ends, with no alert on the wire.
    stream = _FakeTLSStream()
    TonioBackend().close_transport(stream)
    assert stream.transport.closed  # underlying socket really closed
    assert not stream.close_coro_called  # the async close_notify coroutine untouched


@pytest.mark.tonio
async def test_shutdown_transport_plain_socket_calls_close():
    stream = _FakePlainStream()
    await TonioBackend().shutdown_transport(stream)
    assert stream.closed


@pytest.mark.tonio
async def test_shutdown_transport_tls_awaits_close_notify():
    # The ORDERLY end (hyper `poll_shutdown` on the IO): the TLSStream's own close()
    # coroutine — close_notify, then the socket — is what runs.
    stream = _FakeTLSStream()
    await TonioBackend().shutdown_transport(stream)
    assert stream.close_coro_called


@pytest.mark.tonio
async def test_shutdown_transport_tls_swallows_a_failed_alert_write():
    # A peer already gone: the alert cannot be written. hyper surfaces that as
    # `Kind::Shutdown` from a connection future the serve loop has already left;
    # the socket is closed regardless (the TLSStream's `finally`), so nothing to raise.
    class _Broken(_FakeTLSStream):
        async def close(self):
            self.close_coro_called = True
            raise BrokenPipeError

    stream = _Broken()
    await TonioBackend().shutdown_transport(stream)
    assert stream.close_coro_called


# ----- select_events: one suspension on several events, no verdict -----


@pytest.mark.tonio
async def test_select_events_resumes_on_either_event_without_a_verdict():
    # tonio's merged waiter (`Waiter.any`): resumes once ANY event is set, at once when
    # one already is, returns nothing — the flags are the answer; reusable after a clear.
    backend = TonioBackend()
    a, b = backend.event(), backend.event()
    a.set()
    assert await backend.select_events(a, b) is None
    a, b = backend.event(), backend.event()

    async def setter():
        b.set()

    async with scope() as s:
        s.spawn(setter())
        await backend.select_events(a, b)
    assert not a.is_set() and b.is_set()
    b.clear()
    async with scope() as s:
        s.spawn(setter())
        await backend.select_events(a, b)
    assert b.is_set()


@pytest.mark.tonio
async def test_select_events_is_cancellable_while_parked():
    # The wait is the calling task's own suspension: cancelling the task ends it with
    # `CancelledError`, nothing else was spawned to clean up.
    backend = TonioBackend()
    a, b = backend.event(), backend.event()
    seen, parked, unwound = [], Event(), Event()

    async def waiter():
        parked.set()
        try:
            await backend.select_events(a, b)
        except CancelledError:
            seen.append("cancelled")
            unwound.set()
            raise

    async with scope() as s:
        s.spawn(waiter())
        await parked.wait()
        s.cancel()
    await unwound.wait()  # the scope exit does not wait for a cancelled child to unwind
    assert seen == ["cancelled"]


# ----- real streams: a loopback pair and a TLS pair -----


class _Pair:
    """`client`/`server` ends of one accepted connection, opened inside `scope` (the
    accept is a spawned task); `close()` ends both abortively and cancels the scope."""

    def __init__(self, client, server, s):
        self.client, self.server, self._s = client, server, s

    def close(self):
        backend = TonioBackend()
        backend.close_transport(self.client)
        backend.close_transport(self.server)
        self._s.cancel()


async def _tcp_pair(s):
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    accepted, got = [], Event()

    async def accept():
        accepted.append(await listener.accept())
        got.set()

    s.spawn(accept())
    client = await TonioBackend().connect_tcp(host, port)
    await got.wait()
    listener.close()
    return _Pair(client, accepted[0], s)


async def _tls_pair(s, ca):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(ctx)
    listener = (await open_tls_over_tcp_listeners(0, ctx, host="127.0.0.1"))[0]
    host, port = listener.transport.socket.getsockname()[:2]
    accepted, got = [], Event()

    async def accept():
        accepted.append(await listener.accept())  # handshaken on return
        got.set()

    s.spawn(accept())
    client_ctx = ssl.create_default_context()
    ca.configure_trust(client_ctx)
    client, _ = await TonioBackend().connect_tls(host, port, ssl_context=client_ctx)
    await got.wait()
    listener.close()
    return _Pair(client, accepted[0], s)


# ----- receive_nowait: bytes | b"" (EOF) | None (nothing ready), never a suspension -----


@pytest.mark.tonio
async def test_receive_nowait_plain_socket():
    backend = TonioBackend()
    async with scope() as s:
        pair = await _tcp_pair(s)
        try:
            server, client = pair.server, pair.client
            assert backend.receive_nowait(server) is None  # nothing ready: distinct from EOF
            await client.send_all(b"raw bytes")
            await server.wait_readable()  # the bytes are in the socket (no peek runs before that)
            assert backend.receive_nowait(server) == b"raw bytes"
            assert backend.receive_nowait(server) is None
            client.close()
            await server.wait_readable()  # the FIN is there
            assert backend.receive_nowait(server) == b""  # EOF
        finally:
            pair.close()


@pytest.mark.tonio
async def test_receive_nowait_tls_decrypts_what_the_socket_holds(ca):
    """Over TLS the peek is tonio's no-wait read: ciphertext already in the socket is
    decrypted without suspending (hyper's read over rustls does the same), so the
    server's drain finds a small unread body where the old plaintext-only peek saw
    nothing. A close_notify already there is `b""`."""
    backend = TonioBackend()
    async with scope() as s:
        pair = await _tls_pair(s, ca)
        try:
            server, client = pair.server, pair.client
            assert backend.receive_nowait(server) is None
            await client.send_all(b"hello")
            await server.transport.wait_readable()  # the RECORD is in the raw socket, nothing decoded yet
            assert backend.receive_nowait(server) == b"hello"  # decrypted by the peek itself
            assert backend.receive_nowait(server) is None
            await client.close()  # close_notify
            await server.transport.wait_readable()
            assert backend.receive_nowait(server) == b""  # a clean TLS EOF
        finally:
            pair.close()


@pytest.mark.tonio
async def test_receive_nowait_tls_abrupt_close_is_broken_transport(ca):
    """The peer's socket closed with no close_notify (what `close_transport` does): the
    peek raises `ResourceBroken` — a `broken_transport_errors` shape, as a plain socket's
    `ConnectionResetError` is. The server drain treats any raise as "not drainable"."""
    backend = TonioBackend()
    async with scope() as s:
        pair = await _tls_pair(s, ca)
        try:
            backend.close_transport(pair.client)  # the socket beneath, no alert
            await pair.server.transport.wait_readable()
            with pytest.raises(ResourceBroken):
                backend.receive_nowait(pair.server)
            assert isinstance(ResourceBroken(), backend.broken_transport_errors)
        finally:
            pair.close()


# ----- bounded_reader on real streams: the deadline rides the read, nothing is spawned -----


@pytest.mark.tonio
async def test_bounded_reader_plain_socket():
    backend = TonioBackend()
    async with scope() as s:
        pair = await _tcp_pair(s)
        try:
            server, client = pair.server, pair.client
            read = backend.bounded_reader(server)  # chosen once per connection
            assert await read(100, backend.monotonic() - 1.0) is None  # deadline passed, nothing there: at once
            await client.send_all(b"early")
            await server.wait_readable()
            assert await read(100, backend.monotonic() - 1.0) == b"early"  # bytes first, whatever the clock says

            async def sender():
                await client.send_all(b"parked")

            s.spawn(sender())
            assert await read(100, backend.monotonic() + 5.0) == b"parked"  # the readiness wake (or at once)
            assert await read(100, backend.monotonic() + 0.02) is None  # the timer; the socket stays usable
            await client.send_all(b"after")
            assert await server.receive_some(100) == b"after"
        finally:
            pair.close()


@pytest.mark.tonio
async def test_bounded_reader_tls(ca):
    backend = TonioBackend()
    async with scope() as s:
        pair = await _tls_pair(s, ca)
        try:
            server, client = pair.server, pair.client
            read = backend.bounded_reader(server)
            assert await read(100, backend.monotonic() - 1.0) is None
            await client.send_all(b"early")
            await server.transport.wait_readable()  # the record is in the socket, undecoded
            assert await read(100, backend.monotonic() - 1.0) == b"early"  # the no-wait read decrypts it

            async def sender():
                await client.send_all(b"parked")

            s.spawn(sender())
            assert await read(100, backend.monotonic() + 5.0) == b"parked"
            assert await read(100, backend.monotonic() + 0.02) is None
            await client.send_all(b"after")
            assert await server.receive_some(100) == b"after"
            await client.close()  # close_notify: the bounded read reports the clean EOF
            assert await read(100, backend.monotonic() + 5.0) == b""
        finally:
            pair.close()


# ----- bounded_reader's loop, on a scripted stream presenting the same surface -----


class _ScriptedStream:
    """A `SocketStream`'s no-wait read and readiness waiter, scripted: `recvs` are the
    read's answers in order (`NotReady` = EAGAIN), `arms` the waiter's — "ready" (None:
    the bits are set, read now), "wake" (a waiter that resumes at once: a readiness
    wake), "timer" (a waiter that resumes when the asked-for timeout fires, as the
    runtime's does), "park" (a waiter that never resumes: the bare question's answer
    when no bits are set — never awaited). `calls` records every timeout asked for (µs;
    None = the bare readiness question after a wake)."""

    class NotReady:
        pass

    def __init__(self, recvs, arms):
        self._recvs, self._arms, self.calls = list(recvs), list(arms), []

    def receive_some_nowait(self, max_bytes):
        return self._recvs.pop(0)

    def waiter_readable(self, timeout=None):
        self.calls.append(timeout)
        kind = self._arms.pop(0)
        if kind == "ready":
            return None
        evt = Event()
        if kind == "wake":
            evt.set()
        return evt.waiter(timeout)


def _bounded(stream, timeout=1.0):
    return TonioBackend().bounded_reader(stream)(100, _now() + timeout)


def _about(micros, seconds):
    return seconds * 1_000_000 - 5_000 < micros <= seconds * 1_000_000  # the clock ran a little


@pytest.mark.tonio
async def test_bounded_loop_readable_at_once_never_arms():
    # The read comes first (hyper `poll_read` before its timer poll): bytes there = done.
    stream = _ScriptedStream(recvs=[b"head"], arms=[])
    assert await _bounded(stream) == b"head"
    assert stream.calls == []  # no clock, no arm


@pytest.mark.tonio
async def test_bounded_loop_readiness_wake_reads_bytes():
    # EAGAIN (the probe: stale bits, as tokio's readiness) -> arm for the time left -> park
    # -> the readiness wake -> the question ("readable?" yes: the edge is now observed, so
    # the next EAGAIN's clear is honoured) -> the bytes.
    stream = _ScriptedStream(recvs=[_ScriptedStream.NotReady, b"head"], arms=["wake", "ready"])
    assert await _bounded(stream, 0.5) == b"head"
    assert len(stream.calls) == 2 and _about(stream.calls[0], 0.5)  # the arm carried the deadline...
    assert stream.calls[1] is None  # ...the question carries none


@pytest.mark.tonio
async def test_bounded_loop_bits_landing_before_the_arm_read_at_once():
    # EAGAIN -> the arm finds the bits already set (data landed in between) -> read now.
    stream = _ScriptedStream(recvs=[_ScriptedStream.NotReady, b"head"], arms=["ready"])
    assert await _bounded(stream) == b"head"
    assert len(stream.calls) == 1


@pytest.mark.tonio
async def test_bounded_loop_past_deadline_expires_without_arming():
    # Nothing readable and the deadline already passed (a spurious wake landing after it, a
    # partial head whose next read starts late): hyper's Pending + a ready timer poll ->
    # `None` at once. No zero-length timer is ever armed.
    stream = _ScriptedStream(recvs=[_ScriptedStream.NotReady], arms=[])
    assert await _bounded(stream, -0.001) is None
    assert stream.calls == []


@pytest.mark.tonio
async def test_bounded_loop_timer_wake_is_told_by_the_readiness_question():
    # EAGAIN -> arm -> the timer fires -> the question ("readable?" no: a waiter) -> None.
    # No read after the timer (the script has none to give), nothing re-armed with a time.
    stream = _ScriptedStream(recvs=[_ScriptedStream.NotReady], arms=["timer", "park"])
    assert await _bounded(stream, 0.02) is None
    assert len(stream.calls) == 2 and _about(stream.calls[0], 0.02) and stream.calls[1] is None


@pytest.mark.tonio
async def test_bounded_loop_spurious_wake_rearms_with_the_remaining_time():
    # ... -> park -> a readiness wake whose read finds nothing (spurious) -> re-arm for
    # what is LEFT of the same deadline (never the full timeout again) -> the bytes.
    stream = _ScriptedStream(
        recvs=[_ScriptedStream.NotReady, _ScriptedStream.NotReady, b"head"],
        arms=["wake", "ready", "wake", "ready"],
    )
    assert await _bounded(stream, 0.5) == b"head"
    first, again = stream.calls[0], stream.calls[2]
    assert _about(first, 0.5)
    assert 0 < again <= first  # the deadline is absolute
