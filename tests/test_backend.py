"""TonioBackend glue. `receive_nowait` is a synchronous non-blocking peek used by
the h1 server's unread-body drain; it must read *decrypted plaintext* over TLS
(via the SSLObject), not the raw socket — a `TLSStream` carries ciphertext and
has no `.socket`, so the plain-socket path would be wrong (and would AttributeError)."""

import threading

import pytest
from tonio.colored.net.tls import TLSStream
from tonio.colored.time import time as _now

from httpunk._backend.tonio import TonioBackend


class _FakeInnerSSL:
    """Enough of `ssl.SSLObject` for `receive_nowait`: a plaintext buffer that
    `pending()` counts and `read(n)` drains — never touching a BIO/socket."""

    def __init__(self, plaintext):
        self._buf = plaintext

    def pending(self):
        return len(self._buf)

    def read(self, n):
        chunk, self._buf = self._buf[:n], self._buf[n:]
        return chunk


class _FakeSSLObject:
    """The shape of tonio's `_SSLProxy`: the `SSLObject` behind `_inner`, every use
    of it under `_lock` — the peek takes that lock for its pending-then-read."""

    def __init__(self, plaintext):
        self._lock = threading.Lock()
        self._inner = _FakeInnerSSL(plaintext)


class _FakeUnderlyingSocket:
    """The plain socket stream beneath a `TLSStream` (`TLSStream.transport`): its
    `close()` is synchronous."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeTLSStream(TLSStream):
    """A stand-in for tonio's `TLSStream` — a subclass, since the backend dispatches on
    the type: exposes `._ssl`, the underlying `.transport` socket, and — deliberately —
    NO `.socket`, so a raw-socket peek would AttributeError. Its own `close()` is a
    coroutine (the TLS `close_notify` dance), which a sync caller must NOT invoke."""

    def __init__(self, plaintext):  # noqa: PLW0231 - the real __init__ needs a socket + context
        self._ssl = _FakeSSLObject(plaintext)
        self.transport = _FakeUnderlyingSocket()
        self.close_coro_called = False

    async def close(self):
        self.close_coro_called = True


class _FakeRawSocket:
    def __init__(self, data):
        self._data = data

    def recv(self, n):
        if not self._data:
            raise BlockingIOError  # non-blocking socket with nothing ready
        chunk, self._data = self._data[:n], self._data[n:]
        return chunk


class _FakeSocket:
    def __init__(self, data):
        self._sock = _FakeRawSocket(data)


class _FakePlainStream:
    """A stand-in for tonio's `SocketStream`: `.socket._sock` is the raw socket and
    `close()` is synchronous."""

    def __init__(self, data):
        self.socket = _FakeSocket(data)
        self.closed = False

    def close(self):
        self.closed = True


def test_receive_nowait_tls_reads_decrypted_plaintext():
    # A TLSStream: peek the SSLObject's already-decrypted plaintext, not the socket.
    stream = _FakeTLSStream(b"decrypted")
    assert TonioBackend().receive_nowait(stream) == b"decrypted"


def test_receive_nowait_tls_empty_when_no_pending_plaintext():
    # No decrypted plaintext buffered -> "nothing ready" (never touches the socket).
    assert TonioBackend().receive_nowait(_FakeTLSStream(b"")) is None  # nothing ready (EOF is unknowable for TLS)


def test_receive_nowait_tls_never_touches_socket():
    # Regression: the old code did `transport.socket._sock.recv`, which AttributeErrors
    # on a TLSStream (no `.socket`). The TLS branch must not reach for `.socket`.
    stream = _FakeTLSStream(b"ok")
    assert not hasattr(stream, "socket")
    assert TonioBackend().receive_nowait(stream) == b"ok"  # no AttributeError


def test_receive_nowait_plain_socket_raw_recv():
    assert TonioBackend().receive_nowait(_FakePlainStream(b"raw bytes")) == b"raw bytes"


def test_receive_nowait_plain_socket_empty_when_would_block():
    # A non-blocking socket with nothing ready raises BlockingIOError -> b"".
    assert TonioBackend().receive_nowait(_FakePlainStream(b"")) is None  # would block -> None, distinct from EOF b""


def test_close_transport_plain_socket_calls_close():
    stream = _FakePlainStream(b"")
    TonioBackend().close_transport(stream)
    assert stream.closed


def test_close_transport_tls_closes_underlying_socket_synchronously():
    # The ABORTIVE end (hyper dropping the IO): a TLSStream's own close() is the
    # close_notify coroutine, which this end must NOT run; the underlying socket is
    # closed directly so the peer's read ends, with no alert on the wire.
    stream = _FakeTLSStream(b"")
    TonioBackend().close_transport(stream)
    assert stream.transport.closed  # underlying socket really closed
    assert not stream.close_coro_called  # the async close_notify coroutine untouched


@pytest.mark.tonio
async def test_shutdown_transport_plain_socket_calls_close():
    stream = _FakePlainStream(b"")
    await TonioBackend().shutdown_transport(stream)
    assert stream.closed


@pytest.mark.tonio
async def test_shutdown_transport_tls_awaits_close_notify():
    # The ORDERLY end (hyper `poll_shutdown` on the IO): the TLSStream's own close()
    # coroutine — close_notify, then the socket — is what runs.
    stream = _FakeTLSStream(b"")
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

    stream = _Broken(b"")
    await TonioBackend().shutdown_transport(stream)
    assert stream.close_coro_called


# ----- bounded_reader: the head-read deadline riding the read (no task, nothing cancelled) -----


class _Wake:
    """A `Waiter` stand-in: the runtime resuming a parked waiter — readiness or timer,
    both hand back None. A plain awaitable (not a coroutine), like tonio's `Waiter`:
    the readiness question hands one out that is never awaited."""

    def __await__(self):
        return iter(())


class _FakeArmSocket:
    """The `_Socket` surface the bounded reader drives: `_io_arm_r(timeout)` hands out a
    waiter (park) or None (readable now), `_io_clear_r()`, and the raw `_sock.recv`.
    `arms` scripts the arm answers in order (True = a waiter); `recvs` the syscall's
    (`BlockingIOError` = EAGAIN); `calls` records every timeout asked for."""

    def __init__(self, arms, recvs):
        self._arms = list(arms)
        self._recvs = list(recvs)
        self.calls = []
        self.cleared = 0
        self._sock = self

    def _io_arm_r(self, timeout=None):
        self.calls.append(timeout)
        return _Wake() if self._arms.pop(0) else None

    def _io_clear_r(self):
        self.cleared += 1

    def recv(self, n):
        ret = self._recvs.pop(0)
        if ret is BlockingIOError:
            raise BlockingIOError
        return ret


class _ArmStream:
    def __init__(self, sock):
        self.socket = sock


async def _bounded(sock, timeout=1.0):
    read = TonioBackend().bounded_reader(_ArmStream(sock))  # chosen once per connection
    return await read(100, _now() + timeout)


@pytest.mark.tonio
async def test_receive_bounded_readable_at_once_never_parks():
    sock = _FakeArmSocket(arms=[False], recvs=[b"head"])
    assert await _bounded(sock) == b"head"
    assert len(sock.calls) == 1  # bits were set: no wait


@pytest.mark.tonio
async def test_receive_bounded_timer_wake_is_told_by_the_readiness_word():
    # park -> wake -> the readiness question hands out a waiter: no bits, so the timer fired.
    sock = _FakeArmSocket(arms=[True, True], recvs=[])
    assert await _bounded(sock, 0.5) is None
    assert 499_000 < sock.calls[0] <= 500_000  # the arm carried the timer (micros; the clock ran a little)
    assert sock.calls[1] is None  # the question carries none
    assert sock.cleared == 0  # no syscall on the timer path


@pytest.mark.tonio
async def test_receive_bounded_readiness_wake_reads_bytes_first():
    sock = _FakeArmSocket(arms=[True, False], recvs=[b"head"])
    assert await _bounded(sock) == b"head"


@pytest.mark.tonio
async def test_receive_bounded_spurious_wake_rearms_with_the_remaining_time():
    # park -> wake -> readable? yes -> EAGAIN (a spurious wake) -> clear -> re-arm, then bytes.
    sock = _FakeArmSocket(arms=[True, False, True, False], recvs=[BlockingIOError, b"head"])
    assert await _bounded(sock, 0.5) == b"head"
    assert sock.cleared == 1
    first, again = sock.calls[0], sock.calls[2]
    assert 499_000 < first <= 500_000
    assert 0 < again <= first  # the deadline is absolute: never reset to the full timeout
