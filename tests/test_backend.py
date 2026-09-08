"""TonioBackend glue. `receive_nowait` is a synchronous non-blocking peek used by
the h1 server's unread-body drain; it must read *decrypted plaintext* over TLS
(via the SSLObject), not the raw socket — a `TLSStream` carries ciphertext and
has no `.socket`, so the plain-socket path would be wrong (and would AttributeError)."""

import threading

import pytest

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


class _FakeTLSStream:
    """A stand-in for tonio's `TLSStream`: exposes `._ssl`, the underlying
    `.transport` socket, and — deliberately — NO `.socket`, so a raw-socket peek
    would AttributeError. Its own `close()` is a coroutine (the TLS `close_notify`
    dance), which a sync caller must NOT invoke."""

    def __init__(self, plaintext):
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
