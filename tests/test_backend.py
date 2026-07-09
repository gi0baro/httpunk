"""TonioBackend glue. `receive_nowait` is a synchronous non-blocking peek used by
the h1 server's unread-body drain; it must read *decrypted plaintext* over TLS
(via the SSLObject), not the raw socket — a `TLSStream` carries ciphertext and
has no `.socket`, so the plain-socket path would be wrong (and would AttributeError)."""

from httpunk._backend.tonio import TonioBackend


class _FakeSSLObject:
    """Enough of `ssl.SSLObject` for `receive_nowait`: a plaintext buffer that
    `pending()` counts and `read(n)` drains — never touching a BIO/socket."""

    def __init__(self, plaintext):
        self._buf = plaintext

    def pending(self):
        return len(self._buf)

    def read(self, n):
        chunk, self._buf = self._buf[:n], self._buf[n:]
        return chunk


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
    assert TonioBackend().receive_nowait(_FakeTLSStream(b"")) == b""


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
    assert TonioBackend().receive_nowait(_FakePlainStream(b"")) == b""


def test_close_transport_plain_socket_calls_close():
    stream = _FakePlainStream(b"")
    TonioBackend().close_transport(stream)
    assert stream.closed


def test_close_transport_tls_closes_underlying_socket_synchronously():
    # Regression: a TLSStream's own close() is a coroutine (close_notify) the sync
    # close paths can't await; close the underlying socket instead so the peer's
    # read ends, and never invoke the (un-awaitable here) TLS close() coroutine.
    stream = _FakeTLSStream(b"")
    TonioBackend().close_transport(stream)
    assert stream.transport.closed  # underlying socket really closed
    assert not stream.close_coro_called  # the async close_notify coroutine untouched
