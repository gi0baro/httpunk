"""tonio backend: wraps the runtime primitives the h2 driver needs.

The driver only depends on this small surface — connect, a scope to hold the
background read-pump, a lock to order socket writes, and an event to signal
across coroutines — so a future asyncio/trio backend is a drop-in replacement.
"""

import ssl as _ssl

from tonio import colored as _colored
from tonio._net._tls import _is_eof  # private: the TLS clean-EOF test `TLSStream.receive_some` applies
from tonio.colored.net import SocketStream as _SocketStream, open_tcp_stream as _open_tcp_stream
from tonio.colored.net.tls import TLSStream as _TLSStream, open_tls_over_tcp_stream as _open_tls_over_tcp_stream
from tonio.colored.sync import Lock as _Lock, Semaphore as _Semaphore
from tonio.colored.sync.channel import unbounded as _unbounded
from tonio.colored.time import time as _now, timeout as _timeout
from tonio.exceptions import CancelledError, ResourceBroken


class _TonioSemaphore:
    """Adapts tonio's `Semaphore` to the backend-neutral async-`acquire`/sync-`release`
    contract. tonio's `acquire()` is sync and returns None (a permit was free) or an
    `Event` that fires once a permit is handed to this waiter — so awaiting that event
    *is* the acquisition (fair handoff, no retry)."""

    __slots__ = ["_sem"]

    def __init__(self, value):
        self._sem = _Semaphore(value)

    async def acquire(self):
        event = self._sem.acquire()
        if event is not None:
            try:
                await event.waiter(None)
            except CancelledError:
                if not event.set():
                    self._sem.release()
                raise

    def release(self):
        self._sem.release()


class TonioBackend:
    connect_tcp = staticmethod(_open_tcp_stream)

    async def connect_tls(self, host, port, *, alpn=None, ssl_context=None):
        """Dial `host:port` over TLS and return `(stream, selected_alpn)` — the
        one place TLS/ssl glue lives (behind the seam, like `receive_nowait`).

        `alpn` is the ordered ALPN offer (e.g. `("h2", "http/1.1")`); it is set on
        the context before the handshake and the peer's choice is read back from the
        completed handshake (`None` if the peer declined / offered no ALPN). tonio's
        `open_tls_over_tcp_stream` performs the TCP connect *and* the TLS handshake,
        so the returned stream is ready to carry HTTP — httpunk never handshakes
        itself. Used by `httpunk.util.connect` to pick h2 vs h1 (RFC 7301)."""
        if ssl_context is None:
            ssl_context = _ssl.create_default_context()
        if alpn:
            ssl_context.set_alpn_protocols(list(alpn))
        stream = await _open_tls_over_tcp_stream(host, port, ssl_context=ssl_context)
        return stream, stream._ssl.selected_alpn_protocol()

    async def wrap_tls(self, stream, *, server_hostname, alpn=None, ssl_context=None, prefix=b""):
        """TLS over a stream this backend already produced — `connect_tcp`'s, or the IO
        `H1Upgraded.downcast()` hands back (a CONNECT tunnel through a proxy to an
        `https` origin). Returns `(stream, selected_alpn)` exactly like `connect_tls`,
        and the result is accepted everywhere a `connect_tls` stream is. `prefix` is the
        tunnel's `read_buf`: bytes read past the CONNECT response that are the start of
        the TLS conversation (hyper wraps the `Rewind`ed `Upgraded`, so they are the
        first ciphertext) — written into the ingress BIO before the handshake (private
        `_SSLProxy` API, tonio and httpunk share an author).

        The TLS layer sits DIRECTLY on the `SocketStream` (the same stack `connect_tls`
        builds), so every dispatch in this backend holds — the plaintext peek, the
        bounded reader's raw read, the abortive close of the socket beneath. tonio's
        `TLSStream` is designed over a socket stream only: TLS over TLS (a tunnel
        through an `https://` proxy) is not supported here and is refused at setup.
        On a handshake failure the socket is closed before the error propagates
        (hyper: the failed connect drops the IO) — the caller has nothing to clean up.
        Failure exceptions are `connect_tls`'s (`ResourceBroken` from the ssl error)."""
        if not isinstance(stream, _SocketStream):
            raise TypeError(
                f"wrap_tls needs a tonio SocketStream, got {type(stream).__name__}: TLS over TLS "
                "(an https:// proxy tunnel) is outside tonio's TLSStream design"
            )
        if ssl_context is None:
            ssl_context = _ssl.create_default_context()
        if alpn:
            ssl_context.set_alpn_protocols(list(alpn))
        tls = _TLSStream(stream, ssl_context, server_hostname=server_hostname)
        if prefix:
            tls._ssl._ingress_write(prefix)
        try:
            await tls.handshake()
        except BaseException:
            stream.close()
            raise
        return tls, tls._ssl.selected_alpn_protocol()

    def receive_nowait(self, transport, max_bytes=65536):
        """A synchronous, non-blocking read: whatever bytes are immediately
        available without suspending, `b""` at EOF, or `None` if nothing is ready
        right now. The readiness primitive hyper's server drain (`poll_read_body`
        inside `poll_drain_or_close_read`) relies on. EOF and not-ready are distinct.

        - **Plain socket**: tonio's sockets are non-blocking under the hood (its own
          `recv` does exactly this `_sock.recv` inline before ever suspending), so
          we read the raw socket directly rather than route through the timer-backed
          `timeout(..., 0)`, which cannot express an instantaneous peek.
        - **TLS (`TLSStream`)**: the raw socket carries *ciphertext*, so reading it
          would bypass decryption (and a `TLSStream` has no `.socket` anyway). The
          non-blocking-plaintext equivalent is the SSLObject's already-decrypted
          buffer: `pending()` bytes can be `read()` without touching the BIO/socket.
          tonio's `_SSLProxy` serialises EVERY use of its `SSLObject` — the parked
          reader's `_read` included — under one `threading.Lock`; the peek takes that
          same lock (private API, tonio and httpunk share an author) so its
          pending-then-read is ONE step against a reader decrypting on another thread,
          never a `read()` of bytes the reader just consumed.
          (Necessarily conservative — tonio exposes no non-blocking "decrypt more",
          so unread ciphertext on the socket reads as "nothing ready"; the drain
          then closes rather than reuses, which matches hyper's cheap-drain-or-close.
          EOF is never reported for TLS either — it is only knowable by decrypting.)
        - A peek beside a parked plain-socket reader is safe: both are one `recv`
          syscall on the same non-blocking socket, the bytes go to exactly one of
          them, and the loser's `EAGAIN` sends it back to waiting (tonio `recv`)."""
        if isinstance(transport, _TLSStream):  # peek only already-decrypted plaintext
            ssl_obj = transport._ssl
            with ssl_obj._lock:  # tonio `_SSLProxy._lock`: the SSLObject's one lock
                pending = ssl_obj._inner.pending()
                return ssl_obj._inner.read(min(max_bytes, pending)) if pending else None
        try:
            return transport.socket._sock.recv(max_bytes)  # b"" only at EOF
        except (BlockingIOError, InterruptedError):
            return None

    def bounded_reader(self, transport):
        """Chosen ONCE per connection: how `transport`'s reads are bounded by a deadline —
        the h1 server's head-read deadline (hyper `header_read_timeout`) riding the read
        itself, so no task is spawned per head and no parked read is ever cancelled.
        Returns `async read(max_bytes, deadline) -> bytes | b"" (EOF) | None (expired)`;
        `deadline` is an instant on this backend's `monotonic` clock. The transport's kind
        is fixed for the connection's life, so the dispatch happens here, never per read,
        and the plain-socket reader is one coroutine over bound methods resolved here —
        the cost of tonio's own `receive_some`, plus one arm on the (cold) timer path.

        Bytes first, hyper's order (`poll_read_head` polls the timer only after `parse`
        returned Pending): a wake that finds bytes returns them, whatever the timer did.

        - **Plain socket**: the socket's own arm (`_io_arm_r(timeout)`) puts the readiness
          wait and the timer on ONE suspension (a `Waiter` with a timer). Both wakes resume
          with `None`; the readiness word tells them apart: a readiness wake set bits
          before waking (`set_readiness` precedes `wake`), a timer wake set none — so a
          bare `_io_arm_r()` after the wake asks "readable?": `None` = bits set (read now),
          a waiter = the timer fired. That waiter is never awaited: a stale reader slot,
          overwritten by the next arm, harmless by tonio's contract. One reader per socket,
          and `clear_r` runs only on this reader's failed syscall, so nothing consumes the
          bits in between. Every arm is computed from the absolute deadline, so a spurious
          readiness wake (`EAGAIN` after a wake) re-arms for the remaining time.
        - **TLS (`TLSStream`)**: `TLSStream.receive_some` is `_ssl_dance(_read)` with
          `_recv` feeding the ingress BIO from the raw socket; this is that dance with the
          raw read bounded (private API — tonio and httpunk share an author; keep in step
          with `tonio/_colored/_net/_tls.py`).
        A transport is one of tonio's two streams — a `TLSStream` or a `SocketStream` —
        and nothing else; anything else fails here, at setup, not on a read."""
        if isinstance(transport, _TLSStream):
            return self._tls_bounded_reader(transport)
        return self._socket_bounded_reader(transport.socket)

    @staticmethod
    def _socket_bounded_reader(sock):
        arm, clear, recv = sock._io_arm_r, sock._io_clear_r, sock._sock.recv

        async def read(max_bytes, deadline):
            while True:
                micros = round((deadline - _now()) * 1_000_000)
                waiter = arm(0 if micros < 0 else micros)
                if waiter is not None:
                    await waiter
                    if arm() is not None:  # no readiness bits: the timer woke us
                        return None
                try:
                    return recv(max_bytes)  # bytes first, whatever the timer did
                except InterruptedError:
                    continue
                except BlockingIOError:
                    clear()  # a spurious readiness wake: re-arm for the remaining time

        return read

    @staticmethod
    def _tls_bounded_reader(stream):
        ssl_obj = stream._ssl
        raw_read = TonioBackend._socket_bounded_reader(stream.transport.socket)

        async def read(max_bytes, deadline):
            stream._check_ready()
            try:
                while True:
                    try:
                        ret, want_read, to_send = ssl_obj._read(max_bytes)
                    except (_ssl.SSLError, _ssl.CertificateError) as exc:
                        stream._set_broken()
                        raise ResourceBroken from exc
                    if to_send:
                        await stream._send(to_send)
                    elif want_read:
                        recv_count = stream._recv_count
                        async with stream._lock_recv:
                            if recv_count == stream._recv_count:  # nobody fed the BIO meanwhile
                                data = await raw_read(65536, deadline)
                                if data is None:
                                    return None
                                if not data:
                                    ssl_obj._ingress_write_eof()
                                else:
                                    stream._recv_est_size = max(stream._recv_est_size, len(data))
                                    ssl_obj._ingress_write(data)
                                stream._recv_count += 1
                    if not want_read:
                        return ret
            except ResourceBroken as exc:
                if stream._compat_https and _is_eof(exc.__cause__):
                    return b""
                raise

        return read

    def close_transport(self, transport):
        """The ABORTIVE close (sync): hyper dropping the IO without `poll_shutdown` —
        an error out of the connection (a transport failure, the header-read deadline,
        a body that errored mid-write, the peer gone mid-message), or its future
        dropped. Sync, so the failure paths can commit it without a suspension.

        - **Plain socket**: `close()` — a FIN, or a RST if unread bytes are pending,
          exactly what dropping a `TcpStream` does.
        - **TLS (`TLSStream`)**: the underlying socket is closed directly, so no
          `close_notify` goes out — what dropping a tokio-rustls stream does. The peer
          sees a TLS EOF without close_notify (`ResourceBroken` on tonio).
        Either way a parked `receive_some` on the transport ends (tonio deregisters
        the fd before closing it, which wakes the reader; its retry fails on the closed
        socket)."""
        if isinstance(transport, _TLSStream):  # close the underlying socket
            transport.transport.close()
        else:
            transport.close()

    async def shutdown_transport(self, transport):
        """The ORDERLY close: hyper's `Connection` future completing calls `poll_shutdown`
        on the IO (proto/h1/dispatch.rs `poll_inner`; h2's `codec.shutdown` = flush then
        shutdown), which over TLS is tokio-rustls' `send_close_notify` + flush + socket
        shutdown. tonio's `TLSStream.close()` is that exchange (`unwrap` -> the alert is
        written -> the socket is closed, in a `finally`, so an interruption still closes
        it); a plain socket just closes. A write failure while sending the alert is
        swallowed: hyper surfaces it as `Kind::Shutdown` from the connection future,
        which httpunk's serve loop has already left — the socket is closed regardless."""
        if not isinstance(transport, _TLSStream):
            transport.close()
            return
        try:
            await transport.close()
        except OSError:  # the alert did not go out; the socket closed regardless
            pass

    # Exceptions `receive_some`/`send_all` raise when the peer tears the
    # transport down ABRUPTLY instead of a clean EOF: an RST on plain TCP
    # (`ConnectionError`: ConnectionResetError/BrokenPipeError), or a TLS close
    # without close_notify (tonio wraps the SSLEOFError in `ResourceBroken`) —
    # which httpunk's own abortive `close_transport` produces, so an httpunk
    # peer that drops a connection makes the other side see one. The h1 server
    # maps these at the request-head boundary to a clean end-of-iteration (F47).
    broken_transport_errors = (ConnectionError, ResourceBroken)

    # Spawn task(s) NOW, discarding their results; returns the join handle.
    # `await handle` resolves once ALL of them finished — exactly one owner
    # awaits it, exactly once (tonio's handle is Barrier-based one-shot). NO
    # cancellation surface, by design: this is the primitive for tasks that
    # must never be cancelled (flush/handoff obligations — the h2 write pump,
    # the h1 idle watcher); a group that needs teardown-with-cancel uses
    # `scope`. Spawned coroutines must not let exceptions escape (drivers
    # route errors into connection state); an escaped one surfaces at the
    # join in a backend-specific shape and is a driver bug, not API.
    spawn_without_results = staticmethod(_colored.spawn.without_results)

    select = staticmethod(_colored.select)
    scope = staticmethod(_colored.scope)
    lock = _Lock
    event = _colored.Event
    timed_event = _colored.Event  # tonio's `Event.wait` already takes a timeout: the same class
    semaphore = _TonioSemaphore
    queue = staticmethod(_unbounded)
    monotonic = staticmethod(_now)
    sleep = staticmethod(_colored.sleep)  # async sleep(seconds)
    # `timeout(coro, seconds) -> (result, completed)` — tonio's native deadline; the h1 server
    # uses it to bound the request-head read (measured ~12-16% faster than racing sleep() via
    # select). Same contract as AsyncioBackend.timeout.
    timeout = staticmethod(_timeout)
