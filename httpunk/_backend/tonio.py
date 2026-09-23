"""tonio backend: wraps the runtime primitives the h2 driver needs.

The driver only depends on this small surface — connect, a scope to hold the
background read-pump, a lock to order socket writes, and an event to signal
across coroutines — so a future asyncio/trio backend is a drop-in replacement.
"""

import ssl as _ssl

from tonio import Waiter as _Waiter, colored as _colored
from tonio._tonio import get_runtime as _get_runtime  # private: the runtime's µs clock (`_clock`), see `bounded_reader`
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
        """A synchronous, non-blocking read: whatever bytes are immediately available
        without suspending, `b""` at EOF, or `None` if nothing is ready right now. The
        readiness primitive hyper's server drain (`poll_read_body` inside
        `poll_drain_or_close_read`) relies on. EOF and not-ready are distinct.

        tonio's own no-wait read on both streams (`receive_some_nowait`: `NotReady` is
        this seam's `None`). On a plain socket it is one `recv` (EAGAIN clears the
        readiness bits, as tonio's `recv` does). Over TLS it decrypts what the socket
        already holds — feeds the ingress BIO without suspending — the way hyper's read
        over rustls does, so the server's drain finds a small unread body instead of
        closing; a close_notify already in the socket is `b""`, an abrupt close raises
        `ResourceBroken` (a `broken_transport_errors` shape, like the plain socket's
        `ConnectionResetError`). Beside a parked reader: on a plain socket both are one
        `recv` on the same non-blocking socket and the bytes go to exactly one of them;
        over TLS the parked reader holds the receive lock, so the peek answers `None`
        whatever the socket holds — hyper's `require_empty_read` looks at its `read_buf`
        the same way, and ciphertext not yet decrypted is invisible to it too."""
        data = transport.receive_some_nowait(max_bytes)
        return None if data is transport.NotReady else data

    def bounded_reader(self, transport):
        """Chosen ONCE per connection: how `transport`'s reads are bounded by a deadline —
        the h1 server's head-read deadline (hyper `header_read_timeout`) riding the read
        itself, so no task is spawned per head and no parked read is ever cancelled.
        Returns `async read(max_bytes, deadline) -> bytes | b"" (EOF) | None (expired)`;
        `deadline` is an instant on this backend's `monotonic` clock (seconds, the seam's
        unit). The transport's kind is fixed for the connection's life, so the dispatch
        happens here, never per read, over bound methods resolved here.

        Both readers are tonio's own `wait_readable(timeout)` loop inlined over the
        stream's no-wait read and its readiness waiter — the same primitives, without the
        coroutine frame per park and without the seconds -> µs -> seconds round trip: the
        deadline is converted once per read, and each park compares it with the runtime's
        µs clock (`_clock`, read off the runtime cached at bind time rather than through
        `get_runtime()` on every pass; private API — tonio and httpunk share an author).

        Bytes first, hyper's order (`poll_read_head`: `parse` -> `poll_read`, and the timer
        is polled only once that returned Pending): every pass tries the read before it
        looks at the clock, so a wake that finds bytes returns them, whatever the timer
        did. The readiness bits are stale-set after any successful read (nothing but a
        failed syscall clears them), exactly as tokio's readiness word is when hyper's
        `poll_read` runs, so both pay one `EAGAIN` probe per head; only on `NotReady`
        (which cleared the bits) is the remaining time computed. Past the deadline that is
        the answer at once — hyper's Pending followed by a ready timer poll
        (`new_header_timeout`) — and no zero-length timer is ever armed. Otherwise the
        waiter puts the readiness wait and the timer on ONE suspension, and after the wake
        the readiness question is asked BEFORE the next read: `None` = the bits are set
        (read now), a waiter = none are, so the timer fired (a stale reader slot, overwritten
        by the next arm, harmless by tonio's contract). That question is not optional: the
        socket's `_io_clear_r` is tick-guarded — it clears the bits only if no readiness
        edge landed since an arm last SAW them set — so a wake followed straight by a read
        leaves the edge unobserved, the next head's `EAGAIN` clear is ignored, and that head
        pays a second syscall (measured: 3 reads and 2 arms per request instead of 2 and 1).
        Every arm is computed from the absolute deadline, so a wake that finds nothing to
        read re-arms for what is left.

        - **Plain socket**: `waiter_readable(µs)` is the socket's own arm: `None` when the
          bits landed between the probe and the arm (read now), else the waiter; bare, it
          is the readiness question.
        - **TLS (`TLSStream`)**: `watch_readable()` holds the receive lock across the arm
          and the park (as tonio's `wait_readable` does); its waiter is `None` when
          plaintext is already decoded, the raw socket's arm otherwise, and the lock's own
          hand-off when a parked reader holds it; `ready()` is the question (plaintext
          pending, or the socket's bare arm). A "not ready" wake loops back to the clock
          rather than returning: after a lock hand-off nothing was armed, so it is not a
          verdict there (that hand-off never happens in httpunk — one reader per connection
          — but the loop stays exact). `receive_some_nowait` decrypts.
        A transport is one of tonio's two streams — a `TLSStream` or a `SocketStream` —
        and nothing else; anything else fails here, at setup, not on a read."""
        runtime = _get_runtime()  # one runtime per process, bound once per connection
        if isinstance(transport, _TLSStream):
            return self._tls_bounded_reader(transport, runtime)
        return self._socket_bounded_reader(transport, runtime)

    @staticmethod
    def _socket_bounded_reader(stream, runtime):
        nowait, arm, not_ready = stream.receive_some_nowait, stream.waiter_readable, stream.NotReady

        async def read(max_bytes, deadline):
            deadline_micros = int(deadline * 1_000_000)  # the seam's seconds -> the runtime clock's µs, once per read
            while (data := nowait(max_bytes)) is not_ready:  # bytes first; EAGAIN clears the bits
                remaining = deadline_micros - runtime._clock
                if remaining <= 0:
                    return None  # nothing readable and the deadline passed: hyper's timer poll is ready
                if (waiter := arm(remaining)) is not None:  # None: bits landed between the probe and the arm
                    await waiter
                    if arm() is not None:  # the readiness question: no bits = the timer woke us
                        return None
            return data

        return read

    @staticmethod
    def _tls_bounded_reader(stream, runtime):
        nowait, watch, not_ready = stream.receive_some_nowait, stream.watch_readable, stream.NotReady

        async def read(max_bytes, deadline):
            deadline_micros = int(deadline * 1_000_000)
            while (data := nowait(max_bytes)) is not_ready:
                remaining = deadline_micros - runtime._clock
                if remaining <= 0:
                    return None
                with watch() as watcher:  # the receive lock is held across the park, as tonio's own wait
                    if (waiter := watcher.waiter(remaining)) is not None:
                        await waiter
                        if not watcher.ready():  # the readiness question: nothing readable, back to the clock
                            continue
            return data

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
    # `await select_events(*events)`: resume once ANY of the events is set — ONE suspension
    # of the calling task (tonio's merged waiter), no wrapper tasks, no scope, nothing to
    # cancel; returns no verdict, the caller reads the events' flags. The race of "pump
    # done" against "peer gone" in `_send_async_body` (h1 and h2): hyper's
    # `PipeToSendStream` polling `poll_reset` beside the body future, in one task.
    select_events = staticmethod(_Waiter.any)
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
