"""asyncio backend: the runtime primitives the drivers need, on `asyncio`.

The second backend (after tonio), validating that the drivers depend only on the
seam. `import asyncio` below is the stdlib (absolute imports) — this
module is `httpunk._backend.asyncio`, not the top-level package.

asyncio's TCP is transport/protocol-based and eagerly
drains the kernel socket into a userspace buffer, so — unlike tonio — a non-blocking
`receive_nowait` must peek *that* buffer. Rather than lean on `StreamReader`'s
private `_buffer`, we own a custom `asyncio.Protocol` (`_AsyncioStream`) that IS the
seam's stream: it buffers `data_received`, presents `receive_some`/`send_all`/`close`
/`read_nowait`, and manages its own read/write backpressure. This one class serves
both mode 1 (httpunk dials via `create_connection`) and — subclassed — Phase 6b's
reusable server protocols (`H1/H2/Auto` `ServerProtocol`), so keep it inert + subclassable:
a pure byte-mover + stream interface, no h1/h2 or driver logic.
"""

import asyncio
import collections as _collections
import errno as _errno
import ssl as _ssl
import time as _time

from ..exceptions import fresh_exc as _fresh_exc


_READ_HIGH_WATER = 2**16  # 64 KiB — pause reading past this (matches StreamReader's default limit)


class _AsyncioStream(asyncio.Protocol):
    """The dual-fed IO layer: an `asyncio.Protocol` that owns a byte buffer and
    presents the seam's stream interface. Fed by a socket (mode 1) or a host loop
    (mode 2). Protocol-agnostic and driver-less."""

    def __init__(self):
        self._loop = asyncio.get_running_loop()
        self._transport = None
        # Received `bytes` objects as they came from the socket, in order, plus the
        # unread offset into the first one. A reader that asks for at least a whole
        # object gets THAT object, no copy (BOUNDARY_NOTES S2); only a read smaller than
        # the object at the head copies out its slice. `_buffered` is the unread total
        # (the backpressure watermark).
        self._chunks = _collections.deque()
        self._head_off = 0
        self._buffered = 0
        self._eof = False
        self._error = None
        self._read_waiter = None  # Future parked in receive_some (single reader)
        self._reading_paused = False
        self._writing_paused = False
        self._drain_waiter = None  # Future parked in send_all under write backpressure
        self._conn_lost = False  # the socket died (connection_lost) — writes must now fail

    # ----- asyncio.Protocol callbacks (fed from the socket / host loop) -----

    def connection_made(self, transport):
        self._transport = transport

    def data_received(self, data):
        if not data:
            return
        if type(data) is not bytes:  # a host loop may feed a bytearray/memoryview
            data = bytes(data)
        self._chunks.append(data)
        self._buffered += len(data)
        self._wake_reader()
        if not self._reading_paused and self._buffered >= _READ_HIGH_WATER:
            self._transport.pause_reading()
            self._reading_paused = True

    def eof_received(self):
        self._eof = True
        self._wake_reader()
        # Keep the write half open — HTTP half-close: the peer closed its write side
        # (our read sees EOF) but we still send the response. NOTE (F33b): asyncio
        # honours this over plain TCP, but its SSL layer force-closes the transport on
        # an unexpected EOF (no close_notify) regardless of this return — so a TLS
        # half-close won't keep writing here the way tonio's does. That's an asyncio
        # SSL-runtime limitation, not something this layer can paper over faithfully.
        return True

    def connection_lost(self, exc):
        # Store the error PRISTINE (no traceback) and only ever raise copies of
        # it (below): this object outlives the failure, and a stored exception
        # instance re-raised through the caller's stack would accumulate those
        # frames onto its `__traceback__` — a refcount-invisible cycle pinning
        # everything the frames reference until a gen-2 GC (exceptions.fresh_exc).
        self._error = exc.with_traceback(None) if exc is not None else None
        self._eof = True
        self._conn_lost = True  # writes to a dead socket must now raise (F32)
        self._wake_reader()
        self._writing_paused = False
        if self._drain_waiter is not None and not self._drain_waiter.done():
            if exc is None:
                self._drain_waiter.set_result(None)
            else:
                self._drain_waiter.set_exception(_fresh_exc(exc))

    def pause_writing(self):
        self._writing_paused = True

    def resume_writing(self):
        self._writing_paused = False
        if self._drain_waiter is not None and not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
        self._drain_waiter = None

    # ----- the seam's stream interface (called by the drivers) -----

    async def receive_some(self, max_bytes=65536):
        """Up to `max_bytes` of the next available bytes; `b""` at EOF. Blocks only
        when the buffer is empty and no EOF/error has arrived yet."""
        if self._buffered:
            return self._take(max_bytes)
        if self._eof:
            if self._error is not None:
                # A fresh copy per raise — never the stored instance (see
                # connection_lost / exceptions.fresh_exc).
                raise _fresh_exc(self._error) from self._error
            return b""
        if self._read_waiter is not None:
            # Single-reader contract: a second concurrent receive_some would overwrite
            # the first's waiter, and the first caller would then park forever. The
            # drivers read one-at-a-time, so this is a bug — fail loudly rather than
            # hang silently (F55).
            raise RuntimeError("concurrent receive_some on one stream (single-reader contract)")
        self._read_waiter = self._loop.create_future()
        try:
            await self._read_waiter
        finally:
            self._read_waiter = None
        if self._buffered:
            return self._take(max_bytes)
        if self._error is not None:
            raise _fresh_exc(self._error) from self._error
        return b""

    async def receive_bounded(self, max_bytes, deadline):
        """`receive_some` bounded by `deadline` (an instant on the loop's clock): `None`
        once it passed with nothing buffered and no EOF. One `call_at` on the loop
        resolves the read future with a marker; data or EOF arriving first resolves it
        with None and the timer handle is cancelled — no task, no `wait_for`, nothing
        cancelled. Bytes first (hyper polls its timer after `parse`): a timer wake that
        finds bytes buffered returns them."""
        if self._buffered:
            return self._take(max_bytes)
        if self._eof:
            if self._error is not None:
                raise _fresh_exc(self._error) from self._error
            return b""
        if self._read_waiter is not None:
            raise RuntimeError("concurrent receive_some on one stream (single-reader contract)")
        waiter = self._read_waiter = self._loop.create_future()
        handle = self._loop.call_at(deadline, self._expire_reader, waiter)
        try:
            expired = await waiter
        finally:
            self._read_waiter = None
            handle.cancel()
        if self._buffered:
            return self._take(max_bytes)
        if self._error is not None:
            raise _fresh_exc(self._error) from self._error
        if self._eof:
            return b""
        return None if expired else b""

    @staticmethod
    def _expire_reader(waiter):
        if not waiter.done():
            waiter.set_result(True)

    async def send_all(self, data):
        # A write to a dead socket must fail (F32): asyncio's transport silently
        # DISCARDS writes after connection_lost, but tonio (like a raw socket) raises
        # EPIPE/ECONNRESET — drivers detect a dead peer via that failure. Surface the
        # real error, or EPIPE if the close carried none (a clean peer FIN).
        if self._conn_lost:
            if self._error is not None:
                raise _fresh_exc(self._error) from self._error
            raise BrokenPipeError(_errno.EPIPE, "connection lost")
        self._transport.write(data)
        if self._writing_paused:  # transport buffer over high-water — wait for resume (drain)
            if self._drain_waiter is None:
                self._drain_waiter = self._loop.create_future()
            await self._drain_waiter

    def close(self):
        """The ORDERLY end — hyper's `poll_shutdown` on the IO when its `Connection`
        future completes: over TLS asyncio's `close()` runs the `close_notify`
        exchange before closing the socket (tokio-rustls' `poll_shutdown`); over plain
        TCP it is a FIN. Pending writes are flushed first."""
        if self._transport is None:
            return
        self._transport.close()

    def abort(self):
        """The ABORTIVE end — hyper dropping the IO without `poll_shutdown` (an error
        out of the connection, or its future dropped): no `close_notify`, no flush of
        pending writes over TLS; plain TCP has nothing to skip, so it is a `close()`."""
        if self._transport is None:
            return
        if self._transport.get_extra_info("ssl_object") is not None:
            self._transport.abort()
        else:
            self._transport.close()

    def read_nowait(self, max_bytes=65536):
        """Synchronous non-blocking peek: whatever is buffered right now, `b""` once
        EOF arrived, else `None` (the `receive_nowait` primitive — approach B peeks
        *our* buffer)."""
        if self._buffered:
            return self._take(max_bytes)
        return b"" if self._eof else None

    # ----- helpers -----

    def _take(self, max_bytes):
        head = self._chunks[0]
        off = self._head_off
        if off == 0 and max_bytes >= len(head):
            data = head  # the received object itself
            self._chunks.popleft()
        else:
            end = min(off + max_bytes, len(head))
            data = head[off:end]
            if end == len(head):
                self._chunks.popleft()
                self._head_off = 0
            else:
                self._head_off = end
        self._buffered -= len(data)
        if self._reading_paused and self._buffered < _READ_HIGH_WATER:
            self._transport.resume_reading()
            self._reading_paused = False
        return data

    def _wake_reader(self):
        waiter = self._read_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)


class _AsyncioScope:
    """A nursery over `asyncio` tasks, matching tonio's scope surface: `spawn`,
    `cancel`, and `__aenter__`/`__aexit__` (which joins). Supports both lexical use
    and the detached h2 pattern (`__aenter__` in `_begin`, `__aexit__` in `close`).
    `asyncio.TaskGroup` can't do either (strictly lexical, no `cancel()`)."""

    __slots__ = ["_tasks"]

    def __init__(self):
        self._tasks = set()

    async def __aenter__(self):
        return self

    def spawn(self, coro):
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def cancel(self):
        for task in list(self._tasks):
            task.cancel()

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        # Join-only teardown, matching tonio's scope exactly: `__aexit__` never
        # cancels children on its own — a body exception still just JOINS the
        # in-flight children (tonio's `_exit` aborts them only when the explicit
        # `cancel()` flag is set). Callers that want children torn down on error
        # must call `cancel()` themselves (h2 `close()`, h1 client, graceful watch,
        # the h2 handler nursery in httpunk.asyncio all do). gather-and-swallow —
        # a spawned task routes its own failures elsewhere, so exceptions surfacing
        # here are teardown noise, not results.
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        return False


class _Event(asyncio.Event):
    """`asyncio.Event` with tonio's `wait(timeout)`: returns after the set OR the
    timeout, either way without a verdict — `is_set()` afterwards is the answer. Only
    for the one event a driver waits on under a deadline (`backend.timed_event()`: the
    h1 watcher hand-off, `await done.wait(remaining)` then `done.is_set()`); every
    other event is a plain `asyncio.Event`, with no wrapper on its `wait`."""

    async def wait(self, timeout=None):
        if timeout is None:
            return await super().wait()
        try:
            return await asyncio.wait_for(super().wait(), timeout)
        except (TimeoutError, asyncio.TimeoutError):
            return False


class _QueueSender:
    __slots__ = ["_q"]

    def __init__(self, q):
        self._q = q

    def send(self, item):
        self._q.put_nowait(item)


class _QueueReceiver:
    __slots__ = ["_q"]

    def __init__(self, q):
        self._q = q

    def receive(self):
        return self._q.get()


class AsyncioBackend:
    async def connect_tcp(self, host, port):
        loop = asyncio.get_running_loop()
        _transport, stream = await loop.create_connection(_AsyncioStream, host, port)
        return stream

    async def connect_tls(self, host, port, *, alpn=None, ssl_context=None):
        """Dial `host:port` over TLS and return `(stream, selected_alpn)` (see the
        tonio backend's `connect_tls`). `create_connection(ssl=...)` completes after
        the TLS handshake, so ALPN is readable from the transport on return."""
        if ssl_context is None:
            ssl_context = _ssl.create_default_context()
        if alpn:
            ssl_context.set_alpn_protocols(list(alpn))
        loop = asyncio.get_running_loop()
        transport, stream = await loop.create_connection(
            _AsyncioStream, host, port, ssl=ssl_context, server_hostname=host
        )
        ssl_obj = transport.get_extra_info("ssl_object")
        selected = ssl_obj.selected_alpn_protocol() if ssl_obj is not None else None
        return stream, selected

    async def wrap_tls(self, stream, *, server_hostname, alpn=None, ssl_context=None, prefix=b""):
        """TLS over a stream this backend already produced — `connect_tcp`'s, or the IO
        `H1Upgraded.downcast()` hands back (a CONNECT tunnel through a proxy to an
        `https` origin). Returns `(stream, selected_alpn)` like `connect_tls` (see the
        tonio twin). `loop.start_tls` swaps the socket's protocol for asyncio's
        `SSLProtocol`, runs the handshake, and hands back the TLS transport this SAME
        `_AsyncioStream` is now fed from and writes to — so the returned stream IS
        `stream`, re-pointed at it. `close`/`abort` already dispatch on the SSL object
        being there, and TLS over TLS (an `https://` proxy) works: sslproto cascades
        both ends through stacked transports.

        Handshake failure: sslproto force-closes the socket itself and the error
        (`ssl.SSLError`/`CertificateError`, or `ConnectionResetError` on an EOF mid-
        handshake) comes out of `start_tls` — nothing for the caller to clean up. A
        stream whose socket is already gone fails up front the way `send_all` does
        (F32): `start_tls` on a dead transport would otherwise sit in the handshake
        until its timeout, the ClientHello silently discarded.

        Runtime limitation (like F33b): `start_tls` has no seam between the protocol
        swap and the ClientHello, so bytes the peer sent BEFORE the handshake — `prefix`
        (the tunnel's `read_buf`), or bytes asyncio's eager drain already buffered on
        `stream` — cannot be fed to the SSL layer the way the tonio twin (hyper's
        `Rewind`) does. A TLS server never speaks before the ClientHello, so such bytes
        mean the peer is not one and the handshake would fail on them anyway: that
        failure is raised up front, as `ssl.SSLError`, with the stream aborted."""
        if ssl_context is None:
            ssl_context = _ssl.create_default_context()
        if alpn:
            ssl_context.set_alpn_protocols(list(alpn))
        if stream._eof:
            stream.abort()
            if stream._error is not None:
                raise _fresh_exc(stream._error) from stream._error
            raise ConnectionResetError(_errno.ECONNRESET, "connection closed before the TLS handshake")
        if prefix or stream._buffered:
            stream.abort()
            raise _ssl.SSLError("bytes received before the TLS handshake: the peer is not a TLS server")
        loop = asyncio.get_running_loop()
        tls_transport = await loop.start_tls(stream._transport, stream, ssl_context, server_hostname=server_hostname)
        stream.connection_made(tls_transport)
        ssl_obj = tls_transport.get_extra_info("ssl_object")
        selected = ssl_obj.selected_alpn_protocol() if ssl_obj is not None else None
        return stream, selected

    def receive_nowait(self, transport, max_bytes=65536):  # bytes | b"" (EOF) | None (nothing ready)
        """Synchronous non-blocking peek of the userspace buffer (approach B)."""
        return transport.read_nowait(max_bytes)

    def bounded_reader(self, transport):
        """Chosen once per connection (see the tonio twin): the seam's stream bounds its
        own read (`_AsyncioStream.receive_bounded`: one timer handle on the read future)."""
        return transport.receive_bounded

    def close_transport(self, transport):
        """The ABORTIVE close (sync): hyper dropping the IO without `poll_shutdown` —
        an error out of the connection (a transport failure, the header-read deadline,
        a body that errored mid-write, the peer gone mid-message), or its future
        dropped. Over TLS no `close_notify` is sent. See `shutdown_transport`."""
        transport.abort()

    async def shutdown_transport(self, transport):
        """The ORDERLY close: hyper's `Connection` future completing calls `poll_shutdown`
        on the IO (proto/h1/dispatch.rs `poll_inner`; h2's `codec.shutdown`), which over
        TLS sends `close_notify` before the socket closes. asyncio's `transport.close()`
        does exactly that (the SSL protocol runs the shutdown exchange, then closes)."""
        transport.close()

    def spawn_without_results(self, *coros):
        """Spawn task(s) NOW, discarding their results; returns the join handle
        (see the tonio backend's twin for the full contract: await-once by a
        single owner, no cancellation surface, coroutines route their own
        errors). One coro -> its Task; several -> a gather with
        `return_exceptions=True`, so the join waits for ALL of them exactly
        like tonio's barrier does."""
        if len(coros) == 1:
            return asyncio.ensure_future(coros[0])
        return asyncio.gather(*coros, return_exceptions=True)

    async def select(self, *coros):
        """Race `coros`; return the first-ready one's result, cancelling the losers.
        Matches tonio's `select` (`_ctl.select`: spawn all in argument order, keep the
        first stored outcome, then `scope.cancel()` the rest):

        - only the WINNER's outcome propagates — a loser's result or exception is
          discarded (tonio never fetches a loser's stored value);
        - the winner is the first coro in ARGUMENT ORDER among any ready in the same
          wakeup (asyncio.wait's `done` set is unordered — pick deterministically);
        - if `select` itself is cancelled, every racer is cancelled too (tonio's scope
          aborts its children on teardown) — no coro is orphaned.
        """
        tasks = [asyncio.ensure_future(c) for c in coros]
        try:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        winner = next(task for task in tasks if task in done)
        losers = [task for task in tasks if task is not winner]
        for task in losers:
            task.cancel()
        # Drain losers so their outcome is discarded (never surfaced here) and no
        # "exception never retrieved" fires; return_exceptions swallows their
        # error/cancellation. Losers that also finished this tick are drained too.
        if losers:
            await asyncio.gather(*losers, return_exceptions=True)
        return winner.result()

    def queue(self):
        """An unbounded queue as `(sender, receiver)`: `sender.send(x)` is sync;
        `await receiver.receive()` yields items in order."""
        q = asyncio.Queue()
        return _QueueSender(q), _QueueReceiver(q)

    # Abrupt-peer-teardown exceptions (see the tonio backend's twin attribute):
    # an RST surfaces as `ConnectionError` from `connection_lost`; a TLS close
    # without close_notify as `ssl.SSLEOFError` (asyncio's SSL layer raises it
    # on an unexpected EOF). The h1 server maps these at the request-head
    # boundary to a clean end-of-iteration (F47).
    broken_transport_errors = (ConnectionError, _ssl.SSLEOFError)

    # asyncio's Lock/Event/Semaphore already match the seam's neutral contract
    # (Event: set/wait/clear/is_set; Semaphore: async acquire / sync release — the
    # shape step 1 normalized the h1 slot to).
    lock = asyncio.Lock
    event = asyncio.Event
    timed_event = _Event  # the one event waited on under a deadline (the h1 watcher hand-off)
    semaphore = asyncio.Semaphore
    scope = _AsyncioScope
    monotonic = staticmethod(_time.monotonic)
    sleep = staticmethod(asyncio.sleep)  # async sleep(seconds), for deadline races (`select`)

    async def timeout(self, coro, seconds):
        """Run `coro` bounded by `seconds`; return `(result, True)` on completion, or
        `(None, False)` on timeout (the coro is cancelled). One task + one timer, far cheaper
        than racing a `sleep()` coroutine via `select`."""
        try:
            return await asyncio.wait_for(coro, seconds), True
        except (TimeoutError, asyncio.TimeoutError):
            return None, False
