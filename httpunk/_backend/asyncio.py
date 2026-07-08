"""asyncio backend: the runtime primitives the drivers need, on `asyncio`.

The second backend (after tonio), validating that the drivers depend only on the
seam (see PLAN §12). `import asyncio` below is the stdlib (absolute imports) — this
module is `httpunk._backend.asyncio`, not the top-level package.

Approach B (PLAN §12.4): asyncio's TCP is transport/protocol-based and eagerly
drains the kernel socket into a userspace buffer, so — unlike tonio — a non-blocking
`receive_nowait` must peek *that* buffer. Rather than lean on `StreamReader`'s
private `_buffer`, we own a custom `asyncio.Protocol` (`_AsyncioStream`) that IS the
seam's stream: it buffers `data_received`, presents `receive_some`/`send_all`/`close`
/`read_nowait`, and manages its own read/write backpressure. This one class serves
both mode 1 (httpunk dials via `create_connection`) and — subclassed — Phase 6b's
reusable server protocols (`H1/H2/AutoProtocol`), so keep it inert + subclassable:
a pure byte-mover + stream interface, no h1/h2 or driver logic.
"""

import asyncio
import ssl as _ssl
import time as _time


_READ_HIGH_WATER = 2**16  # 64 KiB — pause reading past this (matches StreamReader's default limit)


class _AsyncioStream(asyncio.Protocol):
    """The dual-fed IO layer: an `asyncio.Protocol` that owns a byte buffer and
    presents the seam's stream interface. Fed by a socket (mode 1) or a host loop
    (mode 2). Protocol-agnostic and driver-less."""

    def __init__(self):
        self._loop = asyncio.get_running_loop()
        self._transport = None
        self._buffer = bytearray()
        self._eof = False
        self._error = None
        self._read_waiter = None  # Future parked in receive_some (single reader)
        self._reading_paused = False
        self._writing_paused = False
        self._drain_waiter = None  # Future parked in send_all under write backpressure

    # ----- asyncio.Protocol callbacks (fed from the socket / host loop) -----

    def connection_made(self, transport):
        self._transport = transport

    def data_received(self, data):
        self._buffer += data
        self._wake_reader()
        if not self._reading_paused and len(self._buffer) >= _READ_HIGH_WATER:
            self._transport.pause_reading()
            self._reading_paused = True

    def eof_received(self):
        self._eof = True
        self._wake_reader()
        return True  # keep the write half open (HTTP half-close: still send the response)

    def connection_lost(self, exc):
        self._error = exc
        self._eof = True
        self._wake_reader()
        self._writing_paused = False
        if self._drain_waiter is not None and not self._drain_waiter.done():
            if exc is None:
                self._drain_waiter.set_result(None)
            else:
                self._drain_waiter.set_exception(exc)

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
        if self._buffer:
            return self._take(max_bytes)
        if self._eof:
            if self._error is not None:
                raise self._error
            return b""
        self._read_waiter = self._loop.create_future()
        try:
            await self._read_waiter
        finally:
            self._read_waiter = None
        if self._buffer:
            return self._take(max_bytes)
        if self._error is not None:
            raise self._error
        return b""

    async def send_all(self, data):
        self._transport.write(data)
        if self._writing_paused:  # transport buffer over high-water — wait for resume (drain)
            if self._drain_waiter is None:
                self._drain_waiter = self._loop.create_future()
            await self._drain_waiter

    def close(self):
        if self._transport is not None:
            self._transport.close()

    def read_nowait(self, max_bytes=65536):
        """Synchronous non-blocking peek: whatever is buffered right now, else `b""`
        (the `receive_nowait` primitive — approach B peeks *our* buffer)."""
        return self._take(max_bytes) if self._buffer else b""

    # ----- helpers -----

    def _take(self, max_bytes):
        if max_bytes >= len(self._buffer):
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:max_bytes])
            del self._buffer[:max_bytes]
        if self._reading_paused and len(self._buffer) < _READ_HIGH_WATER:
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
        if self._tasks:
            # Join (and swallow the CancelledError of any task cancel() cancelled) —
            # the read-pump routes its own failures through `_fail`, so exceptions
            # here are teardown noise, not results.
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
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

    def receive_nowait(self, transport, max_bytes=65536):
        """Synchronous non-blocking peek of the userspace buffer (approach B)."""
        return transport.read_nowait(max_bytes)

    def close_transport(self, transport):
        """Close the transport (sync). `transport.close()` covers TLS too (asyncio
        drives the `close_notify`), so no plain/TLS split like tonio."""
        transport.close()

    async def select(self, *coros):
        """Race `coros`; return the first result, cancelling the losers."""
        tasks = [asyncio.ensure_future(c) for c in coros]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass
        return next(iter(done)).result()

    def queue(self):
        """An unbounded queue as `(sender, receiver)`: `sender.send(x)` is sync;
        `await receiver.receive()` yields items in order."""
        q = asyncio.Queue()
        return _QueueSender(q), _QueueReceiver(q)

    # asyncio's Lock/Event/Semaphore already match the seam's neutral contract
    # (Event: set/wait/clear/is_set; Semaphore: async acquire / sync release — the
    # shape step 1 normalized the h1 slot to).
    lock = asyncio.Lock
    event = asyncio.Event
    semaphore = asyncio.Semaphore
    scope = _AsyncioScope
    monotonic = staticmethod(_time.monotonic)
