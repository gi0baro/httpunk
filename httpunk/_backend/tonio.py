"""tonio backend: wraps the runtime primitives the h2 driver needs.

The driver only depends on this small surface — connect, a scope to hold the
background read-pump, a lock to order socket writes, and an event to signal
across coroutines — so a future asyncio/trio backend is a drop-in replacement.
"""

from tonio import colored as _colored
from tonio.colored.net import open_tcp_stream as _open_tcp_stream
from tonio.colored.sync import Lock as _Lock, Semaphore as _Semaphore
from tonio.colored.sync.channel import unbounded as _unbounded
from tonio.colored.time import time as _now


class TonioBackend:
    async def connect_tcp(self, host, port):
        return await _open_tcp_stream(host, port)

    def receive_nowait(self, transport, max_bytes=65536):
        """A synchronous, non-blocking read: the bytes already sitting in the
        socket buffer, or `b""` if none are ready right now (also `b""` at EOF) —
        it never suspends. This is the readiness primitive hyper's server drain
        (`poll_read_body` inside `poll_drain_or_close_read`) relies on: tonio's
        sockets are non-blocking under the hood (its own `recv` does exactly this
        `_sock.recv` inline before ever suspending), so we read the raw socket
        directly rather than route through the timer-backed `timeout(..., 0)`,
        which cannot express an instantaneous peek."""
        try:
            return transport.socket._sock.recv(max_bytes)
        except BlockingIOError, InterruptedError:
            return b""

    def scope(self):
        return _colored.scope()

    def lock(self):
        return _Lock()

    def event(self):
        return _colored.Event()

    def semaphore(self, value):
        return _Semaphore(value)

    def queue(self):
        """An unbounded queue: `(sender, receiver)`. `sender.send(x)` is sync;
        `await receiver.receive()` yields items in order."""
        return _unbounded()

    def monotonic(self):
        """The runtime clock, in seconds. Used to age out the reset-stream store
        (h2 uses `Instant::now`)."""
        return _now()
