"""tonio backend: wraps the runtime primitives the h2 driver needs.

The driver only depends on this small surface — connect, a scope to hold the
background read-pump, a lock to order socket writes, and an event to signal
across coroutines — so a future asyncio/trio backend is a drop-in replacement.
"""

import ssl as _ssl

from tonio import colored as _colored
from tonio.colored.net import open_tcp_stream as _open_tcp_stream
from tonio.colored.net.tls import open_tls_over_tcp_stream as _open_tls_over_tcp_stream
from tonio.colored.sync import Lock as _Lock, Semaphore as _Semaphore
from tonio.colored.sync.channel import unbounded as _unbounded
from tonio.colored.time import time as _now


class TonioBackend:
    async def connect_tcp(self, host, port):
        return await _open_tcp_stream(host, port)

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

    def receive_nowait(self, transport, max_bytes=65536):
        """A synchronous, non-blocking read: whatever bytes are immediately
        available without suspending, or `b""` if none are ready right now (also
        `b""` at EOF). The readiness primitive hyper's server drain (`poll_read_body`
        inside `poll_drain_or_close_read`) relies on.

        - **Plain socket**: tonio's sockets are non-blocking under the hood (its own
          `recv` does exactly this `_sock.recv` inline before ever suspending), so
          we read the raw socket directly rather than route through the timer-backed
          `timeout(..., 0)`, which cannot express an instantaneous peek.
        - **TLS (`TLSStream`)**: the raw socket carries *ciphertext*, so reading it
          would bypass decryption (and a `TLSStream` has no `.socket` anyway). The
          non-blocking-plaintext equivalent is the SSLObject's already-decrypted
          buffer: `pending()` bytes can be `read()` without touching the BIO/socket.
          (Necessarily conservative — tonio exposes no non-blocking "decrypt more",
          so unread ciphertext on the socket reads as "nothing ready"; the drain
          then closes rather than reuses, which matches hyper's cheap-drain-or-close.)"""
        ssl_obj = getattr(transport, "_ssl", None)
        if ssl_obj is not None:  # a TLSStream — peek only already-decrypted plaintext
            pending = ssl_obj.pending()
            return ssl_obj.read(min(max_bytes, pending)) if pending else b""
        try:
            return transport.socket._sock.recv(max_bytes)
        except BlockingIOError, InterruptedError:
            return b""

    def close_transport(self, transport):
        """Synchronously close a transport, unblocking the peer's read. The driver
        closes from both sync (body-release, failure) and async paths, so this must
        be sync.

        - **Plain socket**: its `close()` is synchronous.
        - **TLS (`TLSStream`)**: its own `close()` is a coroutine (it writes a TLS
          `close_notify`), which a sync caller can't await — so close the underlying
          socket (`.transport`) directly. The `close_notify` is skipped (a
          best-effort/abortive close, like hyper's hard close on a dropped
          connection), but the socket is really closed so the peer's read ends.
          (`H1Upgraded.aclose`, which is async and caller-owned, awaits the full
          `close_notify` dance instead.)"""
        ssl_obj = getattr(transport, "_ssl", None)
        if ssl_obj is not None:  # a TLSStream — close the underlying socket synchronously
            transport.transport.close()
        else:
            transport.close()

    async def select(self, *coros):
        """Race `coros`, returning the first to complete and cancelling the losers.
        The runtime's first-wins primitive — used by the h1 server to race an idle
        request-head read against a graceful-shutdown signal (a socket read parked in
        another task can only be released by cancellation, not by closing it)."""
        return await _colored.select(*coros)

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
