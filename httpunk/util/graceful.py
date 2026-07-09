"""Graceful shutdown coordinator — `httpunk.util`'s analogue of hyper-util's
`server::graceful::GracefulShutdown`.

hyper-util wraps a *connection future* and, when a shutdown signal fires, calls the
connection's non-blocking `graceful_shutdown()` once, then keeps driving that same
future to completion; `shutdown()` sends the signal and waits until every watched
connection has finished (its watch-channel receiver dropped).

We keep the same shape. Because httpunk's servers are driven by a pull loop
(`async for req in server`) rather than a baked-in `Service`, the connection future
and the `graceful_shutdown()` handle are two things, so `watch` takes both: the
`server` (signalled on shutdown) and `serve` (the coroutine that drives it). Only
the public `graceful_shutdown()` is touched — no driver internals.

    graceful = GracefulShutdown()

    async def serve(server):
        async with server:                 # closes when the loop ends
            async for req in server:
                await handle(req)

    async with scope() as nursery:
        async for transport in listener:
            server = await util.auto.serve(transport)
            nursery.spawn(graceful.watch(server, serve))
        # on a shutdown signal:
        await graceful.shutdown()
"""

import threading

from .. import _backend


class GracefulShutdown:
    def __init__(self, backend=None):
        self._backend = _backend.resolve(backend)
        self._signal = self._backend.event()  # the shutdown signal (≈ hyper-util's watch channel)
        self._lock = threading.Lock()  # guards the live-connection count (free-threaded)
        self._live = 0
        self._all_done = self._backend.event()
        self._all_done.set()  # no connections watched yet

    def count(self):
        """Number of connections currently being watched (≈ `receiver_count`)."""
        return self._live

    async def watch(self, server, serve):
        """Drive `serve(server)` — the connection future — to completion. If
        `shutdown()` is signalled while it runs, `server.graceful_shutdown()` is
        called once (non-blocking) so in-flight work finishes and new work is
        refused; the connection then completes on its own and the count drops."""
        with self._lock:
            self._live += 1
            self._all_done.clear()
        try:
            async with self._backend.scope() as inner:

                async def _trigger():
                    await self._signal.wait()
                    await server.graceful_shutdown()

                inner.spawn(_trigger())
                try:
                    await serve(server)  # the connection future — driven to completion
                finally:
                    # Stop watching the signal however serve() ended. MUST be in a
                    # `finally`: if serve() raises (a broken transport is routine),
                    # skipping this leaves `_trigger` parked on the signal forever —
                    # the scope join then hangs (tonio) and `_live` never drops. The
                    # explicit cancel() is honored identically on both backends.
                    inner.cancel()
        finally:
            with self._lock:
                self._live -= 1
                if self._live == 0:
                    self._all_done.set()

    async def shutdown(self):
        """Signal every watched connection to shut down gracefully and wait until
        they have all finished their in-flight work and closed."""
        self._signal.set()
        await self._all_done.wait()
