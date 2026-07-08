"""Reusable asyncio server protocols — embed httpunk in any asyncio server (uvicorn,
hypercorn, …) by subclassing one of these and implementing `handle(request)`.

Unlike uvicorn's h11/httptools protocols (HTTP/1 only), these also bring **HTTP/2**.
They are `asyncio.Protocol` subclasses of the asyncio backend's `_AsyncioStream`
(approach B, PLAN §12.8): the host owns the loop and hands the protocol a transport
via `loop.create_server(factory)`; the protocol drives httpunk's server over *itself*
and calls your `handle` per request.

    class MyServer(httpunk.asyncio.AutoServerProtocol):
        async def handle(self, request):
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

    server = await loop.create_server(MyServer, "0.0.0.0", 8000)
    async with server:
        await server.serve_forever()

`H1ServerProtocol` / `H2ServerProtocol` force the protocol; `AutoServerProtocol` sniffs h1-vs-h2 from
the client's opening bytes.
"""

import asyncio

from ._backend.asyncio import AsyncioBackend, _AsyncioStream
from .h1.server import H1Server
from .h2.server import H2Server
from .util import auto


__all__ = ["AutoServerProtocol", "H1ServerProtocol", "H2ServerProtocol"]


class _ServerProtocol(_AsyncioStream):
    """Base for the reusable server protocols. `_AsyncioStream` provides the byte
    plumbing (it *is* the transport the driver reads/writes); on `connection_made`
    this spawns a task that runs the httpunk server driver over `self` and calls
    `handle(request)` per request. Subclasses provide `_make_server`; embedders
    provide `handle`. Kept minimal — no h1/h2 assumptions beyond `_make_server`."""

    def __init__(self):
        super().__init__()
        self._backend = AsyncioBackend()  # it's an asyncio protocol -> the asyncio backend
        self._serve_task = None
        self._server = None  # the httpunk server driver, once _serve builds it
        self._graceful_requested = False
        self._graceful_applied = False

    def connection_made(self, transport):
        super().connection_made(transport)
        self._serve_task = self._loop.create_task(self._serve())
        # Retrieve the task's result so a handler error doesn't surface as a bare
        # "Task exception was never retrieved" (an embedder handles errors in `handle`).
        self._serve_task.add_done_callback(lambda t: t.cancelled() or t.exception())

    async def _make_server(self):
        raise NotImplementedError

    async def _serve(self):
        self._server = await self._make_server()
        await self._maybe_apply_graceful()  # a shutdown requested before the server existed
        async with self._server as server:
            if isinstance(server, H2Server):
                # HTTP/2 multiplexes — handle requests concurrently; the scope joins
                # in-flight handlers as the connection winds down.
                async with self._backend.scope() as handlers:
                    async for request in server:
                        handlers.spawn(self.handle(request))
            else:
                # HTTP/1 serves one request/response at a time (the driver won't
                # yield the next until this one is answered) — handle serially.
                async for request in server:
                    await self.handle(request)

    async def handle(self, request):
        """Override: produce the response for `request` via `request.respond(...)`.
        This is where uvicorn/hypercorn bridge to ASGI."""
        raise NotImplementedError("subclass must implement `async def handle(self, request)`")

    # ----- host-coordinated graceful shutdown -----

    async def graceful_shutdown(self):
        """Signal a graceful shutdown of THIS connection (non-blocking): the driver
        stops accepting new requests (h2 GOAWAY / h1 disable-keep-alive), in-flight
        ones finish, then it closes. Idempotent. The host tracks its live protocols
        (via its `create_server` factory), calls this on each, then awaits
        `wait_closed()` with its own timeout — and `close()`-es any straggler.
        Requested before the driver exists (an `AutoServerProtocol` still sniffing) is
        remembered and applied once it is built."""
        self._graceful_requested = True
        await self._maybe_apply_graceful()

    async def wait_closed(self):
        """Wait until this connection's serve loop has finished (drained + closed).
        Does not re-raise a handler/serve error — the connection is simply done."""
        if self._serve_task is not None:
            await asyncio.wait({self._serve_task})

    async def _maybe_apply_graceful(self):
        # Apply exactly once, whether graceful_shutdown() or _serve() gets here first.
        # Setting the guard before the await (no yield between) keeps it single.
        if self._server is None or not self._graceful_requested or self._graceful_applied:
            return
        self._graceful_applied = True
        await self._server.graceful_shutdown()


class H1ServerProtocol(_ServerProtocol):
    """Serve every connection as HTTP/1."""

    async def _make_server(self):
        return H1Server(self, backend=self._backend)


class H2ServerProtocol(_ServerProtocol):
    """Serve every connection as HTTP/2."""

    async def _make_server(self):
        return H2Server(self, backend=self._backend)


class AutoServerProtocol(_ServerProtocol):
    """Serve each connection as HTTP/1 or HTTP/2, sniffed from the client preface."""

    async def _make_server(self):
        return await auto.serve(self, backend=self._backend)
