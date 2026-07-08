"""Reusable asyncio server protocols — embed httpunk in any asyncio server (uvicorn,
hypercorn, …) by subclassing one of these and implementing `handle(request)`.

Unlike uvicorn's h11/httptools protocols (HTTP/1 only), these also bring **HTTP/2**.
They are `asyncio.Protocol` subclasses of the asyncio backend's `_AsyncioStream`
(approach B, PLAN §12.8): the host owns the loop and hands the protocol a transport
via `loop.create_server(factory)`; the protocol drives httpunk's server over *itself*
and calls your `handle` per request.

    class MyServer(httpunk.asyncio.AutoProtocol):
        async def handle(self, request):
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

    server = await loop.create_server(MyServer, "0.0.0.0", 8000)
    async with server:
        await server.serve_forever()

`H1Protocol` / `H2Protocol` force the protocol; `AutoProtocol` sniffs h1-vs-h2 from
the client's opening bytes.
"""

from ._backend.asyncio import AsyncioBackend, _AsyncioStream
from .h1.server import H1Server
from .h2.server import H2Server
from .util import auto


__all__ = ["AutoProtocol", "H1Protocol", "H2Protocol"]


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

    def connection_made(self, transport):
        super().connection_made(transport)
        self._serve_task = self._loop.create_task(self._serve())
        # Retrieve the task's result so a handler error doesn't surface as a bare
        # "Task exception was never retrieved" (an embedder handles errors in `handle`).
        self._serve_task.add_done_callback(lambda t: t.cancelled() or t.exception())

    async def _make_server(self):
        raise NotImplementedError

    async def _serve(self):
        server = await self._make_server()
        async with server:
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


class H1Protocol(_ServerProtocol):
    """Serve every connection as HTTP/1."""

    async def _make_server(self):
        return H1Server(self, backend=self._backend)


class H2Protocol(_ServerProtocol):
    """Serve every connection as HTTP/2."""

    async def _make_server(self):
        return H2Server(self, backend=self._backend)


class AutoProtocol(_ServerProtocol):
    """Serve each connection as HTTP/1 or HTTP/2, sniffed from the client preface."""

    async def _make_server(self):
        return await auto.serve(self, backend=self._backend)
