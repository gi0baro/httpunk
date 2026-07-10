"""Reusable asyncio protocols — embed httpunk in any asyncio program. They are
`asyncio.Protocol` subclasses of the asyncio backend's `_AsyncioStream` (the byte
transport the driver reads/writes); the host owns the loop and hands the protocol a
transport. Unlike uvicorn's h11/httptools protocols (HTTP/1 only), these also bring **HTTP/2**.

**Server** — subclass one and implement `handle(request)`, then hand it to
`loop.create_server(factory)`; the protocol drives httpunk's server over *itself* and calls
your `handle` per request:

    class MyServer(httpunk.asyncio.AutoServerProtocol):
        async def handle(self, request):
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

    server = await loop.create_server(MyServer, "0.0.0.0", 8000)
    async with server:
        await server.serve_forever()

**Client** (the mirror) — hand one to `loop.create_connection(factory)`; once connected,
`await proto.ready()` returns the httpunk client connection to send requests on:

    transport, proto = await loop.create_connection(
        lambda: httpunk.asyncio.H2ClientProtocol(authority="example.com:443", scheme="https"),
        "example.com", 443, ssl=ctx,
    )
    conn = await proto.ready()
    resp = await conn.request("GET", "/", headers={"host": "example.com"})
    await proto.aclose()

`H1*`/`H2*` force the protocol; `Auto*` picks h1-vs-h2 (server: from the client's opening
bytes; client: from the TLS ALPN result).
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, TypeVar

from ._backend.asyncio import AsyncioBackend, _AsyncioStream
from .h1.client import H1Connection
from .h1.server import H1Server
from .h2.client import H2Connection
from .h2.server import H2Server
from .util import auto


__all__ = [
    "AutoClientProtocol",
    "AutoServerProtocol",
    "H1ClientProtocol",
    "H1ServerProtocol",
    "H2ClientProtocol",
    "H2ServerProtocol",
    "ServerConnections",
]

_ProtocolT = TypeVar("_ProtocolT", bound="_ServerProtocol")


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
        try:
            self._server = await self._make_server()
        except auto.SniffCancelledError:
            # A graceful shutdown interrupted the preface sniff of a still-silent
            # connection (AutoServerProtocol) — there is nothing to serve, so close
            # promptly instead of lingering until the host's force-close timeout (F36).
            self.close()
            return
        await self._maybe_apply_graceful()  # a shutdown requested before the server existed
        async with self._server as server:
            if isinstance(server, H2Server):
                # HTTP/2 multiplexes — handle requests concurrently; the scope joins
                # in-flight handlers as the connection winds down.
                async with self._backend.scope() as handlers:
                    try:
                        async for request in server:
                            handlers.spawn(self._run_h2_handler(request))
                    except BaseException:
                        # Connection error or force-close (serve task cancelled):
                        # cancel in-flight handlers explicitly so a stuck one can't
                        # wedge the scope join. The scope is join-only (matching
                        # tonio), so this cancel() is what tears them down; a normal
                        # end-of-stream falls through and joins them instead.
                        handlers.cancel()
                        raise
            else:
                # HTTP/1 serves one request/response at a time (the driver won't
                # yield the next until this one is answered) — handle serially.
                async for request in server:
                    await self.handle(request)

    async def _run_h2_handler(self, request):
        # h2 handlers run concurrently in a join-only scope, which would otherwise
        # SWALLOW a handler exception silently — leaving the client's stream hanging
        # forever with no response (F34). Reset that one stream so the peer sees a
        # failure; the connection and its other streams keep running (h2 isolates a
        # service error to the stream, hyper `SendResponse` drop). A host that wants to
        # log handler errors should catch them inside its own `handle()`.
        try:
            await self.handle(request)
        except Exception:
            with contextlib.suppress(Exception):
                await request.reset()

    async def handle(self, request: Any) -> None:
        """Override: produce the response for `request` via `request.respond(...)`.
        This is where uvicorn/hypercorn bridge to ASGI."""
        raise NotImplementedError("subclass must implement `async def handle(self, request)`")

    # ----- host-coordinated graceful shutdown -----

    async def graceful_shutdown(self) -> None:
        """Signal a graceful shutdown of THIS connection (non-blocking): the driver
        stops accepting new requests (h2 GOAWAY / h1 disable-keep-alive), in-flight
        ones finish, then it closes. Idempotent. The host tracks its live protocols
        (via its `create_server` factory), calls this on each, then awaits
        `wait_closed()` with its own timeout — and `close()`-es any straggler.
        Requested before the driver exists (an `AutoServerProtocol` still sniffing) is
        remembered and applied once it is built."""
        self._graceful_requested = True
        await self._maybe_apply_graceful()

    async def wait_closed(self) -> None:
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

    def __init__(self):
        super().__init__()
        self._sniff_cancel = self._backend.event()  # fires to abort an in-progress sniff (F36)

    async def _make_server(self):
        return await auto.serve(self, backend=self._backend, cancel=self._sniff_cancel)

    async def graceful_shutdown(self) -> None:
        # Interrupt an in-progress preface sniff so a graceful shutdown of a
        # still-silent client doesn't linger (≈ hyper-util `ReadVersion::cancel`, F36).
        # Harmless once the server is built (nothing is waiting on the signal).
        self._sniff_cancel.set()
        await super().graceful_shutdown()


class ServerConnections:
    """Tracks live server-protocol connections for host-coordinated graceful
    shutdown. `track(ProtocolCls)` returns a `create_server` factory that registers
    each connection on open and deregisters it on close; `shutdown()` drains every
    live connection, force-closing any that don't within an optional timeout.

    Optional convenience — a host with its own connection tracking (e.g. uvicorn)
    would instead call the protocols' `graceful_shutdown()`/`wait_closed()`/`close()`
    directly. Usage:

        conns = ServerConnections()
        server = await loop.create_server(conns.track(MyProtocol), host, port)
        # ... on shutdown:
        server.close()                    # stop accepting new connections
        await conns.shutdown(timeout=30)  # drain in-flight, force-close stragglers
    """

    def __init__(self) -> None:
        self._live = set()

    def track(self, protocol_cls: type[_ProtocolT]) -> type[_ProtocolT]:
        """Return a `create_server` factory (a `protocol_cls` subclass) that keeps
        this registry's live set current — add on `connection_made`, discard when
        the connection's serve task finishes."""
        registry = self

        class _Tracked(protocol_cls):
            def connection_made(self, transport):
                super().connection_made(transport)  # sets _serve_task
                registry._live.add(self)
                self._serve_task.add_done_callback(lambda _t: registry._live.discard(self))

        return _Tracked  # type: ignore[return-value]  # a dynamic subclass of protocol_cls

    def count(self) -> int:
        """How many connections are currently live."""
        return len(self._live)

    async def shutdown(self, *, timeout: float | None = None) -> None:
        """Signal every live connection to shut down gracefully and await them to
        drain; past `timeout`, force-close the stragglers (transport close + cancel
        their serve task) so this always returns."""
        conns = list(self._live)
        if not conns:
            return
        # return_exceptions so ONE connection whose graceful_shutdown() raises can't
        # abort the whole shutdown — every other connection must still be drained and
        # the force-close path below must still run (F54).
        await asyncio.gather(*(c.graceful_shutdown() for c in conns), return_exceptions=True)
        waits = [asyncio.ensure_future(c.wait_closed()) for c in conns]
        _done, pending = await asyncio.wait(waits, timeout=timeout)
        if pending:
            for c in conns:
                c.close()  # unblock IO-bound waits
                if c._serve_task is not None:
                    c._serve_task.cancel()  # hard-cancel a stuck handler / accept loop
            await asyncio.wait(pending)


class _ClientProtocol(_AsyncioStream):
    """Base for the reusable client protocols — the client-side mirror of `_ServerProtocol`.
    `_AsyncioStream` is the transport the driver reads/writes; on `connection_made` this builds
    an httpunk client connection over `self` and spawns its handshake. `ready()` awaits the
    handshake and returns the connection facade to send requests on. Subclasses provide
    `_make_client`. There is no `handle` — the client is caller-driven, not push."""

    def __init__(self, *, authority: str | None = None, scheme: str | None = None) -> None:
        super().__init__()
        self._backend = AsyncioBackend()  # it's an asyncio protocol -> the asyncio backend
        self._authority = authority
        self._scheme = scheme
        # Built in connection_made; None until then (so aclose() before a dial is a no-op).
        self._client: H1Connection | H2Connection | None = None
        self._connect_task: asyncio.Task[object] | None = None

    def connection_made(self, transport):
        super().connection_made(transport)
        self._client = self._make_client()
        # Run the HTTP handshake (the facade's __aenter__ -> conn.connect(): preface + SETTINGS
        # for h2, a no-op for h1) concurrently; `ready()` awaits it. Retrieve the task's result
        # so a handshake failure doesn't surface as a bare "Task exception was never retrieved".
        task = self._loop.create_task(self._client.__aenter__())
        self._connect_task = task
        task.add_done_callback(lambda t: t.cancelled() or t.exception())

    def _make_client(self) -> H1Connection | H2Connection:
        raise NotImplementedError

    async def ready(self) -> H1Connection | H2Connection:
        """Await the connection handshake, then return the ready client facade
        (`H1Connection`/`H2Connection`) to send requests on."""
        assert self._connect_task is not None and self._client is not None  # set in connection_made
        await self._connect_task
        await self._client.ready()
        return self._client

    async def aclose(self) -> None:
        """Close the connection — drops the read/write scopes and the transport. A no-op if the
        connection was never established (`connection_made` not yet called)."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)


class H1ClientProtocol(_ClientProtocol):
    """Speak HTTP/1 on this connection."""

    def _make_client(self):
        return H1Connection(self, authority=self._authority, backend=self._backend)


class H2ClientProtocol(_ClientProtocol):
    """Speak HTTP/2 on this connection."""

    def _make_client(self):
        return H2Connection(self, authority=self._authority, scheme=self._scheme or "http", backend=self._backend)


class AutoClientProtocol(_ClientProtocol):
    """Pick HTTP/1 or HTTP/2 from the TLS ALPN result — the client-side analogue of
    `AutoServerProtocol`'s preface sniff. `h2` -> HTTP/2; anything else (including plain TCP
    with no ALPN) -> HTTP/1, matching `httpunk.util.connect`."""

    def _make_client(self):
        ssl_object = self._transport.get_extra_info("ssl_object")
        selected = ssl_object.selected_alpn_protocol() if ssl_object is not None else None
        if selected == "h2":
            return H2Connection(self, authority=self._authority, scheme=self._scheme or "https", backend=self._backend)
        return H1Connection(self, authority=self._authority, backend=self._backend)
