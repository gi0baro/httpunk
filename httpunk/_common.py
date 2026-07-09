"""Cross-protocol glue shared by the h1 and h2 public API + drivers.

Pure orchestration with **no protocol behavior** — the one place h1 and h2
(separate crates upstream, sharing nothing at the protocol level) legitimately
share code: body-chunk normalization, and the public connection/server facade
shells that collapse the driver into an async-context-managed handle. Everything
protocol-specific stays in the h1/h2 role files.
"""

from .types import Request


async def aiter_body(body):
    """Normalize a message body — `bytes`, a sync iterable, or an async iterable
    of `bytes` — into an async stream of chunks. Protocol-neutral: the caller
    frames + sends each chunk (h1 via the codec, h2 via flow-controlled DATA).
    `None` (a bodyless message) is handled by the caller, not here."""
    if isinstance(body, (bytes, bytearray)):
        yield bytes(body)
    elif hasattr(body, "__aiter__"):
        async for chunk in body:
            yield chunk
    elif hasattr(body, "__iter__"):
        for chunk in body:
            yield chunk
    else:
        raise TypeError("body must be None, bytes, or an (async) iterable of bytes")


async def read_all(aiter):
    """Drain an async byte-chunk iterator into a single `bytes`."""
    return b"".join([chunk async for chunk in aiter])


class BaseClientConnection:
    """Shared public client-facade glue (h1/h2): async-context-manager entry/exit
    + the `request`/`get` wrappers over the protocol-specific `send_request`.
    Subclasses build `self._conn` (which exposes `connect`/`close`) and implement
    `send_request` + `ready`."""

    async def __aenter__(self):
        await self._conn.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._conn.close()
        return False

    async def request(self, method, target, *, headers=None, body=None):
        return await self.send_request(Request(method, target, headers=headers, body=body))

    async def get(self, target, *, headers=None):
        return await self.request("GET", target, headers=headers)


class BaseServer:
    """Shared public server-facade glue (h1/h2): async-context-manager entry/exit
    + the accept iterator over `self._conn.next_request()`. Subclasses build
    `self._conn` (which exposes `start`/`close`/`next_request`)."""

    async def __aenter__(self):
        await self._conn.start()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._conn.close()
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        request = await self._conn.next_request()
        if request is None:  # connection closed / can serve no more
            raise StopAsyncIteration
        return request

    async def accept(self):
        """Return the next incoming `ServerRequest`, or None once the connection
        can serve no more. (`async for` over the server is the ergonomic form.)"""
        return await self._conn.next_request()

    async def graceful_shutdown(self):
        """Signal a graceful shutdown (non-blocking, like hyper's
        `Connection::graceful_shutdown`): h2 sends GOAWAY and refuses new streams;
        h1 stops reusing the connection and releases an idle read. In-flight work
        finishes as the caller keeps driving the accept loop, which then ends and
        closes. `httpunk.util.GracefulShutdown` coordinates this over many
        connections (§11.3)."""
        await self._conn.graceful_shutdown()
