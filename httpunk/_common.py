"""Cross-protocol glue shared by the h1 and h2 public API + drivers.

Pure orchestration with **no protocol behavior** — the one place h1 and h2
(separate crates upstream, sharing nothing at the protocol level) legitimately
share code: body-chunk normalization, and the public connection/server facade
shells that collapse the driver into an async-context-managed handle. Everything
protocol-specific stays in the h1/h2 role files.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Generic, TypeVar

from .types import Body, HeadersInput, Request, Response


if TYPE_CHECKING:
    from typing import Self


_RequestT = TypeVar("_RequestT")


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


class BodyInterruptedError(Exception):
    """Internal: the event raced against an async body's next chunk fired first
    (`iter_async_body_or_event`). Callers convert it to their protocol's error."""


_END = object()  # sentinel: the async body is exhausted
_FIRED = object()  # sentinel: the event won the race


async def _next_chunk(it):
    try:
        return await it.__anext__()
    except StopAsyncIteration:
        return _END


async def _await_event(evt):
    await evt.wait()
    return _FIRED


async def iter_async_body_or_event(body, evt, select):
    """Yield an ASYNC body's chunks, racing each `__anext__` against `evt`; raise
    `BodyInterruptedError` as soon as the event fires. hyper's shape for both protocols: the
    body-writing future re-checks the peer's abandonment on every poll while it waits
    for the next chunk (h2 `PipeToSendStream::poll` -> `poll_reset`; h1 the connection
    poll that runs `mid_message_detect_eof`), so a parked producer — SSE, long poll —
    fails fast instead of after it finally yields. Only an async body can park, so only it
    pays the per-chunk race; `bytes` / sync iterables are sent directly by the callers.
    The loser the race cancels is an Event wait or the app's own `__anext__` step — never
    one of our transport reads or writes (those happen outside the race)."""
    it = body.__aiter__()
    while True:
        if evt.is_set():
            raise BodyInterruptedError
        result = await select(_next_chunk(it), _await_event(evt))
        if result is _FIRED:
            raise BodyInterruptedError
        if result is _END:
            return
        yield result


class BaseClientConnection:
    """Shared public client-facade glue (h1/h2): async-context-manager entry/exit
    + the `request` wrapper over the protocol-specific `send_request`. Subclasses
    build `self._conn` (which exposes `connect`/`close`) and implement
    `send_request` + `ready`.

    `request(method, target, ...)` is a thin convenience over `send_request(Request)`;
    it takes the method explicitly (no per-verb helpers like `get()` — httpunk is
    low-level, and a single arbitrary shortcut would be inconsistent)."""

    async def __aenter__(self) -> Self:
        await self._conn.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        await self._conn.close()
        return False

    def request(
        self,
        method: str,
        target: str,
        *,
        headers: HeadersInput = None,
        body: Body = None,
        trailers: HeadersInput = None,
    ) -> Awaitable[Response]:
        return self.send_request(Request(method, target, headers=headers, body=body, trailers=trailers))


class BaseServer(Generic[_RequestT]):
    """Shared public server-facade glue (h1/h2): async-context-manager entry/exit
    + the accept iterator over `self._conn.next_request()`. Subclasses build
    `self._conn` (which exposes `start`/`close`/`next_request`)."""

    async def __aenter__(self) -> Self:
        await self._conn.start()
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        await self._conn.close()
        return False

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> _RequestT:
        request = await self._conn.next_request()
        if request is None:  # connection closed / can serve no more
            raise StopAsyncIteration
        return request

    def accept(self) -> Awaitable[_RequestT | None]:
        """Return the next incoming `ServerRequest`, or None once the connection
        can serve no more. (`async for` over the server is the ergonomic form.)"""
        return self._conn.next_request()

    def graceful_shutdown(self) -> Awaitable[None]:
        """Signal a graceful shutdown (non-blocking, like hyper's
        `Connection::graceful_shutdown`): h2 sends GOAWAY and refuses new streams;
        h1 stops reusing the connection and releases an idle read. In-flight work
        finishes as the caller keeps driving the accept loop, which then ends and
        closes. `httpunk.util.GracefulShutdown` coordinates this over many
        connections (§11.3)."""
        return self._conn.graceful_shutdown()
