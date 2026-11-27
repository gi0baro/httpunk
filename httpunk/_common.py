"""Cross-protocol glue shared by the h1 and h2 public API + drivers.

Pure orchestration with **no protocol behavior** — the one place h1 and h2
(separate crates upstream, sharing nothing at the protocol level) legitimately
share code: body-chunk normalization, the builders of the connection errors both
protocols raise, and the public connection/server facade shells that collapse the
driver into an async-context-managed handle. Everything protocol-specific stays in
the h1/h2 role files.
"""

from __future__ import annotations

import ssl as _ssl
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Generic, TypeVar

from ._httpunk import (
    BodyWriteError,
    ConnectionClosedError,
    H1BodyError,
    H2BodyError,
    HTTPConnectionError,
    RequestNotSentError,
    TransportError,
    h1_io_kind,
)
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


async def aclose_body(body):
    """Deterministically close an async body once its pump is done with it — tonio
    finalizes nothing, so an async generator left unclosed never runs its cleanup."""
    aclose = getattr(body, "aclose", None)
    if aclose is not None:
        try:
            await aclose()
        except Exception:  # noqa: S110 - the app's cleanup failing must not mask the send outcome
            pass


_CLOSED = "connection closed"


def _detail(cause):
    """The text a message takes from what it was raised from: its own, or — empty, as
    tonio's `ResourceBroken` — its type's name."""
    return str(cause) or type(cause).__name__


def _errno(cause):
    if isinstance(cause, _ssl.SSLError):
        return None  # its errno is an OpenSSL library code, not an OS error number
    if isinstance(cause, (OSError, HTTPConnectionError)):
        return cause.errno
    return None


def _with_errno(exc, cause):
    errno = _errno(cause)
    if errno is not None:
        exc.errno = errno
    return exc


def transport_error(cause):
    """The `TransportError` for a transport operation that raised `cause`."""
    return _with_errno(TransportError(f"connection error: {_detail(cause)}"), cause)


def body_write_error(cause):
    return _with_errno(BodyWriteError(f"error writing a body to connection: {_detail(cause)}"), cause)


def h1_body_error(cause):
    """An HTTP/1 body read that failed with `cause`: `io_kind` is the kind Rust derives
    from its errno (hyper's `Body` over the `io::Error`), `other` without one."""
    errno = _errno(cause)
    exc = H1BodyError(h1_io_kind(errno), f"error reading a body from connection: {_detail(cause)}")
    if errno is not None:
        exc.errno = errno
    return exc


def h2_body_error(cause):
    return _with_errno(H2BodyError(f"error reading a body from connection: {_detail(cause)}"), cause)


# DIVERGENCE: hyper's `Kind::Canceled` reads "operation was canceled" and
# `Kind::ChannelClosed` "channel closed" (error.rs:539-540), Rust plumbing words;
# `RequestNotSentError` and `ConnectionClosedError` use httpunk's own wording
# (maintainer's decision).
def request_not_sent_error(cause=None):
    """The `RequestNotSentError` for a connection that could not take a request:
    `cause` is its stored error (or the local close), `None` when it just closed."""
    if cause is None:
        return RequestNotSentError(f"request not sent: {_CLOSED}")
    return _with_errno(RequestNotSentError(f"request not sent: {_detail(cause)}"), cause)


def connection_closed_error(cause=None):
    """The `ConnectionClosedError` for a closed connection; `cause` its stored error."""
    exc = ConnectionClosedError(_CLOSED)
    if cause is None:
        return exc
    return _with_errno(exc, cause)


class BaseClientConnection:
    """Shared public client-facade glue (h1/h2): async-context-manager entry/exit
    + the `request` wrapper over the protocol-specific `send_request`. Subclasses
    build `self._conn` (which exposes `connect`/`close`) and implement
    `send_request` + `ready`.

    `request(method, target, ...)` is a thin convenience over `send_request(Request)`;
    it takes the method explicitly (no per-verb helpers like `get()` — httpunk is
    low-level, and a single arbitrary shortcut would be inconsistent)."""

    async def __aenter__(self) -> Self:
        try:
            await self._conn.connect()
        except BaseException:
            # hyper client/conn/http1.rs:586, http2.rs:584: the handshake owns
            # the IO; failing or dropping it drops the IO without poll_shutdown.
            self._conn.abort()
            raise
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
        try:
            await self._conn.start()
        except BaseException:
            # h2 server.rs:1506-1525: the handshake owns the flushing codec/IO.
            self._conn.abort()
            raise
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
        """Signal a graceful shutdown (hyper's `Connection::graceful_shutdown`): h2
        sends GOAWAY and refuses new streams; h1 stops reusing the connection, and
        closes it at once when it is idle between requests (hyper `disable_keep_alive`
        on an idle connection) — the orderly transport shutdown is awaited here, since
        it is what ends the parked idle read. In-flight work finishes as the caller
        keeps driving the accept loop, which then ends and closes.
        `httpunk.util.GracefulShutdown` coordinates this over many connections."""
        return self._conn.graceful_shutdown()
