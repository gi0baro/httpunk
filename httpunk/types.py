"""Shared, protocol-neutral message types — mirror the `http` crate.

These sit *above* the Rust codec boundary and are used by every connection type
— h2 now, h1 later — so a caller can treat h2 and h1 connections interchangeably.
Headers are the Rust-backed `httpunk.http.HeaderMap` (reused from the `http`
crate, not re-implemented here).

Cross-reference: the `http` crate's `Request` / `Response` / `HeaderMap`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from .http import HeaderMap


if TYPE_CHECKING:
    from .h1.share import H1Upgraded


# Public annotation aliases, reused across the client/server facades.
HeadersInput = HeaderMap | Mapping[str, str] | Iterable[tuple[str, str]] | None
Body = bytes | Iterable[bytes] | AsyncIterator[bytes] | None


class Request:
    """A client request (≈ `http::Request<B>`).

    `target` is a path (becomes the `:path` pseudo-header) or an absolute URL;
    a connection resolves a bare path against its own authority. `headers` is
    normalized to a `HeaderMap` (accepts None, a mapping, an iterable of pairs,
    or a `HeaderMap`). `body` is None, `bytes`, or a (sync/async) iterable of `bytes`.

    `trailers` (optional) are header fields sent AFTER the body — chunked trailers on
    HTTP/1, a trailing HEADERS frame on HTTP/2. Static (a `HeaderMap`/mapping, known
    before sending); setting them forces a chunked body on HTTP/1 and auto-declares the
    fields in the `Trailer` request header. Trailers whose value depends on the streamed
    content are not expressible with this static field.
    """

    __slots__ = ("method", "target", "headers", "body", "trailers")

    def __init__(
        self,
        method: str,
        target: str,
        *,
        headers: HeadersInput = None,
        body: Body = None,
        trailers: HeadersInput = None,
    ) -> None:
        self.method = method
        self.target = target
        self.headers = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
        self.body = body
        self.trailers = (
            None if trailers is None else (trailers if isinstance(trailers, HeaderMap) else HeaderMap(trailers))
        )

    def __repr__(self) -> str:
        return f"Request(method={self.method!r}, target={self.target!r})"


class Response:
    """A client response (≈ `http::Response<Incoming>`), protocol-neutral.

    Status + headers + a lazily-streamed body, so a caller treats h2 and h1
    responses identically. The body is backed by a protocol-specific stream
    (`h2/share.py`'s `H2ResponseBody` or `h1/share.py`'s `H1ResponseBody`) that
    this shell drives; `aclose()` is a neutral "cancel" the backend interprets
    (h2 -> RST_STREAM(CANCEL); h1 -> close the connection, as h1 has no per-request
    reset). Use as an async context manager to guarantee release.
    """

    def __init__(self, status: int, headers: HeaderMap, body: Any) -> None:
        self.status = status
        self.headers = headers  # httpunk.http.HeaderMap
        self._body = body

    @property
    def trailers(self) -> HeaderMap | None:
        """Trailing headers (a `HeaderMap`) delivered after the body, else None
        (h2 trailers frame / h1 chunked trailers). Available once the body is read."""
        return self._body.trailers

    @property
    def upgraded(self) -> H1Upgraded | None:
        """For an HTTP/1 101 / CONNECT upgrade, the raw tunnel (`H1Upgraded`) the
        caller now owns; None otherwise (including every HTTP/2 response)."""
        return self._body.upgraded

    @property
    def is_upgrade(self) -> bool:
        return self._body.upgraded is not None

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        """Yield body chunks as they arrive (decoded/flow-controlled by the backend)."""
        async for chunk in self._body.aiter_bytes():
            yield chunk

    async def read(self) -> bytes:
        return b"".join([chunk async for chunk in self._body.aiter_bytes()])

    def aclose(self) -> Awaitable[None]:
        """Release the response. If the body wasn't fully read, cancel it (the
        backend RSTs the stream on h2, or closes the connection on h1). Idempotent."""
        return self._body.aclose()

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        await self.aclose()
        return False

    def __repr__(self) -> str:
        if self._body.upgraded is not None:
            return f"Response(status={self.status}, upgraded)"
        return f"Response(status={self.status})"
