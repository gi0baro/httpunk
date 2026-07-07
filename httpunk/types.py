"""Shared, protocol-neutral message types — mirror the `http` crate.

These sit *above* the Rust codec boundary and are used by every connection type
— h2 now, h1 later — so a caller can treat h2 and h1 connections interchangeably.
Headers are the Rust-backed `httpunk.http.HeaderMap` (reused from the `http`
crate, not re-implemented here).

Cross-reference: the `http` crate's `Request` / `Response` / `HeaderMap`.
"""

from .http import HeaderMap


class Request:
    """A client request (≈ `http::Request<B>`).

    `target` is a path (becomes the `:path` pseudo-header) or an absolute URL;
    a connection resolves a bare path against its own authority. `headers` is
    normalized to a `HeaderMap` (accepts None, a mapping, an iterable of pairs,
    or a `HeaderMap`). `body` is None, `bytes`, or a (sync/async) iterable of `bytes`.
    """

    __slots__ = ("method", "target", "headers", "body")

    def __init__(self, method, target, *, headers=None, body=None):
        self.method = method
        self.target = target
        self.headers = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
        self.body = body

    def __repr__(self):
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

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers  # httpunk.http.HeaderMap
        self._body = body

    @property
    def trailers(self):
        """Trailing headers (a `HeaderMap`) delivered after the body, else None
        (h2 trailers frame / h1 chunked trailers). Available once the body is read."""
        return self._body.trailers

    @property
    def upgraded(self):
        """For an HTTP/1 101 / CONNECT upgrade, the raw tunnel (`H1Upgraded`) the
        caller now owns; None otherwise (including every HTTP/2 response)."""
        return self._body.upgraded

    @property
    def is_upgrade(self):
        return self._body.upgraded is not None

    async def aiter_bytes(self):
        """Yield body chunks as they arrive (decoded/flow-controlled by the backend)."""
        async for chunk in self._body.aiter_bytes():
            yield chunk

    async def read(self):
        return b"".join([chunk async for chunk in self._body.aiter_bytes()])

    async def aclose(self):
        """Release the response. If the body wasn't fully read, cancel it (the
        backend RSTs the stream on h2, or closes the connection on h1). Idempotent."""
        await self._body.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.aclose()
        return False

    def __repr__(self):
        if self._body.upgraded is not None:
            return f"Response(status={self.status}, upgraded)"
        return f"Response(status={self.status})"
