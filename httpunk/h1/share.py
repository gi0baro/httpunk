"""HTTP/1 response body + upgraded-tunnel handle — the h1 backend for the
protocol-neutral `Response` (`httpunk/types.py`).

`H1ResponseBody` streams the body on demand through the Rust `H1BodyDecoder`,
pulling more transport bytes from the driver as needed. When the body is fully
read (or `aclose`d), it releases the connection's single in-flight slot — freeing
it for the next request on a keep-alive connection, or closing the connection
otherwise. Adapts the read side of hyper's `proto/h1/conn.rs` (`Reading::Body` ->
`KeepAlive` on a fully-read body, `conn.rs` L370-400) + `IncomingBody`; `aclose`
on a partially-read body maps to hyper marking the connection non-reusable (there
is no per-request reset in h1 — the connection itself is closed).

A 101 (Switching Protocols) response, or a 2xx to a CONNECT request, is an
*upgrade*: the connection stops being HTTP and becomes a raw tunnel. The body
then carries an `H1Upgraded` (surfaced as `Response.upgraded`) — the transport
handed off to the caller — mirroring hyper's `Upgraded` (`on_upgrade` /
`Connection::into_parts`).
"""


class H1Upgraded:
    """The raw connection after an HTTP/1 upgrade (101) or CONNECT tunnel — a
    byte stream the caller now owns and drives directly. Reads first drain any
    bytes already received past the response head (the start of the upgraded
    protocol), then read live from the transport.

    hyper: `hyper::upgrade::Upgraded` (the IO + the parser's leftover read buffer).
    """

    def __init__(self, transport, leftover):
        self._transport = transport
        self._leftover = bytes(leftover)  # bytes read past the head, not yet consumed
        self._closed = False

    async def receive_some(self, max_bytes=65536):
        """Read up to `max_bytes` of the upgraded protocol. Empty bytes = EOF."""
        if self._leftover:
            chunk, self._leftover = self._leftover[:max_bytes], self._leftover[max_bytes:]
            return chunk
        return await self._transport.receive_some(max_bytes)

    def send_all(self, data):
        return self._transport.send_all(data)

    async def aclose(self):
        if not self._closed:
            self._closed = True
            # The caller owns this raw tunnel and closes it from an async context, so
            # — unlike the driver's sync close paths — we can await a `TLSStream`'s
            # `close()` coroutine (the full `close_notify` dance); a plain socket's
            # `close()` returns None and needs no await.
            result = self._transport.close()
            if result is not None:
                await result

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.aclose()
        return False

    def __repr__(self):
        return f"H1Upgraded(buffered={len(self._leftover)})"


class H1ResponseBody:
    """The `Response` body backend for an HTTP/1 connection."""

    def __init__(self, driver, decoder, *, keep_alive, upgraded=None):
        self._driver = driver
        self._decoder = decoder
        self._keep_alive = keep_alive
        # The raw tunnel for a 101 / CONNECT upgrade, else None. When set there
        # is no HTTP body: the connection belongs to `upgraded` (see the driver).
        self.upgraded = upgraded
        self.trailers = None  # chunked trailers, populated once the body is read
        # An upgrade has already handed the transport to `upgraded`; there is no
        # slot to release and no body to read.
        self._released = upgraded is not None
        # A bodyless response (204, HEAD, CL: 0) has nothing to read, so its slot can
        # be freed at once — but freeing it now must also tear down the request-body
        # writer (`release_slot` is async since it may cancel/join that writer), which
        # `__init__` can't await. `send_request` (async) drives this eager finish.
        self._needs_eager_finish = upgraded is None and decoder.is_complete

    async def aiter_bytes(self):
        """Yield response body chunks as they arrive (decoded by `H1BodyDecoder`)."""
        if self.upgraded is not None:
            return  # an upgraded connection has no HTTP body — use `Response.upgraded`
        try:
            while True:
                chunk = self._decoder.decode()
                if chunk is not None:
                    yield chunk
                    continue
                if self._decoder.is_complete:
                    break
                data = await self._driver.read_body_more()
                if data:
                    self._decoder.feed(data)
                else:
                    self._decoder.mark_eof()  # transport closed; decoder ends or errors
        except BaseException:
            await self._release(keep_alive=False)  # broken body -> connection unusable
            raise
        # Chunked trailers (if any) are available once the body is fully decoded.
        self.trailers = self._decoder.take_trailers()
        await self._finish()

    async def _finish(self):
        """The body is fully decoded. hyper's client validates the read buffer is
        empty before reusing the connection (`require_empty_read` ->
        `new_unexpected_message`, conn.rs L463-465): any bytes the server sent
        past the response body are an HTTP/1 protocol violation (a server may not
        send anything before the next request). Poison the connection rather than
        silently dropping them and reusing a corrupted stream (was G35)."""
        if self._released:
            return
        leftover = self._decoder.take_buffered()
        if leftover:
            self._driver.poison_unexpected(len(leftover))
            await self._release(keep_alive=False)
        else:
            await self._release()

    async def aclose(self):
        """Release the connection. If the body wasn't fully read, the connection
        can't be safely reused (unread bytes remain), so it is closed. Safe to
        call more than once. (No-op for an upgraded response — the caller owns the
        tunnel via `Response.upgraded` and closes it there.)"""
        if not self._released:
            await self._release(keep_alive=False)

    async def _release(self, keep_alive=None):
        if self._released:
            return
        self._released = True
        await self._driver.release_slot(self._keep_alive if keep_alive is None else keep_alive)
