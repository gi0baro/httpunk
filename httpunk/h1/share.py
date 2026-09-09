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

from .._httpunk import OnceLatch


class _Spent:
    """What a downcast `H1Upgraded` reads from / writes to: nothing. hyper's `downcast`
    consumes the `Upgraded`; here the handle stays reachable, so using it is a loud
    error rather than a read on a transport someone else now owns."""

    __slots__ = []

    def receive_some(self, max_bytes=65536):
        raise RuntimeError("H1Upgraded spent: downcast() took its transport")

    def send_all(self, data):
        raise RuntimeError("H1Upgraded spent: downcast() took its transport")


_SPENT = _Spent()


class H1Upgraded:
    """The raw connection after an HTTP/1 upgrade (101) or CONNECT tunnel — a
    byte stream the caller now owns and drives directly. Reads first drain any
    bytes already received past the response head (the start of the upgraded
    protocol), then read live from the transport.

    hyper: `hyper::upgrade::Upgraded` (the IO + the parser's leftover read buffer).
    """

    def __init__(self, transport, leftover):
        self._transport = transport
        self._leftover = leftover  # `bytes` read past the head, not yet consumed
        self._close_latch = OnceLatch()  # `aclose` runs once, whichever task gets there first

    async def receive_some(self, max_bytes=65536):
        """Read up to `max_bytes` of the upgraded protocol. Empty bytes = EOF."""
        if self._leftover:
            chunk, self._leftover = self._leftover[:max_bytes], self._leftover[max_bytes:]
            return chunk
        return await self._transport.receive_some(max_bytes)

    def send_all(self, data):
        return self._transport.send_all(data)

    def downcast(self):
        """Take the IO back out of the handle: `(transport, read_buf)` — the backend's
        stream this tunnel rides, and the `bytes` read past the response head that
        no read consumed yet. hyper: `Upgraded::downcast` -> `Parts { io, read_buf }`.
        It consumes the handle: this `H1Upgraded` is spent afterwards (`aclose` is a
        no-op — the caller owns the transport now — and reads/writes raise). The
        seam's `wrap_tls(transport, prefix=read_buf, ...)` takes exactly these two to
        run an origin's TLS handshake inside a CONNECT tunnel."""
        if not self._close_latch.try_acquire():
            raise RuntimeError("H1Upgraded already closed or downcast")
        transport, self._transport = self._transport, _SPENT
        read_buf, self._leftover = self._leftover, b""
        return transport, read_buf

    async def aclose(self):
        if self._close_latch.try_acquire():
            # The caller owns this raw tunnel: the orderly end (hyper's `poll_shutdown`
            # on the `Upgraded` IO — `close_notify` over TLS). A tonio `TLSStream`'s
            # `close()` is a coroutine (the alert is written, then the socket closes);
            # a plain socket's, and asyncio's stream `close()`, return None.
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
        # One-shot latches (a cross-thread CAS, never blocking — safe from a GC
        # finalization context): the release runs exactly once, whichever of the two
        # release paths gets there first (a GC finalizer unwinding an abandoned body
        # generator vs. the user's aclose chain — both passing a plain check would
        # double-release the driver's single slot and put two exchanges on one
        # connection); the body is iterated by ONE consumer (hyper's `IncomingBody`
        # is owned). An upgrade has already handed the transport to `upgraded`: no
        # slot to release, no body to read — pre-latched.
        self._release_latch = OnceLatch()
        self._iter_latch = OnceLatch()
        if upgraded is not None:
            self._release_latch.try_acquire()
        # A bodyless response (204, HEAD, CL: 0) has nothing to read, so its slot can
        # be freed at once — but freeing it now must also tear down the request-body
        # writer (`release_slot` is async since it may cancel/join that writer), which
        # `__init__` can't await. `send_request` (async) drives this eager finish.
        self._needs_eager_finish = upgraded is None and decoder.is_complete

    @property
    def _released(self):
        return self._release_latch.is_set

    async def aiter_bytes(self):
        """Yield response body chunks as they arrive (decoded by `H1BodyDecoder`).
        Single-consumer: a second iteration while the first is still reading is
        refused (h2's `RecvStream` / hyper's `IncomingBody` are owned)."""
        if self.upgraded is not None:
            return  # an upgraded connection has no HTTP body — use `Response.upgraded`
        if not self._iter_latch.try_acquire():
            if self._released:
                return  # already fully read (or closed): nothing more to yield
            raise RuntimeError("response body is already being read")
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
        leftover = self._decoder.buffered
        if leftover:
            self._driver.poison_unexpected(leftover)
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
        # Atomic one-shot (the latch is a cross-thread CAS that never blocks): the
        # first caller wins; every later (or concurrent) caller no-ops.
        if not self._release_latch.try_acquire():
            return
        await self._driver.release_slot(self._keep_alive if keep_alive is None else keep_alive)
