"""HTTP/1 connection driver — an *adaptation* of hyper's `proto/h1/conn.rs` +
`proto/h1/dispatch.rs` (the Client path) to a Python coroutine model, the same
way `h2/connection.py` adapts h2's `Connection` future. Only the genuinely-async
orchestration lives here; all byte work is the Rust sans-IO core (`H1Codec` head
parse/encode + body encode, `H1BodyDecoder` body decode over the vendored hyper
`Decoder`).

Unlike h2 (which multiplexes and needs a background read-pump), HTTP/1 is strictly
one request/response at a time, so there is no persistent pump. But hyper's
`Dispatcher` does *not* collapse to strict send-then-read: its `poll_loop`
interleaves reads and writes each turn (dispatch.rs L172-211), so a response head
can arrive while the request body is still being written. `send_request` therefore
writes head+body in a spawned task and reads the response head concurrently; if the
response arrives first (a server answering an upload early — 413/401/redirect), it
stops writing and surfaces the response instead of deadlocking against a full send
buffer. A single in-flight "slot" serializes requests (hyper's `Conn` is `busy`
while a message is in flight, `conn.rs` L196); the response body frees it when it
is fully read (or on `aclose`), reusing the connection on keep-alive or closing it
otherwise (hyper's `Reading::KeepAlive` vs `Reading::Closed`, `conn.rs` L370-400).

Faithfulness notes:
- Like hyper's `client::conn::http1`, this is a *low-level* connection API: the
  caller must supply a `Host` header (hyper documents "`req` must have a `Host`
  header", client/conn/http1.rs L192). We do NOT auto-add one — that (and
  connect/pool) is the downstream client's / `httpunk.util`'s job. (h2 differs:
  `:authority` is a protocol pseudo-header derived from the URI.)
- 1xx-informational responses (100 Continue, …) are skipped by the vendored
  `role::Client::parse` (role.rs L1013), so they never surface here.
- A 101 upgrade or a 2xx to CONNECT hands the raw transport to the caller as an
  `H1Upgraded` (`resp.upgraded`); the driver detaches and no longer owns/closes
  the transport (hyper `on_upgrade` / `Connection::into_parts`).
- `Expect: 100-continue` and the *request's* `Connection: close` are NOT gaps for
  the client path: hyper's client hard-codes `expect_continue: false` (role.rs
  L1161 — it never enters `Reading::Continue`) and derives connection reuse solely
  from the *response's* keep-alive (conn.rs L294; role.rs L1088), which is exactly
  what `H1ResponseBody`/`release_slot` do.

Cross-reference: hyperium/hyper 1.10.1 `src/proto/h1/{conn,dispatch,role}.rs` and
`src/client/conn/http1.rs`.
"""

from .._backend.tonio import TonioBackend
from .._httpunk import H1BodyDecoder, H1Codec
from ..exceptions import ConnectionClosedError
from ..http import HeaderMap
from ..types import Response
from .share import H1ResponseBody, H1Upgraded


_READ_SIZE = 65536


class Connection:
    """Drives one HTTP/1 connection over a caller-supplied transport (the h1
    analogue of `h2/connection.py`'s `Connection`)."""

    def __init__(self, transport, *, authority=None, backend=None):
        self.transport = transport
        self.authority = authority
        self.backend = backend or TonioBackend()
        self.error = None
        self._closed = False
        # Set once the connection is upgraded (101 / CONNECT): the transport is
        # handed to an `H1Upgraded` the caller owns, so we must not close it.
        self._upgraded = False
        # Remembers the last response's version (hyper `state.version`, conn.rs
        # L295): once a peer answers in HTTP/1.0, later requests on the reused
        # connection downgrade to 1.0 and re-assert keep-alive (enforce_version).
        self._peer_http10 = False
        # One request/response in flight at a time (h1 has no multiplexing).
        self._slot = self.backend.semaphore(1)

    async def connect(self):
        # HTTP/1 has no connection preface / handshake (unlike h2).
        pass

    async def close(self):
        self._closed = True
        # After an upgrade the transport belongs to the caller's `H1Upgraded`;
        # closing it here would tear their tunnel down.
        if self.transport is not None and not self._upgraded:
            self.transport.close()

    async def _acquire(self):
        event = self._slot.acquire()  # None if free, else an Event to await
        if event is not None:
            await event.waiter(None)

    async def wait_idle(self):
        """Resolve once the connection can accept a request (h1 analogue of
        `SendRequest::ready`); raise if it has failed or closed."""
        await self._acquire()
        self._slot.release()
        if self.error is not None:
            raise self.error
        if self._closed:
            raise ConnectionClosedError("connection closed")

    async def send_request(self, method, url, headers, body):
        # hyper: client/conn/http1.rs `SendRequest::send_request` (L213) — writes
        # the head+body, reads the response. The caller must have supplied `Host`
        # (L192); we do not add it (see the module note). `Conn` is busy for the
        # duration (conn.rs L196), modeled by the 1-permit slot.
        await self._acquire()
        if self._closed or self.error is not None:
            self._slot.release()
            raise self.error or ConnectionClosedError("connection closed")
        try:
            codec = H1Codec()
            content_length, chunked = self._body_framing(body)
            # If a previous response on this (reused) connection was HTTP/1.0,
            # downgrade this request to 1.0 and re-assert keep-alive — 1.0 defaults
            # to close, so hyper's `fix_keep_alive` injects `Connection: keep-alive`
            # (conn.rs L662-673) and `enforce_version` sets the version (L682-702).
            http10 = self._peer_http10
            if http10 and (headers is None or "connection" not in headers):
                headers = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
                headers.add("connection", "keep-alive")
            head = codec.serialize_request(
                method, url, headers, http10=http10, content_length=content_length, chunked=chunked
            )
            # hyper's `poll_loop` interleaves reads and writes (dispatch.rs
            # L172-211): the response head can arrive while the request body is
            # still being written. Writing the whole body *before* reading (a
            # server that answers early — 413/401/redirect/expect-reject — and
            # stops reading the upload) would deadlock against a full send buffer.
            # So we write head+body in a spawned task and read the head
            # concurrently; if the response arrives first, we stop writing.
            body_done = self.backend.event()
            write_error = []
            async with self.backend.scope() as s:
                s.spawn(self._write_request(codec, head, body, body_done, write_error))
                try:
                    resp_head = await self._read_head(codec, write_error)
                finally:
                    # Always stop the writer — via `finally` so a `_read_head`
                    # failure (e.g. a malformed head) doesn't leave the scope
                    # awaiting a writer still blocked on a full send buffer (the
                    # scope doesn't cancel children on an exception exit).
                    s.cancel()
            # Remember the peer's version so the next request on a reused
            # connection can fix itself up (hyper conn.rs L295).
            self._peer_http10 = resp_head.http10
            if resp_head.is_upgrade:
                # 101 Switching Protocols / 2xx to CONNECT: the connection stops
                # being HTTP. Hand the transport (plus any bytes already read past
                # the head — the start of the upgraded protocol) to an H1Upgraded
                # the caller owns; this driver won't touch the transport again
                # (hyper `on_upgrade` / `Connection::into_parts`).
                upgraded = H1Upgraded(self.transport, codec.take_body())
                self._detach()
                self._slot.release()
                body = H1ResponseBody(self, None, keep_alive=False, upgraded=upgraded)
                return Response(resp_head.status, resp_head.headers, body)
            fully_sent = body_done.is_set()
            decoder = H1BodyDecoder(resp_head.body_kind, resp_head.content_length or 0)
            decoder.feed(codec.take_body())  # body bytes already read alongside the head
            # If the request body wasn't fully sent (early response), the
            # connection has a truncated request on it and can't be reused,
            # regardless of the response's keep-alive signal.
            keep_alive = resp_head.keep_alive and fully_sent
            # A close-delimited body ends only at EOF: the server closes the
            # connection to signal the end, so it can never be reused even if the
            # keep-alive signal said otherwise (hyper reads to EOF + closes the
            # read side rather than offering a clean reuse, conn.rs L458-489).
            if resp_head.body_kind == "close":
                keep_alive = False
            # The response body owns the slot from here; it releases it when the
            # body is fully read (or on aclose). A bodyless response frees it
            # immediately.
            body = H1ResponseBody(self, decoder, keep_alive=keep_alive)
            return Response(resp_head.status, resp_head.headers, body)
        except BaseException as exc:
            self._fail(exc)
            self._slot.release()
            raise

    async def _write_request(self, codec, head, body, body_done, write_error):
        # Write the head then the framed body. A write failure (e.g. the server
        # closed the read side after answering early) must not mask a response
        # that did arrive: record it so `_read_head` can still deliver the head,
        # and only surface it if no response is forthcoming. Cancellation
        # (BaseException) propagates so the scope can unwind cleanly.
        try:
            await self.transport.send_all(head)
            await self._send_body(codec, body)
            body_done.set()
        except Exception as exc:  # transport write error, not cancellation
            write_error.append(exc)

    @staticmethod
    def _body_framing(body):
        # None / empty bytes -> no body framing at all (hyper's `set_length`
        # takes the `None` branch for an end-stream body: no Content-Length, no
        # Transfer-Encoding, role.rs L1311-1316). Non-empty bytes -> Content-
        # Length; (async) iterable -> chunked.
        if body is None:
            return None, False
        if isinstance(body, (bytes, bytearray)):
            return (len(body), False) if len(body) else (None, False)
        return None, True

    async def _send_body(self, codec, body):
        if body is None or codec.body_is_eof():
            # No body will be written (no framing, or a bodyless framing). hyper
            # never polls the body when the encoder is eof (conn.rs write_head),
            # so skip the iterable entirely — don't fire its side effects (G37).
            await self.transport.send_all(codec.serialize_end())
            return
        if isinstance(body, (bytes, bytearray)):
            await self.transport.send_all(codec.serialize_data(bytes(body)))
        elif hasattr(body, "__aiter__"):
            async for chunk in body:
                await self.transport.send_all(codec.serialize_data(bytes(chunk)))
        elif hasattr(body, "__iter__"):
            for chunk in body:
                await self.transport.send_all(codec.serialize_data(bytes(chunk)))
        else:
            raise TypeError("body must be None, bytes, or an (async) iterable of bytes")
        await self.transport.send_all(codec.serialize_end())

    async def _read_head(self, codec, write_error=None):
        # hyper: conn.rs `can_read_head` (L175) + `read_head` -> role.rs
        # `Client::parse` (L1013), which loops past 1xx informational responses.
        while True:
            data = await self.transport.receive_some(_READ_SIZE)
            if not data:
                # EOF before a full head. If the body write also failed (server
                # closed both directions), surface that as the cause.
                if write_error:
                    raise write_error[0]
                raise ConnectionClosedError("connection closed before the response head")
            head = codec.receive_head(data)
            if head is not None:
                return head

    async def read_body_more(self):
        """Read more transport bytes for the in-flight response body (used by
        `H1ResponseBody`). Empty bytes = EOF."""
        return await self.transport.receive_some(_READ_SIZE)

    def poison_unexpected(self, nbytes):
        """The server sent `nbytes` unsolicited bytes past the response body — an
        HTTP/1 protocol violation (a server may not send anything before the next
        request). hyper's client fails the connection here via `require_empty_read`
        -> `new_unexpected_message` (conn.rs L463-465); we record the error so the
        next `send_request`/`wait_idle` raises it (the slot release closes it)."""
        if self.error is None:
            self.error = ValueError(f"received {nbytes} unexpected bytes on an idle HTTP/1 connection")

    def release_slot(self, keep_alive):
        """Free the in-flight slot once a response is done. Reuse the connection if
        keep-alive (hyper: `Reading::KeepAlive` -> `Conn` back to `Init`/idle,
        conn.rs L378); otherwise it's unusable, so close it (`Reading::Closed`)."""
        if not keep_alive:
            self._closed = True
            if self.transport is not None and not self._upgraded:
                self.transport.close()
        self._slot.release()

    def _detach(self):
        """Relinquish the transport to a caller-owned `H1Upgraded` tunnel: no more
        requests, and neither `close` nor `_fail` may touch the transport again
        (hyper `Connection::into_parts`)."""
        self._closed = True
        self._upgraded = True
        self.transport = None

    def _fail(self, exc):
        if self.error is None and not isinstance(exc, ConnectionClosedError):
            self.error = exc
        self._closed = True
        if self.transport is not None and not self._upgraded:
            self.transport.close()
