"""HTTP/1 server — the accepting-side analogue of the h1 client
(`connection.py` + `client.py`), and the h1 counterpart of `h2/server.py`.

The sans-IO byte work is all Rust over the vendored hyper core: `H1Codec.
receive_request_head` (head parse via `Server::parse`) + `serialize_response`
(head encode via `Server::encode`, incl. the `Date` header) + the body `Encoder`,
and `H1BodyDecoder` for the request body. Only the orchestration is Python.

HTTP/1 is strictly one request/response at a time (no multiplexing), so — like
the h1 client — there is no background pump: `next_request` reads a request head
inline, hands back a `ServerRequest`, and the app reads the request body and
sends a response on the same connection. On a keep-alive connection the loop
reads the next request; otherwise the connection closes.

Low-level by design (like `hyper::server::conn::http1`): one connection over a
caller-supplied, already-accepted transport; accepting the socket, TLS/ALPN, and
the accept loop are the caller's job. Usage:

    async with H1Server(transport) as server:
        async for request in server:
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

Cross-reference: hyper 1.10.1 `proto/h1/{role,conn,dispatch}.rs` (server path) and
`client/conn`/`server/conn`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any

from .._common import BaseServer, read_all
from .._httpunk import H1BodyDecoder, H1Codec
from ..exceptions import ConnectionClosedError
from ..http import HeaderMap
from .connection import H1ConnectionBase
from .share import H1Upgraded


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Body, HeadersInput


_READ_SIZE = 65536
_SHUTDOWN = object()  # sentinel: a graceful shutdown released an idle head-read
# The interim response hyper's server auto-sends for `Expect: 100-continue`
# (byte-identical to hyper conn.rs L413).
_CONTINUE = b"HTTP/1.1 100 Continue\r\n\r\n"
# Max bytes of an incomplete request head we'll buffer before rejecting it as
# `Parse::TooLarge` (auto 431 + close) — hyper's `DEFAULT_MAX_BUFFER_SIZE`
# (io.rs: 8192 + 4096*100). Bounds per-connection memory against a slow/oversized head.
_MAX_HEAD_SIZE = 8192 + 4096 * 100
_DEFAULT_HEADER_READ_TIMEOUT = 30.0  # hyper's `header_read_timeout` default (http1.rs L249)
# The HTTP/2 connection preface (client prior-knowledge). An h1 server that sees it
# closes silently with a version error rather than writing a 400 (hyper `on_parse_error`
# -> `has_h2_prefix` -> `new_version_h2`, conn.rs L29/L809-812).
_H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


def _connection_has(headers, token):
    """True if the `Connection` header (any value, comma-split, case-insensitive)
    carries `token` — e.g. a response `Connection: close`."""
    if headers is None:
        return False
    for value in headers.get_all("connection"):
        for part in bytes(value).decode("latin-1").split(","):
            if part.strip().lower() == token:
                return True
    return False


def _error_status(message):
    """Map a request-head parse error to hyper's automatic status (`Server::
    on_error`, role.rs L466-484): URI too long -> 414, headers too large -> 431,
    everything else (method/header/uri/version) -> 400."""
    if "UriTooLong" in message:
        return 414
    if "TooLarge" in message:
        return 431
    return 400


class ServerRequest:
    """An incoming request + the handle to respond to it (hyper: the request +
    its `Sender`/response channel). One in flight at a time."""

    method: str
    target: str  # the request-target (origin/absolute/authority form)
    path: str  # alias of target
    headers: HeaderMap
    trailers: HeaderMap | None  # chunked trailers, populated once the body is read
    keep_alive: bool
    is_upgrade: bool
    content_length: int | None  # declared request Content-Length (None if chunked)
    upgraded: H1Upgraded | None  # the raw tunnel once a CONNECT/Upgrade is answered

    def __init__(
        self,
        conn,
        *,
        method,
        target,
        headers,
        decoder,
        keep_alive,
        expect_continue,
        is_upgrade,
        http10,
        content_length,
    ):
        self.method = method  # str
        self.target = target  # str — the request-target (origin/absolute/authority form)
        self.path = target  # alias
        self.headers = headers  # httpunk.http.HeaderMap
        self.trailers = None  # chunked trailers, populated once the body is read
        self.keep_alive = keep_alive
        self.is_upgrade = is_upgrade
        self.content_length = content_length  # declared request Content-Length (None if chunked)
        # The raw tunnel once the app answers a CONNECT/Upgrade with a 101 or a 2xx
        # to CONNECT (hyper `on_upgrade`): the caller owns it and drives it directly.
        self.upgraded = None
        self._conn = conn
        self._decoder = decoder
        self._http10 = http10
        self._expect_continue = expect_continue
        self._continue_sent = False
        self._body_done = decoder.is_complete
        self._responded = False

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        """Yield request body chunks, pulling transport bytes on demand (decoded
        by `H1BodyDecoder`). Sends `100 Continue` first if the client asked for it."""
        if self._decoder.is_complete:
            return
        if self._expect_continue and not self._continue_sent and not self._responded and not self._http10:
            # The client is waiting for a 1xx before sending the body (RFC 9110
            # §10.1.1). hyper auto-sends 100 Continue when the body is first polled,
            # but ONLY while `Writing::Init` (before any response) and ONLY for versions
            # > HTTP/1.0 (conn.rs L409-415, L311). Sending it after `respond()` began
            # (F15) would be parsed as the next response; sending it to a 1.0 client
            # (F16) violates the spec hyper deliberately follows.
            self._continue_sent = True
            await self._conn.write(_CONTINUE)
        try:
            while True:
                chunk = self._decoder.decode()
                if chunk is not None:
                    yield chunk
                    continue
                if self._decoder.is_complete:
                    break
                data = await self._conn.read_body_more()
                if data:
                    self._decoder.feed(data)
                else:
                    self._decoder.mark_eof()  # client closed mid-body
        except BaseException:
            self._conn.mark_unusable()
            raise
        self.trailers = self._decoder.take_trailers()
        self._body_done = True

    def read(self) -> Awaitable[bytes]:
        return read_all(self.aiter_bytes())

    async def respond(self, status: int, *, headers: HeadersInput = None, body: Body = None) -> None:
        """Send the response head (+ body). `body` is None, `bytes`, or a
        (sync/async) iterable of `bytes`. hyper: `Server::encode`."""
        if self._responded:
            raise RuntimeError("response already sent for this request")
        self._responded = True
        await self._conn.send_response(self, status, headers, body)

    def detach(self) -> bytes:
        """Take over the raw connection for a protocol upgrade (WebSocket, or a custom protocol):
        stop the server's accept loop and relinquish the transport WITHOUT closing it, returning
        any bytes already read past the request head (to replay). The caller — which supplied the
        transport to `H1Server` — owns it afterwards and must drive the upgrade itself (httpunk
        sends no response). Like httpunk's 101/CONNECT tunnel hand-off, but caller-driven.

        After this, the accept loop ends (`next_request` returns None) and the server's close on
        `__aexit__` is a no-op, so the transport stays open. cf. Go's `http.Hijacker`."""
        if self._responded:
            raise RuntimeError("cannot detach: a response was already sent for this request")
        self._responded = True
        leftover = self._decoder.take_buffered()
        self._conn._detach()
        return leftover

    def __repr__(self) -> str:
        return f"ServerRequest(method={self.method!r}, target={self.target!r})"


class ServerConnection(H1ConnectionBase):
    """The server-side h1 driver: reads a request, hands back a `ServerRequest`,
    then writes the response; reuses the connection on keep-alive. The accepting
    analogue of the client `Connection`, over the shared `H1ConnectionBase`."""

    def __init__(self, transport, *, backend=None, header_read_timeout=_DEFAULT_HEADER_READ_TIMEOUT):
        super().__init__(transport, backend=backend)
        self._reusable = True  # keep-alive: may we read another request after this one?
        self._codec = None  # current request/response codec
        self._current = None  # current ServerRequest (for body draining)
        self._head_raw = b""  # raw bytes of the in-progress head read (h2-preface check, F49)
        self._shutdown_evt = self.backend.event()  # set by graceful_shutdown()
        # Max time to read a complete request head before closing (slowloris defence),
        # hyper's `header_read_timeout` (default 30s, http1.rs L249). `None` disables it.
        self._header_read_timeout = header_read_timeout
        # The head-read deadline always uses `backend.timeout` (every backend implements it).
        # This capability is extra: a backend that can wake a parked read from another task
        # (asyncio) lets us skip the per-read shutdown `select`; absent (tonio) → use `select`.
        # See `_read_or_shutdown` + `graceful_shutdown`.
        self._native_read_interrupt = getattr(self.backend, "native_read_interrupt", False)
        self._idle_read_parked = False  # True only while parked in the idle between-requests read

    async def start(self):
        pass  # HTTP/1 has no connection preface / handshake

    async def graceful_shutdown(self):
        # h1 `Connection::graceful_shutdown` (hyper Dispatcher `disable_keep_alive`):
        # a non-blocking signal — stop reusing the connection so the accept loop
        # ends after the current request (`next_request` returns None once
        # `_reusable` is False), and wake a read parked idly between requests
        # (`_shutdown_evt`, checked in the head-read). The caller drives the serve
        # loop to completion and closes; nothing is awaited/closed here.
        self._reusable = False
        self._shutdown_evt.set()
        if self._native_read_interrupt and self._idle_read_parked:
            # asyncio: wake the read parked idly between requests so it returns promptly and
            # the connection closes, instead of relying on a per-read select race (tonio's path).
            self.transport.interrupt_read()

    def mark_unusable(self):
        self._reusable = False

    def _detach(self):
        # Same as the base, plus mark non-reusable (a tunnel serves no more requests).
        super()._detach()
        self._reusable = False

    async def next_request(self):
        """Read the next request head and return a `ServerRequest`, or None once
        the connection can serve no more (client closed, tunnel handed off, a
        non-keep-alive response was sent, or a parse error). Drains any unread body
        of the previous request first so the wire is positioned at the next head.

        hyper: the server `Dispatcher::poll_loop` (dispatch.rs L166) →
        `poll_read_head` (L292) → `Server::parse`; the drain mirrors
        `poll_drain_or_close_read` (conn.rs L849-865)."""
        if self._closed or not self._reusable:
            return None
        leftover = b""
        if self._current is not None:
            if not self._current._responded:
                # hyper serializes structurally (the dispatcher won't read the next
                # head until the response is written, dispatch.rs L628-633). Surface
                # the out-of-order use rather than mis-pairing responses.
                raise RuntimeError("respond to the current request before reading the next")
            if not self._current._body_done and not self._drain_unread_body(self._current):
                return None  # body not cheaply drainable — connection closed (see below)
            # Carry any pipelined bytes (the start of the next request, buffered
            # past this request's body) into the next codec — hyper keeps them in
            # its persistent read buffer; a fresh codec would drop them (deadlock).
            leftover = self._current._decoder.take_buffered()
        self._current = None

        codec = H1Codec()
        try:
            head = await self._read_request_head(codec, leftover)
        except ValueError as exc:
            self._closed = True
            self._reusable = False
            if bytes(self._head_raw[: len(_H2_PREFACE)]) == _H2_PREFACE:
                # An HTTP/2 client hit this h1-only server with the prior-knowledge
                # preface. hyper closes silently with a version error rather than
                # writing a 400 (on_parse_error -> has_h2_prefix -> new_version_h2,
                # conn.rs L809-812) (F49).
                self._close_transport()
                return None
            # Any other malformed request head: auto-respond like hyper's
            # `Server::on_error` (role.rs L466-484), then close.
            await self._send_error(codec, _error_status(str(exc)))
            return None
        if head is None:  # clean EOF between requests — client closed
            self._closed = True
            return None
        self._codec = codec
        decoder = H1BodyDecoder(head.body_kind, head.content_length or 0)
        decoder.feed(codec.take_body())  # body bytes read alongside the head
        req = ServerRequest(
            self,
            method=head.method,
            target=head.target,
            headers=head.headers,
            decoder=decoder,
            keep_alive=head.keep_alive,
            expect_continue=head.expect_continue,
            is_upgrade=head.is_upgrade,
            http10=head.http10,
            content_length=head.content_length,
        )
        self._current = req
        return req

    def _drain_unread_body(self, req):
        """Discard an unread request body so the next request parses cleanly, but
        only if it's cheap — a 1:1 mirror of hyper `poll_drain_or_close_read`
        (conn.rs L849-865), which does EXACTLY ONE `poll_read_body` (`let _ =
        self.poll_read_body(cx)`, then close unless the body reached KeepAlive). Pull a
        single frame: decode one from the already-buffered bytes, or — only if the
        buffer held nothing decodable yet (need-more, not end) — do ONE non-blocking
        socket read and decode that. Reuse iff that single poll completed the body;
        otherwise `close_read()` (never loop the socket to drain an arbitrary body,
        never send the skipped `100 Continue`).

        One poll, not a loop, is deliberate and matches hyper exactly: a chunked body
        with any data frame can't cheaply drain (one `decode()` yields one DATA chunk,
        the terminating chunk unseen → not complete → close), and a half-buffered length
        body closes (hyper's length decoder reads from IO only when its buffer is empty,
        so a partial buffer yields a short frame and gives up — decode.rs Length). The
        single non-blocking read is `backend.receive_nowait`, the equivalent of hyper's
        `poll_read_body` seeing `Pending`. Returns True if drained (reusable), else False."""
        dec = req._decoder
        try:
            if dec.decode() is None and not dec.is_complete:  # buffer had no full frame → need more
                data = self.backend.receive_nowait(self.transport, _READ_SIZE)
                if data:  # b"" == nothing ready right now, or EOF -> give up (close)
                    dec.feed(data)
                    dec.decode()
        except Exception:  # noqa: S110 - a decode failure just means "not drainable → close"
            pass
        if dec.is_complete:
            req._body_done = True
            return True
        self._closed = True
        self._reusable = False
        return False

    async def _read_request_head(self, codec, initial=b""):
        # Bound the head read by `header_read_timeout` (slowloris defence, hyper http1.rs L249):
        # if the deadline wins, close with no response (hyper closes on a header-read timeout).
        # `backend.timeout(coro, seconds) -> (result, completed)` cancels the read cleanly on
        # expiry — cheap on every backend (asyncio: one task + one timer; tonio: its native
        # timeout). `None` disables the deadline.
        if self._header_read_timeout is None:
            return await self._read_head_frames(codec, initial)
        result, completed = await self.backend.timeout(
            self._read_head_frames(codec, initial), self._header_read_timeout
        )
        if not completed:
            self._closed = True
            self._reusable = False
            self._close_transport()
            return None
        return result

    async def _read_head_frames(self, codec, initial=b""):
        # Feed any carried-over pipelined bytes before touching the transport, so a
        # request already sitting in the buffer parses without a (blocking) read.
        buffered = 0
        # Retain the raw head bytes read so far (bounded by the head-size cap below)
        # so a parse error can be checked against the HTTP/2 preface (F49).
        raw = bytearray(initial)
        self._head_raw = raw
        if initial:
            buffered += len(initial)
            head = codec.receive_request_head(initial)
            if head is not None:
                return head
        idle = not initial  # no bytes of this request seen yet -> a graceful shutdown may end it
        while True:
            if idle:
                # Between requests: a graceful shutdown may end the wait (see
                # `_read_or_shutdown` — sync; returns the sentinel or the read awaitable).
                read = self._read_or_shutdown(_READ_SIZE)
                if read is _SHUTDOWN:
                    return None
                # Flag the park as idle so graceful_shutdown() may wake THIS read only —
                # never a mid-head read (a request in flight must complete).
                self._idle_read_parked = True
                try:
                    data = await read
                finally:
                    self._idle_read_parked = False
                if data is _SHUTDOWN:  # tonio select: the shutdown signal won the race
                    return None
                # An interrupt_read shutdown wake surfaces as b"" and falls into the EOF
                # return below — same outcome either way (loop ends; close, no response).
            else:
                data = await self.transport.receive_some(_READ_SIZE)
            if not data:
                # EOF. A clean EOF between requests (nothing buffered) is a normal
                # client close. hyper additionally distinguishes a MID-head EOF
                # (`buffered > 0`) as `Parse::Eof`/`IncompleteMessage` (an error), but
                # the wire outcome is identical either way — the connection just closes
                # with no response — so we surface both as a clean end-of-iteration
                # rather than raise (F47, observability-only; raising here would change
                # `next_request`'s clean-close contract for every host loop).
                return None
            idle = False  # a request's bytes have started arriving — don't interrupt now
            buffered += len(data)
            raw += data
            head = codec.receive_request_head(data)
            if head is not None:
                return head
            # Cap the still-incomplete head at hyper's max_buf_size (io.rs L202-205):
            # once the buffered bytes reach the limit it's `Parse::TooLarge` -> auto 431
            # + close. Without this a slow/never-terminating head stream is unbounded
            # memory per connection (F14). `_error_status` maps "TooLarge" -> 431.
            if buffered >= _MAX_HEAD_SIZE:
                raise ValueError("message head is too large (Parse::TooLarge)")

    def _read_or_shutdown(self, n):
        """The next idle-read awaitable, or `_SHUTDOWN` if a graceful shutdown was already
        requested. A plain (sync) function so the fast path adds no wrapper coroutine: the
        caller awaits the returned awaitable with `_idle_read_parked` set around it (so
        `graceful_shutdown()` wakes only an idly-parked read, never a mid-head one) and
        treats an awaited `_SHUTDOWN` — or a woken `b""` — as shutdown, not data.

        asyncio (`native_read_interrupt`): a plain read; if it parks, graceful_shutdown()
        wakes it via `interrupt_read` and the read returns b"". Backends without a native
        read interrupt (tonio can't wake a parked recv from another task): race the read
        against the shutdown signal via `select`, which cancels the losing branch — hyper's
        poll instead re-checks `should_read` and completes without reading."""
        if self._shutdown_evt.is_set():
            return _SHUTDOWN

        if self._native_read_interrupt:
            return self.transport.receive_some(n)

        def _recv():
            return self.transport.receive_some(n)

        async def _await_shutdown():
            await self._shutdown_evt.wait()
            return _SHUTDOWN

        return self.backend.select(_recv(), _await_shutdown())

    async def _send_error(self, codec, status):
        """Best-effort automatic error response (bodyless, `Connection: close`),
        then close — hyper `Server::on_error` + `write_head`."""
        try:
            # hyper `close_read()`s before `on_error`, so `enforce_version` inserts
            # `connection: close` on the auto response. The encoder adds no header
            # itself, so inject it here (F29) rather than emitting a bare error head.
            hdrs = self._negotiate_connection_header(HeaderMap(), keep_alive=False, http10=False, resp_close=False)
            head = codec.serialize_response(status, hdrs, keep_alive=False)
            await self.transport.send_all(head)
            await self.transport.send_all(codec.serialize_end())
        except BaseException:  # noqa: S110 - best-effort: if we can't write the 400, just close
            pass
        self._close_transport()

    async def send_response(self, req, status, headers, body):
        # hyper: role.rs `Server::encode` (L364) writes the status line + headers
        # (incl. Date) + returns the body Encoder; the driver reimplements the
        # keep-alive/version negotiation hyper does in conn.rs
        # (`enforce_version`/`fix_keep_alive`, L656-702) before that call.
        hdrs = headers if headers is None or isinstance(headers, HeaderMap) else HeaderMap(headers)
        content_length, chunked = self._body_framing(body)
        http10 = req._http10
        # A protocol switch turns the connection into a raw tunnel (Server::encode
        # L378-384 forces is_last): no reuse, and we hand the transport to the caller.
        # It's a switch only when the request actually asked for the upgrade — a 101
        # whose request had no Upgrade (hyper `wants_upgrade`, role.rs), or a 2xx to a
        # CONNECT. A 101 the request didn't ask for is NOT a tunnel; it still ends the
        # connection (handled below), it just isn't handed off (F28).
        is_switch = (status == 101 and req.is_upgrade) or (req.method == "CONNECT" and 200 <= status < 300)
        # An HTTP/1.0 iterable body is close-delimited ONLY if the app gave no
        # Content-Length: with an explicit CL the encoder frames it as length(n) (hyper
        # set_length's `existing_con_len` branch, role.rs), so the client knows where the
        # body ends and the connection stays reusable. Deciding this from the body shape
        # alone (iterable ⇒ chunked ⇒ close) wrongly forced a close + dropped the
        # keep-alive header for a CL-framed streamed 1.0 response (F27).
        close_delimited = http10 and chunked and (hdrs is None or hdrs.get("content-length") is None)
        # Reuse iff the request is keep-alive, the response doesn't ask to close,
        # the body isn't close-delimited, and we're not switching protocols. (An
        # unread request body is drained-or-closed later, in next_request — matching
        # hyper, whose drain runs in a poll_read after the response is written, so the
        # already-sent response keeps its keep-alive header and a failed drain just FINs.)
        resp_close = _connection_has(hdrs, "close")
        keep_alive = req.keep_alive and not resp_close and not close_delimited and not is_switch
        if status == 101 and not is_switch:
            keep_alive = False  # a 101 the request didn't ask for still ends the connection (hyper is_last)
        # Graceful shutdown requested mid-request: this in-flight response must
        # advertise `Connection: close` and the connection must not be reused —
        # mirroring hyper's `disable_keep_alive` (KA::Disabled), whose in-flight
        # response is encoded is_last with `connection: close` inserted by
        # enforce_version (conn.rs L682-698). Without this, the `_reusable = keep_alive`
        # below would overwrite the `False` graceful_shutdown() set and keep the
        # connection alive.
        if self._shutdown_evt.is_set():
            keep_alive = False
        if not is_switch:
            # A switch keeps the app's `Connection: upgrade` verbatim; otherwise
            # make the wire header agree with the reuse/version decision.
            hdrs = self._negotiate_connection_header(hdrs, keep_alive, http10, resp_close)
        try:
            head = self._codec.serialize_response(
                status,
                hdrs,
                keep_alive=keep_alive,
                http10=http10,
                content_length=content_length,
                chunked=chunked,
            )
            await self.transport.send_all(head)
            await self._send_body(self._codec, body)
        except BaseException as exc:
            self._reusable = False
            self._closed = True
            self._close_transport()
            raise ConnectionClosedError("failed to send response") from exc
        if is_switch:
            # Hand the raw connection (plus any bytes already buffered past the
            # request head — the start of the tunnel) to the caller and detach.
            req.upgraded = H1Upgraded(self.transport, req._decoder.take_buffered())
            self._detach()
            return
        self._reusable = keep_alive
        if not self._reusable:
            self._closed = True
            self._close_transport()

    @staticmethod
    def _negotiate_connection_header(hdrs, keep_alive, http10, resp_close):
        # Reimplements hyper `fix_keep_alive`/`enforce_version` (conn.rs L656-702):
        # make the wire `Connection` header agree with the reuse + version decision.
        # `Server::encode` writes whatever header is present but adds none itself.
        # hyper uses `HeaderMap::insert` (REPLACE), so set — not add — the header:
        # appending would leave a duplicate/contradictory `Connection` token next to a
        # user-set value (F48).
        if not keep_alive and not resp_close and not http10:
            # HTTP/1.1 defaults to keep-alive → must announce the close.
            hdrs = hdrs or HeaderMap()
            hdrs["connection"] = "close"
        elif keep_alive and http10:
            # HTTP/1.0 defaults to close → must announce the keep-alive.
            hdrs = hdrs or HeaderMap()
            hdrs["connection"] = "keep-alive"
        return hdrs


class H1Server(BaseServer[ServerRequest]):
    """An HTTP/1 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http1`). The
    async-context-manager + accept-iterator come from `BaseServer` (identical to
    `H2Server`).

        async with H1Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def __init__(
        self,
        transport: Any,
        *,
        backend: BackendLike | None = None,
        header_read_timeout: float = _DEFAULT_HEADER_READ_TIMEOUT,
    ) -> None:
        self._conn = ServerConnection(transport, backend=backend, header_read_timeout=header_read_timeout)
