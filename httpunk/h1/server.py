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

from .._common import BaseServer, read_all
from .._httpunk import H1BodyDecoder, H1Codec
from ..exceptions import ConnectionClosedError
from ..http import HeaderMap
from .connection import H1ConnectionBase
from .share import H1Upgraded


_READ_SIZE = 65536
_SHUTDOWN = object()  # sentinel: a graceful shutdown released an idle head-read
# The interim response hyper's server auto-sends for `Expect: 100-continue`
# (byte-identical to hyper conn.rs L413).
_CONTINUE = b"HTTP/1.1 100 Continue\r\n\r\n"


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

    async def aiter_bytes(self):
        """Yield request body chunks, pulling transport bytes on demand (decoded
        by `H1BodyDecoder`). Sends `100 Continue` first if the client asked for it."""
        if self._decoder.is_complete:
            return
        if self._expect_continue and not self._continue_sent:
            # The client is waiting for a 1xx before sending the body (RFC 9110
            # §10.1.1). hyper auto-sends 100 Continue when the body is first polled
            # (conn.rs L410-413, `Reading::Continue`); we do the same on first read.
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

    async def read(self):
        return await read_all(self.aiter_bytes())

    async def respond(self, status, *, headers=None, body=None):
        """Send the response head (+ body). `body` is None, `bytes`, or a
        (sync/async) iterable of `bytes`. hyper: `Server::encode`."""
        if self._responded:
            raise RuntimeError("response already sent for this request")
        self._responded = True
        await self._conn.send_response(self, status, headers, body)

    def __repr__(self):
        return f"ServerRequest(method={self.method!r}, target={self.target!r})"


class ServerConnection(H1ConnectionBase):
    """The server-side h1 driver: reads a request, hands back a `ServerRequest`,
    then writes the response; reuses the connection on keep-alive. The accepting
    analogue of the client `Connection`, over the shared `H1ConnectionBase`."""

    def __init__(self, transport, *, backend=None):
        super().__init__(transport, backend=backend)
        self._reusable = True  # keep-alive: may we read another request after this one?
        self._codec = None  # current request/response codec
        self._current = None  # current ServerRequest (for body draining)
        self._shutdown_evt = self.backend.event()  # set by graceful_shutdown()

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
            # A malformed request head: auto-respond like hyper's `Server::on_error`
            # (role.rs L466-484), then close.
            await self._send_error(codec, _error_status(str(exc)))
            self._closed = True
            self._reusable = False
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
        (conn.rs L849-865): decode what's already buffered, do ONE non-blocking
        read of whatever else is sitting in the socket buffer, and reuse iff that
        completed the body; otherwise `close_read()` (never stream an arbitrary
        body off the socket, never send the skipped `100 Continue`). The single
        non-blocking read is `backend.receive_nowait` — the tonio equivalent of
        hyper's `poll_read_body` returning `Pending` when nothing is ready.
        Returns True if drained (connection reusable), False if it closed."""
        dec = req._decoder
        try:
            while dec.decode() is not None:  # decode already-buffered body bytes
                pass
            if not dec.is_complete:
                data = self.backend.receive_nowait(self.transport, _READ_SIZE)
                if data:  # b"" == nothing ready right now, or EOF -> give up (close)
                    dec.feed(data)
                    while dec.decode() is not None:
                        pass
        except Exception:  # noqa: S110 - a decode failure just means "not drainable → close"
            pass
        if dec.is_complete:
            req._body_done = True
            return True
        self._closed = True
        self._reusable = False
        return False

    async def _read_request_head(self, codec, initial=b""):
        # Feed any carried-over pipelined bytes before touching the transport, so a
        # request already sitting in the buffer parses without a (blocking) read.
        if initial:
            head = codec.receive_request_head(initial)
            if head is not None:
                return head
        idle = not initial  # no bytes of this request seen yet -> a graceful shutdown may end it
        while True:
            if idle:
                # Between requests: race the read against a graceful-shutdown signal
                # so an idle keep-alive connection completes promptly (tonio can't
                # wake a parked recv from another task; hyper's poll re-checks
                # `should_read` and completes instead of reading).
                data = await self._read_or_shutdown(_READ_SIZE)
                if data is _SHUTDOWN:
                    return None
            else:
                data = await self.transport.receive_some(_READ_SIZE)
            if not data:
                return None  # EOF
            idle = False  # a request's bytes have started arriving — don't interrupt now
            head = codec.receive_request_head(data)
            if head is not None:
                return head

    async def _read_or_shutdown(self, n):
        # Read the next head byte(s), or `_SHUTDOWN` if `graceful_shutdown()` fires
        # first. The backend's `select` cancels the losing branch, so the parked read
        # is cleanly dropped when shutdown wins.
        if self._shutdown_evt.is_set():
            return _SHUTDOWN

        async def _recv():
            return await self.transport.receive_some(n)

        async def _await_shutdown():
            await self._shutdown_evt.wait()
            return _SHUTDOWN

        return await self.backend.select(_recv(), _await_shutdown())

    async def _send_error(self, codec, status):
        """Best-effort automatic error response (bodyless, `Connection: close`),
        then close — hyper `Server::on_error` + `write_head`."""
        try:
            head = codec.serialize_response(status, None, keep_alive=False)
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
        # A protocol switch (101) or a 2xx to CONNECT turns the connection into a
        # raw tunnel (Server::encode L378-384 forces is_last): no reuse, and we
        # hand the transport to the caller afterwards.
        is_switch = status == 101 or (req.method == "CONNECT" and 200 <= status < 300)
        # An unknown-length HTTP/1.0 body is close-delimited (role.rs L907-910):
        # the connection must close so the client can detect end-of-body.
        close_delimited = http10 and chunked
        # Reuse iff the request is keep-alive, the response doesn't ask to close,
        # the body isn't close-delimited, and we're not switching protocols.
        resp_close = _connection_has(hdrs, "close")
        keep_alive = req.keep_alive and not resp_close and not close_delimited and not is_switch
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
        if not keep_alive and not resp_close and not http10:
            # HTTP/1.1 defaults to keep-alive → must announce the close.
            hdrs = hdrs or HeaderMap()
            hdrs.add("connection", "close")
        elif keep_alive and http10:
            # HTTP/1.0 defaults to close → must announce the keep-alive.
            hdrs = hdrs or HeaderMap()
            hdrs.add("connection", "keep-alive")
        return hdrs


class H1Server(BaseServer):
    """An HTTP/1 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http1`). The
    async-context-manager + accept-iterator come from `BaseServer` (identical to
    `H2Server`).

        async with H1Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def __init__(self, transport, *, backend=None):
        self._conn = ServerConnection(transport, backend=backend)
