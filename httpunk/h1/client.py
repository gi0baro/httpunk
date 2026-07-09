"""Low-level HTTP/1 client — the `http1` analogue of `h2/client.py`.

Holds the client's full role stack, mirroring `server.py`: `Connection` (the
client-side driver over the shared `H1ConnectionBase`) and `H1Connection` (the
public per-connection handle). `H1Connection` exposes the **same** surface as
`H2Connection` — `send_request(Request) -> Response`, `ready`, and the
`get`/`request` wrappers — so a caller can treat h1 and h2 connections identically.

Cross-reference: hyper `client::conn::http1` (`SendRequest`/`Connection`) +
`proto/h1/{conn,dispatch,role}.rs` (the Client path).
"""

from .._common import BaseClientConnection
from .._httpunk import H1BodyDecoder, H1Codec
from ..exceptions import ConnectionClosedError
from ..http import HeaderMap
from ..types import Response
from .connection import H1ConnectionBase
from .share import H1ResponseBody, H1Upgraded


class Connection(H1ConnectionBase):
    """The client-side h1 driver: writes a request, reads a response, reuses the
    connection on keep-alive. Mirrors hyper's Client `Dispatcher` over `Conn`.

    Unlike h2 (which multiplexes and needs a background read-pump), HTTP/1 is
    strictly one request/response at a time. But hyper's `Dispatcher` does not
    collapse to strict send-then-read: its `poll_loop` interleaves reads and
    writes each turn (dispatch.rs L172-211), so a response head can arrive while
    the request body is still being written. `send_request` therefore writes
    head+body in a spawned task and reads the response head concurrently; if the
    response arrives first (a server answering an upload early — 413/401/redirect),
    it stops writing instead of deadlocking against a full send buffer. A single
    in-flight "slot" serializes requests (hyper's `Conn` is `busy` while a message
    is in flight, conn.rs L293); the response body frees it when fully read (or on
    `aclose`), reusing the connection on keep-alive or closing it otherwise.

    Faithfulness notes:
    - Low-level like `client::conn::http1`: the caller supplies the `Host` header
      (we never auto-add one). (h2 differs: `:authority` is derived from the URI.)
    - 1xx-informational responses are skipped by the vendored `Client::parse`
      (role.rs L1013), so they never surface here.
    - A 101 upgrade / 2xx-to-CONNECT hands the raw transport to the caller as an
      `H1Upgraded` (`resp.upgraded`); the driver detaches (hyper `on_upgrade`).
    - `Expect: 100-continue` and the request's `Connection: close` are non-gaps for
      the client: hyper hard-codes `expect_continue: false` (role.rs L1161) and
      derives reuse solely from the response's keep-alive (conn.rs L294).
    """

    def __init__(self, transport, *, authority=None, backend=None):
        super().__init__(transport, backend=backend)
        self.authority = authority
        self.error = None
        # Remembers the last response's version (hyper `state.version`, conn.rs
        # L295): once a peer answers in HTTP/1.0, later requests on the reused
        # connection downgrade to 1.0 and re-assert keep-alive (enforce_version).
        self._peer_http10 = False
        # One request/response in flight at a time (h1 has no multiplexing).
        self._slot = self.backend.semaphore(1)
        # The in-flight request's background body writer (single-in-flight): its
        # detached scope + an event set when the body was sent in FULL. hyper's
        # poll_loop writes the body independently of reading the response, so an early
        # response doesn't truncate the upload; the writer runs past `send_request` and
        # the reuse decision is deferred to `release_slot`. A fresh scope per request
        # (a scope can't be re-armed after `cancel()`), so reuse isn't blocked.
        self._writer_scope = None
        self._writer_done = None

    async def connect(self):
        pass  # HTTP/1 has no connection preface / handshake (unlike h2)

    async def _teardown_writer(self, *, cancel):
        """Finish with the in-flight body writer: `cancel=True` aborts it (still
        running — an early response we didn't wait out), then joins; `cancel=False`
        just joins an already-finished writer (instant). Idempotent."""
        scope = self._writer_scope
        if scope is None:
            return
        self._writer_scope = self._writer_done = None
        if cancel:
            scope.cancel()
        await scope.__aexit__(None, None, None)

    def _acquire(self):
        return self._slot.acquire()  # blocks until the single in-flight slot is free

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
        # (L192); we do not add it. `Conn` is busy for the duration (conn.rs L293),
        # modeled by the 1-permit slot.
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
            # hyper's `poll_loop` drives reads and writes INDEPENDENTLY each turn
            # (dispatch.rs L172-211): a response head can arrive while the request
            # body is still being written, and an early response (413/401/redirect)
            # does NOT truncate the upload — the body keeps writing, and the
            # connection is reused only if it actually completes. So we write
            # head+body in a DETACHED background task (a per-request scope that
            # outlives this call) and read the head concurrently. The writer is NOT
            # cancelled at head-arrival (that used to truncate the request + burn the
            # connection, F11); `release_slot` decides its fate when the caller has
            # finished the response: joins it if done -> reuse, cancels it if not ->
            # close.
            body_done = self.backend.event()
            write_error = []
            scope = self.backend.scope()
            await scope.__aenter__()
            scope.spawn(self._write_request(codec, head, body, body_done, write_error))
            self._writer_scope, self._writer_done = scope, body_done
            try:
                resp_head = await self._read_head(codec, write_error)
            except BaseException:
                await self._teardown_writer(cancel=True)  # no head -> abandon the write
                raise
            # Remember the peer's version so the next request on a reused
            # connection can fix itself up (hyper conn.rs L295).
            self._peer_http10 = resp_head.http10
            if resp_head.is_upgrade:
                # 101 Switching Protocols / 2xx to CONNECT: the connection stops
                # being HTTP. Hand the transport (plus any bytes already read past
                # the head — the start of the upgraded protocol) to an H1Upgraded
                # the caller owns; this driver won't touch the transport again
                # (hyper `on_upgrade` / `Connection::into_parts`).
                await self._teardown_writer(cancel=True)  # the request-body write is moot
                upgraded = H1Upgraded(self.transport, codec.take_body())
                self._detach()
                self._slot.release()
                body = H1ResponseBody(self, None, keep_alive=False, upgraded=upgraded)
                return Response(resp_head.status, resp_head.headers, body)
            decoder = H1BodyDecoder(resp_head.body_kind, resp_head.content_length or 0)
            decoder.feed(codec.take_body())  # body bytes already read alongside the head
            # The response's own keep-alive contribution; `release_slot` ANDs it with
            # "the request body was fully sent". A close-delimited body ends only at
            # EOF (the server closes to signal end), so it can never be reused even if
            # the keep-alive signal said otherwise (hyper conn.rs L458-489).
            resp_keep_alive = resp_head.keep_alive and resp_head.body_kind != "close"
            # The response body owns the slot from here; it releases it (and resolves
            # the writer) when fully read or on aclose. A bodyless response has nothing
            # to read, so resolve it now (in this async context) instead.
            body = H1ResponseBody(self, decoder, keep_alive=resp_keep_alive)
            if body._needs_eager_finish:
                await body._finish()
            return Response(resp_head.status, resp_head.headers, body)
        except BaseException as exc:
            await self._teardown_writer(cancel=True)
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

    async def _read_head(self, codec, write_error=None):
        # hyper: conn.rs `can_read_head` (L175) + `read_head` -> role.rs
        # `Client::parse` (L1013), which loops past 1xx informational responses.
        while True:
            data = await self.transport.receive_some(65536)
            if not data:
                # EOF before a full head. If the body write also failed (server
                # closed both directions), surface that as the cause.
                if write_error:
                    raise write_error[0]
                raise ConnectionClosedError("connection closed before the response head")
            head = codec.receive_head(data)
            if head is not None:
                return head

    def poison_unexpected(self, nbytes):
        """The server sent `nbytes` unsolicited bytes past the response body — an
        HTTP/1 protocol violation (a server may not send anything before the next
        request). hyper's client fails the connection here via `require_empty_read`
        -> `new_unexpected_message` (conn.rs L463-465); we record the error so the
        next `send_request`/`wait_idle` raises it (the slot release closes it)."""
        if self.error is None:
            self.error = ValueError(f"received {nbytes} unexpected bytes on an idle HTTP/1 connection")

    async def release_slot(self, resp_keep_alive):
        """Free the in-flight slot once the caller has finished the response. Reuse
        the connection only if the response allowed keep-alive AND the request body
        was fully sent — hyper reuses only once both the read and write halves reach
        `KeepAlive` (conn.rs L370-400). If the writer is still running (the server
        answered early and the caller didn't wait out the upload), the request is
        incomplete on the wire, so cancel it and close; otherwise join it (instant)."""
        fully_sent = self._writer_done is None or self._writer_done.is_set()
        await self._teardown_writer(cancel=not fully_sent)
        if not (resp_keep_alive and fully_sent):
            self._closed = True
            self._close_transport()
        self._slot.release()

    async def close(self):
        # A response body left unread (or an early exit) can leave the background
        # writer still running; abort + join it before closing the transport.
        await self._teardown_writer(cancel=True)
        await super().close()

    def _fail(self, exc):
        if self.error is None and not isinstance(exc, ConnectionClosedError):
            self.error = exc
        self._closed = True
        self._close_transport()


class H1Connection(BaseClientConnection):
    """An HTTP/1 client connection over a caller-supplied, already-connected
    `transport`. Use as an async context manager; the transport is closed on exit.
    Serves one request/response at a time (no pipelining); keep-alive connections
    are reused for subsequent requests.

    Like hyper's `client::conn::http1`, this is low-level: the request-target is
    sent verbatim and the caller supplies the `Host` header (we never auto-add
    one). `authority` is accepted for API symmetry with `H2Connection` but is not
    used to rewrite the target. `__aenter__`/`__aexit__`/`request`/`get` come from
    `BaseClientConnection` (identical to `H2Connection`)."""

    def __init__(self, transport, *, authority=None, backend=None):
        self._conn = Connection(transport, authority=authority, backend=backend)

    def ready(self):
        """Wait until the connection can accept a request (h1 has no stream slots;
        this waits for the single in-flight request/response to finish). Mirrors
        h2's `conn.ready`."""
        return self._conn.wait_idle()

    def send_request(self, request):
        """Send `request` and return its `Response` once the head arrives.
        Mirrors h2's `send_request` (hyper `SendRequest::send_request`).

        The request-target is sent **verbatim** (hyper's low-level contract): a
        path (``"/thing"``) is origin-form, an absolute URL (``"http://…"``) is
        absolute-form for a proxy, and an authority (``"host:port"``) is
        authority-form for CONNECT. The caller supplies the ``Host`` header (we
        never auto-add it), exactly like hyper's `client::conn::http1`.
        """
        return self._conn.send_request(request.method, request.target, request.headers, request.body)
