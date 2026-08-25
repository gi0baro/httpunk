"""Low-level HTTP/1 client — the `http1` analogue of `h2/client.py`.

Holds the client's full role stack, mirroring `server.py`: `Connection` (the
client-side driver over the shared `H1ConnectionBase`) and `H1Connection` (the
public per-connection handle). `H1Connection` exposes the **same** surface as
`H2Connection` — `send_request(Request) -> Response`, `ready`, and the `request`
wrapper — so a caller can treat h1 and h2 connections identically.

Cross-reference: hyper `client::conn::http1` (`SendRequest`/`Connection`) +
`proto/h1/{conn,dispatch,role}.rs` (the Client path).
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

from .._common import BaseClientConnection
from .._httpunk import H1BodyDecoder, H1Codec
from ..exceptions import ConnectionClosedError
from ..http import HeaderMap
from ..types import Response
from .connection import H1ConnectionBase
from .share import H1ResponseBody, H1Upgraded


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Request


# Max bytes of a still-incomplete response head before failing the connection —
# hyper's `DEFAULT_MAX_BUFFER_SIZE` (io.rs: 8192 + 4096*100), the same cap the
# server role applies to request heads (`h1/server.py` `_MAX_HEAD_SIZE`).
_MAX_HEAD_SIZE = 8192 + 4096 * 100


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
    - While reusable-idle, a watcher task holds read interest — the re-expression
      of hyper's always-polled `Connection` future (see `_watch_idle`) — so an
      idle peer close or stray byte flips `closed`/`error` when it happens, and
      a `send_request` failure raised before the request was handed to the
      writer carries `request_unsent = True` (hyper's `TrySendError` message
      give-back, client/conn/http1.rs L247-263).
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
        # True while an exchange holds the slot (hyper `Conn::is_busy`, conn.rs
        # L293) — the SYNC observable a pool needs at park time: a connection
        # whose last exchange never completed (its release was interrupted) must
        # be dropped, not parked as idle keepalive. Set on slot acquire, cleared
        # wherever the slot is released.
        self._busy = False
        # The in-flight request's background body writer (single-in-flight): its
        # detached scope + an event set when the body was sent in FULL. hyper's
        # poll_loop writes the body independently of reading the response, so an early
        # response doesn't truncate the upload; the writer runs past `send_request` and
        # the reuse decision is deferred to `release_slot`. A fresh scope per request
        # (a scope can't be re-armed after `cancel()`), so reuse isn't blocked.
        self._writer_scope = None
        self._writer_done = None
        # The idle watcher: a background task parked in ONE read while the
        # connection is reusable-idle — the Python re-expression of hyper's
        # always-polled `Connection` future, which keeps read interest registered
        # for the whole idle period (dispatch.rs L287 falls through to
        # `poll_read_keep_alive`, conn.rs L435-443; `force_io_read` returns
        # Pending, conn.rs L510-520), so a peer FIN or stray byte is observed
        # when it ARRIVES, not at the next send. Python coroutines can't hold
        # read interest without a parked task — the watcher is that task
        # (divergence in mechanism only; behavior matches hyper, see
        # `_watch_idle`). The watcher is NEVER cancelled or signalled: a parked
        # `receive_some` legitimately ends only via data, EOF, or transport
        # close (cancelling one leaves the socket's read registration armed —
        # tonio contract), so its read completes naturally and is dispatched by
        # `_exchange_active`: while idle it enforces the idle rules; once an
        # exchange has started, the bytes that complete it ARE the response's
        # first bytes, handed to `_read_head` via `_watcher_data`/`_watcher_error`
        # — exactly hyper's structure, where one poll loop owns the read across
        # the idle->busy transition. A bare task via `spawn_without_results`
        # (the cancel-free seam primitive — a scope is for groups that DO get
        # cancelled, like the body writer's): the handle IS the per-idle-period
        # join, awaited by `_read_head` (handoff) or a teardown path.
        self._watcher_handle = None
        self._watcher_data = None
        self._watcher_error = None
        self._exchange_active = False
        # Guards the take-ownership pop of the background-task handles
        # (`_writer_scope`, `_watcher_handle`): a `close()` from another thread
        # (tonio is work-stealing) can race the exchange's own join/teardown
        # for the SAME handle — a second `Scope.__aexit__` raises RuntimeError
        # (tonio `_exit`), spawn handles are await-once, and the handoff-field
        # consumption must have one owner — the pop under the lock makes the
        # loser see None. Held only for the sync swap, never across an await.
        # Everything else the watcher shares is lock-free by construction: the
        # handoff fields are published by the join, the handles/flag resets by
        # the slot semaphore, and the `_exchange_active` read by the runtime's
        # causal chain (flag-set -> head write -> peer response -> poller wake).
        self._scopes_lock = threading.Lock()

    async def connect(self):
        # HTTP/1 has no connection preface / handshake (unlike h2), but hyper's
        # `Connection` future is polled from the moment it is spawned — the
        # dial-to-first-send window has read interest upstream, so it is watched
        # here too.
        self._start_watcher()

    async def _teardown_writer(self, *, cancel):
        """Finish with the in-flight body writer: `cancel=True` aborts it (still
        running — an early response we didn't wait out), then joins; `cancel=False`
        just joins an already-finished writer (instant). Idempotent, and
        single-owner via the pop under `_scopes_lock`: a `close()` from another
        thread can race the exchange's own teardown for the SAME scope, and a
        second `Scope.__aexit__` raises RuntimeError (tonio `_exit`) — the
        losing popper sees None and returns instead."""
        with self._scopes_lock:
            scope, self._writer_scope, self._writer_done = self._writer_scope, None, None
        if scope is None:
            return
        if cancel:
            scope.cancel()
        await scope.__aexit__(None, None, None)

    def _start_watcher(self):
        """Arm the idle watcher (see `_watch_idle`) for one idle period — SYNC:
        the bare-task spawn suspends nothing, so callers have no interruption
        window. They guarantee no exchange can be reading: `connect()` runs
        before any request, and `release_slot` starts it BEFORE releasing the
        slot — tonio is work-stealing, so a waiter resumed by the release can
        run in parallel immediately, and the semaphore release/acquire is the
        happens-before edge that guarantees it observes the watcher and takes
        its read as the response's first read (F55 single-reader). No-op if the
        connection can serve no more requests."""
        if self._closed or self.error is not None or self.transport is None:
            return
        self._exchange_active = False
        self._watcher_data = self._watcher_error = None
        self._watcher_handle = self.backend.spawn_without_results(self._watch_idle())

    async def _join_watcher(self):
        """JOIN the idle watcher (never cancel/signal it — see `_watch_idle`):
        await its handle, which resolves when its single read has completed
        and publishes the handoff fields. Idempotent; instant when none is
        armed or it already finished. On teardown paths the transport is
        closed first, which is what ends the parked read. The handle is taken
        via a pop under `_scopes_lock` so a racing `close()` and exchange join
        have one owner (handles are await-once); the loser sees None and
        returns — the connection state it then observes is force-closed either
        way."""
        with self._scopes_lock:
            handle, self._watcher_handle = self._watcher_handle, None
        if handle is None:
            return
        await handle

    async def _watch_idle(self):
        """ONE read, dispatched by connection state when it completes; the
        spawn handle `_join_watcher` awaits resolves on exit. The
        watcher is never cancelled or woken by a signal — a parked
        `receive_some` legitimately ends only via data, EOF, or transport close
        (cancellation would leave the socket's read registration armed for a
        dead task, corrupting the next read). This is also hyper's structure:
        the same poll loop owns the read across the idle->busy transition
        (dispatch.rs L287 `poll_read` -> `poll_read_keep_alive` while idle, the
        response read once busy), so there is no reader handoff to cancel.

        Dispatch of the completed read (`_exchange_active` is set by
        `send_request` before it writes):

        - exchange active -> these are the response's first bytes (or `b""` =
          EOF before the head, or a transport error): store them in
          `_watcher_data`/`_watcher_error` for `_read_head`, touch nothing else
          — the exchange's own paths handle errors exactly as if it had done
          this read itself.
        - idle EOF -> hyper's CLEAN idle close: "found EOF on idle connection,
          closing" (conn.rs L471-481; `should_error_on_eof` is false when idle —
          conn.rs L201-204). `_closed` only, NO error recorded: a pool discards
          silently. The transport is closed NOW — hyper's connection task winds
          down and drops its io (dispatch.rs L152-160) — so the socket never
          parks in CLOSE_WAIT.
        - idle bytes -> `new_unexpected_message` (conn.rs L484-489): poison +
          close. Bytes racing `send_request`'s flag-set land in the same window
          hyper has between `require_empty_read` and the head write — either
          side of it, behavior matches upstream.
        - idle transport error -> fail (hyper `force_io_read` io error ->
          `state.close()`, conn.rs L510-520) — unless a concurrent `close()`
          already committed the flags (its transport close is what woke us)."""
        transport = self.transport
        if transport is None:  # a racing close() nulled it before we ran (F59)
            return
        try:
            data = await transport.receive_some(65536)
        except Exception as exc:
            if self._exchange_active:
                self._watcher_error = exc  # surfaced by _read_head's first read
            elif not self._closed:
                self._fail(exc)
            return
        if self._exchange_active:
            self._watcher_data = data  # the response's first bytes (b"" = EOF)
            return
        if data:
            self.poison_unexpected(len(data))
            return
        self._closed = True
        self._close_transport()

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

    async def send_request(self, method, url, headers, body, trailers=None):
        # hyper: client/conn/http1.rs `SendRequest::send_request` (L213) — writes
        # the head+body, reads the response. The caller must have supplied `Host`
        # (L192); we do not add it. `Conn` is busy for the duration (conn.rs L293),
        # modeled by the 1-permit slot.
        await self._acquire()
        self._busy = True
        # The watcher may have observed EOF/bytes and closed/poisoned the
        # connection while `_acquire` waited — the mirror of
        # `SendRequest::poll_ready` failing once the connection task died
        # (client/conn/http1.rs L156-180). `request_unsent` mirrors hyper
        # handing the request back when the dispatch channel refuses it —
        # `TrySendError { message: Some(req) }`, client/conn/http1.rs L247-263
        # — and a queued-never-taken request canceled on a connection error
        # (the idle-bytes poison, proto/h1/dispatch.rs L711-733): nothing was
        # written, so the caller may safely retry ANY body, streamed included.
        if self._closed or self.error is not None:
            self._busy = False
            self._slot.release()
            exc = self.error or ConnectionClosedError("connection closed")
            exc.request_unsent = True
            raise exc
        # `require_empty_read`'s buffered-bytes fast path at send time (conn.rs
        # L463-465; regression test client_read_bytes_before_writing_request):
        # a server may not send anything before the client's next request, and
        # hyper's single poll loop serializes this check ahead of the write even
        # under scheduler starvation — bytes ALREADY DELIVERED when a request is
        # submitted are always a violation, never that request's "response". A
        # parked watcher can't reproduce that ordering (its wake races this
        # task), so the check runs here too, BEFORE `_exchange_active` is set —
        # the ordering that makes it safe beside a parked watcher: if it steals
        # the violation bytes, the watcher's spurious wake (empty read / closed
        # transport) still dispatches through the idle rules against an
        # already-poisoned connection and touches nothing (F31, problem b).
        pending = self.backend.receive_nowait(self.transport, 65536)
        if pending:
            self.poison_unexpected(len(pending))  # records the error AND closes
            self._busy = False
            self._slot.release()
            self.error.request_unsent = True
            raise self.error
        # The exchange starts NOW: the watcher is not stopped (its parked read
        # is never cancelled — see `_watch_idle`); this sync flag redirects its
        # completing read from the idle rules to the exchange, whose `_read_head`
        # joins it and takes the bytes as the response's first read. Bytes
        # arriving from here to the head write are hyper's own race window
        # (require_empty_read ran; the write turn follows).
        self._exchange_active = True
        try:
            codec = H1Codec()
            content_length, chunked = self._body_framing(body)
            # If a previous response on this (reused) connection was HTTP/1.0,
            # downgrade this request to 1.0 and re-assert keep-alive — 1.0 defaults
            # to close, so hyper's `fix_keep_alive` injects `Connection: keep-alive`
            # (conn.rs L662-673) and `enforce_version` sets the version (L682-702).
            http10 = self._peer_http10
            if http10:
                # hyper `fix_keep_alive`: re-assert keep-alive UNLESS the request's
                # `Connection` header already carries the keep-alive token, and NOT when
                # it asks to close (an explicit close disables keep-alive, so hyper
                # leaves it). Value-checked (a token, not mere header presence) and set —
                # not appended — matching hyper's `insert` so no duplicate token (F58).
                headers = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
                tokens = {
                    part.strip().lower() for value in headers.get_all("connection") for part in bytes(value).split(b",")
                }
                if b"keep-alive" not in tokens and b"close" not in tokens:
                    headers["connection"] = "keep-alive"
            # Request trailers (F45) require a chunked body (RFC 9112 §7.1.2): force
            # chunked framing and declare the fields in the `Trailer` header, which hyper
            # reads to allow-list what `serialize_trailers` may emit after the body.
            trailer_fields = []
            if trailers is not None:
                headers = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
                content_length, chunked = None, True
                trailer_fields = list(trailers.keys())
                if "trailer" not in headers:
                    headers["trailer"] = ", ".join(trailer_fields)
            head = codec.serialize_request(
                method,
                url,
                headers,
                http10=http10,
                content_length=content_length,
                chunked=chunked,
                trailer_fields=trailer_fields,
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
            body_failed = self.backend.event()
            write_error = []
            scope = self.backend.scope()
            await scope.__aenter__()
            # The `request_unsent` boundary: this spawn is httpunk's analogue of
            # hyper's dispatcher taking the request off the channel (dispatch.rs
            # `poll_msg`). From here on a failure NEVER carries the marker — in
            # hyper, once taken, errors return `message: None` even if no byte
            # reached the wire (an encoded-but-unflushed head is not handed
            # back), and this boundary mirrors that exactly.
            scope.spawn(self._write_request(codec, head, body, body_done, write_error, body_failed, trailers))
            self._writer_scope, self._writer_done = scope, body_done
            try:
                # Race the head read against a body-write failure (F12): normally
                # `_read_head` wins; a body-iterable/framing error fires `body_failed` so
                # we don't park on the response head forever. One select per request (not
                # per head-read) to keep the hot path cheap.
                resp_head = await self.backend.select(
                    self._read_head(codec, write_error),
                    self._await_body_failure(body_failed, write_error),
                )
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
                # No idle watcher can be running here: `_read_head` consumed it
                # (its read delivered this 101 head's first bytes) and one is
                # only re-armed by `release_slot` on a reuse verdict, which the
                # upgrade path never reaches — a detached transport is never
                # watched.
                upgraded = H1Upgraded(self.transport, codec.take_body())
                self._detach()
                self._busy = False
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
            self._fail(exc)  # sync poison BEFORE the teardown suspension (see release_slot)
            try:
                await self._teardown_writer(cancel=True)
                # A watcher not yet consumed by `_read_head` (failure before it
                # ran): `_fail` closed the transport, which is what ends its
                # parked read — join it (never cancel).
                await self._join_watcher()
            finally:
                self._busy = False
                self._slot.release()
            raise

    async def _write_request(self, codec, head, body, body_done, write_error, body_failed, trailers=None):
        # Write the head then the framed body. A write failure (e.g. the server
        # closed the read side after answering early) must not mask a response
        # that did arrive: record it so `_read_head` can still deliver the head,
        # and only surface it if no response is forthcoming. Cancellation
        # (BaseException) propagates so the scope can unwind cleanly.
        try:
            await self._send_head_and_body(codec, head, body, trailers)
            body_done.set()
        except OSError as exc:
            # A TRANSPORT write failure (broken pipe / reset): the peer may have closed
            # right after sending an early response (413/redirect) that is still buffered
            # for `_read_head`, so DEFER — record it and surface it only if no head
            # arrives (EOF). Do NOT signal `body_failed`, or we'd race away that buffered
            # response (F11).
            write_error.append(exc)
        except Exception as exc:
            # A BODY-ITERABLE / framing error (the caller's body generator raised, or a
            # length mismatch): the request can't complete and no valid response is
            # coming, so fail PROMPTLY (hyper fails the dispatcher). Record it and wake
            # `_read_head` via `body_failed` so it surfaces the error instead of parking
            # on `receive_some` until the server times out (F12). The transport is closed
            # by `send_request`'s outer `_fail` once the writer is joined — closing it
            # here (inside the writer's scope) wedges that join.
            write_error.append(exc)
            body_failed.set()

    async def _read_head(self, codec, write_error=None):
        # hyper: conn.rs `can_read_head` (L175) + `read_head` -> role.rs
        # `Client::parse` (L1013), which loops past 1xx informational responses.
        buffered = 0
        data = None
        if self._watcher_handle is not None:
            # An idle watcher is armed: its parked `receive_some` is the
            # connection's single reader (F55), so the response's FIRST read is
            # its completing read — join it and take the handoff (bytes, `b""`
            # EOF, or a transport error re-raised here, exactly as if this task
            # had done the read). Only after the join does this task own the
            # transport. Hyper needs no join: one poll loop does both reads.
            await self._join_watcher()
            exc, self._watcher_error = self._watcher_error, None
            if exc is not None:
                raise exc
            data, self._watcher_data = self._watcher_data, None
            if data is None:
                # The watcher exited without a handoff: it dispatched through
                # the IDLE rules against this exchange — either a racing
                # close() tore the connection down under us, or bytes/EOF
                # landed concurrently with the exchange start and the watcher
                # saw the flag before this task's write of it was visible
                # (free-threaded: the junk race has no causal ordering, both
                # dispatches are legitimate). Surface the recorded verdict —
                # the unexpected-bytes poison (hyper `new_unexpected_message`)
                # or the clean close — not a generic error that would mask it.
                raise self.error or ConnectionClosedError("connection closed")
        while True:
            if data is None:
                data = await self.transport.receive_some(65536)
            if not data:
                # EOF before a full head. If the body write also failed (server
                # closed both directions), surface that as the cause.
                if write_error:
                    raise write_error[0]
                raise ConnectionClosedError("connection closed before the response head")
            buffered += len(data)
            head = codec.receive_head(data)
            if head is not None:
                return head
            # Cap the still-incomplete head at hyper's max_buf_size (io.rs
            # L202-205, enforcement tightened in 1.11.0 #4093): a server
            # streaming an endless header section must not grow this
            # connection's buffer without bound. The server role already
            # enforces the same cap (`_MAX_HEAD_SIZE` + auto-431); the client
            # just fails the connection (hyper `Parse::TooLarge`).
            if buffered >= _MAX_HEAD_SIZE:
                raise ValueError("response head too large (Parse::TooLarge)")
            data = None

    async def _await_body_failure(self, body_failed, write_error):
        # Raced against `_read_head` (once per request, in send_request): if the request
        # body write fails with a body-iterable / framing error and no response is
        # forthcoming, wake `send_request` promptly with that error instead of parking on
        # the response head until the server times out (F12). A TRANSPORT write error does
        # NOT fire this (F11 — an early response may be buffered), so the normal path
        # always lets `_read_head` win the race.
        await body_failed.wait()
        raise write_error[0]

    def poison_unexpected(self, nbytes):
        """The server sent `nbytes` unsolicited bytes outside a response — an
        HTTP/1 protocol violation (a server may not send anything before the next
        request). hyper's client fails the connection via `require_empty_read` ->
        `new_unexpected_message` (conn.rs L463-465 buffered, L484-489 read) and
        the errored connection task then winds down, dropping its io
        (dispatch.rs `poll_catch` L123-141) — so record the error AND close, for
        every caller (the idle watcher, the send-time guard, the past-body
        leftover check in share.py). The next `send_request`/`wait_idle` raises
        the recorded error."""
        if self.error is None:
            self.error = ValueError(f"received {nbytes} unexpected bytes on an idle HTTP/1 connection")
        self._closed = True
        self._close_transport()

    async def release_slot(self, resp_keep_alive):
        """Free the in-flight slot once the caller has finished the response. Reuse
        the connection only if the response allowed keep-alive AND the request body
        was fully sent — hyper reuses only once both the read and write halves reach
        `KeepAlive` (conn.rs L370-400). If the writer is still running (the server
        answered early and the caller didn't wait out the upload), the request is
        incomplete on the wire, so cancel it and close; otherwise join it (instant).

        The reuse verdict is committed SYNCHRONOUSLY, before the writer teardown:
        that await is a suspension point, and an interruption there (cancellation,
        or tonio's one-shot GC unwind of an abandoned body generator) must not
        leave an open connection whose `closed` lies to a pool above. In hyper the
        equivalent state transitions are sync between polls, so this unreachable
        state cannot exist there — same discipline here. The slot is released in
        `finally` for the same reason: on non-reuse the connection is already
        poisoned, and on reuse the exchange genuinely completed, so freeing the
        slot is correct even if the writer join was cut short."""
        fully_sent = self._writer_done is None or self._writer_done.is_set()
        reuse = resp_keep_alive and fully_sent
        if not reuse:
            self._closed = True
            self._close_transport()  # sync by design (backend.close_transport)
        try:
            await self._teardown_writer(cancel=not fully_sent)
            if reuse:
                # The connection is reusable-idle again: restore hyper's idle
                # read interest. Started BEFORE the slot release below (see
                # `_start_watcher` — the semaphore is the visibility edge for a
                # parallel waiter); `_start_watcher` no-ops if a racing
                # close()/failure landed during the writer join.
                self._start_watcher()
        finally:
            self._busy = False
            self._slot.release()

    async def close(self):
        # Commit `_closed` + close the transport FIRST (the base close is sync —
        # no suspension): the transport close is what ends the idle watcher's
        # parked read (it sees `_closed` and exits quietly) — then join it and
        # abort + join a still-running background writer. The joins can be
        # interrupted (they suspend); a force-close must never come away with
        # `closed` still False.
        await super().close()
        await self._join_watcher()
        await self._teardown_writer(cancel=True)

    def _fail(self, exc):
        # Sync poison + close. No live watcher to stop here: `_fail` runs only
        # inside an exchange (slot held, watcher joined at send_request entry)
        # or from the watcher itself.
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
    used to rewrite the target. `__aenter__`/`__aexit__`/`request` come from
    `BaseClientConnection` (identical to `H2Connection`)."""

    def __init__(self, transport: Any, *, authority: str | None = None, backend: BackendLike | None = None) -> None:
        self._conn = Connection(transport, authority=authority, backend=backend)

    def ready(self) -> Awaitable[None]:
        """Wait until the connection can accept a request (h1 has no stream slots;
        this waits for the single in-flight request/response to finish). Mirrors
        h2's `conn.ready`."""
        return self._conn.wait_idle()

    @property
    def closed(self) -> bool:
        """True once the connection can serve no more requests (closed or failed). A
        synchronous liveness check so a pool can evict a dead connection
        (util.Singleton self-heal)."""
        return self._conn._closed or self._conn.error is not None

    @property
    def busy(self) -> bool:
        """True while a request/response exchange holds the connection's single
        in-flight slot (hyper `Conn::is_busy`). A synchronous check so a pool can
        refuse to park a connection whose last exchange never completed — e.g. a
        release interrupted mid-teardown left the slot held; parking it would make
        the next request wait forever. Pool discipline: `if conn.closed or
        conn.busy: drop`."""
        return self._conn._busy

    def send_request(self, request: Request) -> Awaitable[Response]:
        """Send `request` and return its `Response` once the head arrives.
        Mirrors h2's `send_request` (hyper `SendRequest::send_request`).

        The request-target is sent **verbatim** (hyper's low-level contract): a
        path (``"/thing"``) is origin-form, an absolute URL (``"http://…"``) is
        absolute-form for a proxy, and an authority (``"host:port"``) is
        authority-form for CONNECT. The caller supplies the ``Host`` header (we
        never auto-add it), exactly like hyper's `client::conn::http1`.

        A failure raised before the request was handed to the writer carries
        ``request_unsent = True`` on the exception: nothing reached the wire, so
        the request — any body, streamed included — is safe to retry. The mirror
        of hyper handing the request back in `TrySendError { message: Some }`
        (client/conn/http1.rs L247-263; proto/h1/dispatch.rs L711-733). Absent
        (falsy) once the write may have begun.
        """
        return self._conn.send_request(request.method, request.target, request.headers, request.body, request.trailers)
