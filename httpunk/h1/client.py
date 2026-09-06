"""Low-level HTTP/1 client — the `http1` analogue of `h2/client.py`.

Holds the client's full role stack, mirroring `server.py`: `Connection` (the
client-side driver over the Rust `H1ClientState`, src/h1/conn.rs) and
`H1Connection` (the public per-connection handle). `H1Connection` exposes the
**same** surface as `H2Connection` — `send_request(Request) -> Response`, `ready`,
and the `request` wrapper — so a caller can treat h1 and h2 connections identically.

The state — the transport, the single in-flight slot (hyper `Conn::is_busy`), the
error slot, the idle watcher's hand-off, the background writer's scope, the peer's
version — lives in `H1ClientState` under one mutex; every decision is one call
into it. This file holds only the async machinery (HTTPUNK_RUST_STATE_DESIGN.md §3.3).

Cross-reference: hyper `client::conn::http1` (`SendRequest`/`Connection`) +
`proto/h1/{conn,dispatch,role}.rs` (the Client path).
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

from .. import _backend
from .._common import BaseClientConnection
from .._httpunk import (
    H1_WATCH_IDLE_BYTES,
    H1_WATCH_IDLE_EOF,
    H1_WATCH_IDLE_ERROR,
    H1BodyDecoder,
    H1ClientState,
    H1Codec,
)
from ..exceptions import (
    ConnectionClosedError,
    H1IncompleteMessageError,
    H1UnexpectedMessageError,
    fresh_exc,
)
from ..types import Response, Version
from .connection import H1Framing
from .share import H1ResponseBody, H1Upgraded


if TYPE_CHECKING:
    from .._backend import BackendLike
    from ..types import Request


_READ_SIZE = 65536


def _version_of(head):
    """The parsed head's version as the public `Version` (hyper role.rs L191-195)."""
    return Version.HTTP_10 if head.http10 else Version.HTTP_11


class Connection(H1Framing, H1ClientState):
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
    - `Expect: 100-continue` is a non-gap for the client: hyper hard-codes
      `expect_continue: false` (role.rs L1161). A request's own `Connection: close`
      disables reuse up front (conn.rs `encode_head` -> `connection_any_close`,
      1.11.1), ANDed with the response's keep-alive (conn.rs L294).
    - While reusable-idle, a watcher task holds read interest — the re-expression
      of hyper's always-polled `Connection` future (see `_watch_idle`) — so an
      idle peer close or stray byte flips `closed`/`error` when it happens, and
      a `send_request` failure raised before the request was handed to the
      writer carries `request_unsent = True` (hyper's `TrySendError` message
      give-back, client/conn/http1.rs L247-263).
    """

    def __new__(cls, transport, *, authority=None, backend=None):
        return H1ClientState.__new__(cls, transport)

    def __init__(self, transport, *, authority=None, backend=None):
        self.backend = _backend.resolve(backend)
        self.authority = authority
        # Wakes the slot waiters when an exchange releases the connection (the waiter
        # idiom: try, clear, try again, wait).
        self._idle_evt = self.backend.event()

    async def connect(self):
        # HTTP/1 has no connection preface / handshake (unlike h2), but hyper's
        # `Connection` future is polled from the moment it is spawned — the
        # dial-to-first-send window has read interest upstream, so it is watched
        # here too.
        await self._start_watcher()

    # ----- transport access (the state owns the transport) -----

    def write(self, data):
        transport = self.transport_ref()
        if transport is None:
            raise ConnectionClosedError("connection closed")
        return transport.send_all(data)

    def read_body_more(self):
        """Read more transport bytes for an in-flight body. Empty bytes = EOF."""
        transport = self.transport_ref()
        if transport is None:
            raise ConnectionClosedError("connection closed")
        return transport.receive_some(_READ_SIZE)

    def _close(self, transport):
        if transport is not None:
            self.backend.close_transport(transport)

    # ----- the single in-flight slot -----

    async def _acquire(self):
        """Block until the single in-flight slot is free, then claim it — `busy` and the
        claim are ONE step in the state (a pool never observes a held slot as idle)."""
        while not self.try_begin_exchange():
            self._idle_evt.clear()
            if self.try_begin_exchange():
                return
            await self._idle_evt.wait()

    def _release(self):
        if self.end_exchange():
            self._idle_evt.set()

    async def wait_idle(self):
        """Resolve once the connection can accept a request (h1 analogue of
        `SendRequest::ready`); raise if it has failed or closed."""
        await self._acquire()
        self._release()
        err = self.error
        if err is not None:
            raise fresh_exc(err) from err  # a copy per raise (exceptions.fresh_exc)
        if self.closed:
            raise ConnectionClosedError("connection closed")

    # ----- the background writer -----

    async def _teardown_writer(self, *, cancel):
        """Finish with the in-flight body writer: `cancel=True` aborts it (still
        running — an early response we didn't wait out), then joins; `cancel=False`
        just joins an already-finished writer (instant). Single-owner via the pop in
        the state: a `close()` racing the exchange's own teardown for the SAME scope
        finds nothing (a second `Scope.__aexit__` would raise)."""
        scope = self.take_writer()
        if scope is None:
            return
        await self._exit_scope(scope, cancel)

    @staticmethod
    async def _exit_scope(scope, cancel):
        if cancel:
            scope.cancel()
        await scope.__aexit__(None, None, None)

    # ----- the idle watcher -----

    async def _start_watcher(self):
        """Arm the idle watcher (see `_watch_idle`) for one idle period. Callers
        guarantee no exchange can be reading: `connect()` runs before any request,
        and `release_slot` arms it BEFORE releasing the slot (F55 single-reader). A
        no-op if the connection can serve no more requests. A handle the state refuses
        (closed meanwhile) is joined right here: the watcher exits at once."""
        done = self.backend.event()
        if not self.arm_watcher(done):
            return
        handle = self.backend.spawn_without_results(self._watch_idle(done))
        refused = self.store_watcher_handle(handle)
        if refused is not None:
            await refused

    async def _watch_idle(self, done):
        """ONE read, dispatched by connection state when it completes. Never
        cancelled or woken by a signal — a parked `receive_some` legitimately ends
        only via data, EOF, or transport close (cancellation would leave the socket's
        read registration armed for a dead task). This is also hyper's structure:
        the same poll loop owns the read across the idle->busy transition
        (dispatch.rs L287 `poll_read` -> `poll_read_keep_alive` while idle, the
        response read once busy), so there is no reader handoff to cancel.

        The state dispatches the completed read (`watcher_completed`):
        - exchange active -> the response's first bytes (or `b""` = EOF before the
          head, or a transport error): kept for `_read_head`;
        - idle EOF -> hyper's CLEAN idle close: "found EOF on idle connection,
          closing" (conn.rs L471-481): closed, NO error recorded — a pool discards
          silently; the transport is closed NOW so the socket never parks in CLOSE_WAIT;
        - idle bytes -> `new_unexpected_message` (conn.rs L484-489): poison + close;
        - idle transport error -> fail (`force_io_read` io error, conn.rs L510-520)
          — unless a concurrent `close()` already committed the flags."""
        transport = self.transport_ref()
        if transport is None:  # a racing close() took it before we ran
            self.watcher_aborted()
            done.set()
            return
        data = error = None
        try:
            data = await transport.receive_some(_READ_SIZE)
        except Exception as exc:
            error = exc.with_traceback(None)  # strip frames: don't pin the connection in a cycle
        code, transport = self.watcher_completed(data, error)
        if code == H1_WATCH_IDLE_EOF:
            self._close(transport)
        elif code == H1_WATCH_IDLE_BYTES:
            self.poison_unexpected(len(data))
        elif code == H1_WATCH_IDLE_ERROR:
            self._fail(error)
        done.set()

    async def _join_watcher(self):
        """JOIN the idle watcher (never cancel/signal it): await its completion event
        (cancellable, the handle stays in its slot), then the handle exactly once
        (already done: no suspension). Instant when none is armed. On teardown paths
        the transport is closed first, which is what ends the parked read."""
        done = self.watcher_done()
        if done is None:
            return
        await done.wait()
        handle = self.take_watcher_handle()
        if handle is not None:
            await handle

    # ----- the exchange (hyper client/conn/http1.rs SendRequest::send_request) -----

    async def send_request(self, method, url, headers, body, trailers=None):
        # hyper: client/conn/http1.rs `SendRequest::send_request` (L213) — writes
        # the head+body, reads the response. The caller must have supplied `Host`
        # (L192); we do not add it. `Conn` is busy for the duration (conn.rs L293),
        # modeled by the slot. Everything after the claim runs inside the try: no
        # failure can leak the slot (H1C-1).
        await self._acquire()
        try:
            # The watcher may have observed EOF/bytes and closed/poisoned the
            # connection while we waited — the mirror of `SendRequest::poll_ready`
            # failing once the connection task died (client/conn/http1.rs L156-180).
            # `request_unsent` mirrors hyper handing the request back
            # (`TrySendError { message: Some(req) }`, L247-263): nothing was written,
            # so the caller may safely retry ANY body, streamed included.
            if self.is_dead():
                raise self._unsent_error()
            # `require_empty_read`'s buffered-bytes fast path at send time (conn.rs
            # L463-465): bytes ALREADY DELIVERED when a request is submitted are always
            # a violation, never that request's "response". Checked BEFORE the exchange
            # is marked active, so a parked watcher's spurious wake dispatches through
            # the idle rules against an already-poisoned connection (F31, problem b).
            # (The peek runs beside a parked idle watcher by design: whichever of the two
            # gets bytes delivered before the request treats them as the violation they
            # are — the watcher through the idle rules, since the exchange is not yet
            # active — so the outcome is the same either way; a plain-socket `recv` is
            # one syscall. The TLS peek reads only already-decrypted plaintext.)
            transport = self.transport_ref()
            pending = self.backend.receive_nowait(transport, _READ_SIZE) if transport is not None else b""
            if pending:
                self.poison_unexpected(len(pending))  # records the error AND closes
                raise self._unsent_error()
            # The exchange starts NOW: the watcher is not stopped (its parked read is
            # never cancelled); the state redirects its completing read to the exchange,
            # whose `_read_head` joins it and takes the bytes as the response's first read.
            self.exchange_started()
            codec = H1Codec()
            content_length, chunked = self._body_framing(body)
            # A previous HTTP/1.0 response on this (reused) connection downgrades this
            # request to 1.0 and re-asserts keep-alive (hyper conn.rs L662-702); the codec
            # also allow-lists chunked trailers from the request's own `Trailer` header.
            head = codec.serialize_request(
                method,
                url,
                headers,
                http10=self.peer_http10,
                content_length=content_length,
                chunked=chunked,
            )
            # hyper's `poll_loop` drives reads and writes INDEPENDENTLY each turn
            # (dispatch.rs L172-211): a response head can arrive while the request
            # body is still being written, and an early response (413/401/redirect)
            # does NOT truncate the upload. So we write head+body in a DETACHED
            # background task (a per-request scope that outlives this call) and read
            # the head concurrently. The writer is NOT cancelled at head-arrival (F11);
            # `release_slot` decides its fate when the caller has finished the response.
            write_error = []
            scope = self.backend.scope()
            await scope.__aenter__()
            # The `request_unsent` boundary: this spawn is httpunk's analogue of
            # hyper's dispatcher taking the request off the channel (dispatch.rs
            # `poll_msg`). From here on a failure NEVER carries the marker.
            scope.spawn(self._write_request(codec, head, body, write_error, trailers))
            refused = self.store_writer(scope)
            if refused is not None:
                # Closed meanwhile (the idle watcher won the race for bytes/EOF that
                # landed in the send-time window and poisoned/closed the connection):
                # nobody else will tear the writer down, and the recorded verdict — the
                # unexpected-bytes poison, or the clean close — is what surfaces.
                await self._exit_scope(refused, True)
                err = self.error
                if err is not None:
                    raise fresh_exc(err) from err
                raise ConnectionClosedError("connection closed")
            resp_head = await self._read_head(codec, write_error)
            # Remember the peer's version so the next request on a reused
            # connection can fix itself up (hyper conn.rs L295).
            self.set_peer_http10(resp_head.http10)
            if resp_head.is_upgrade:
                # 101 Switching Protocols / 2xx to CONNECT: the connection stops
                # being HTTP. Hand the transport (plus any bytes already read past
                # the head — the start of the upgraded protocol) to an H1Upgraded
                # the caller owns; this driver won't touch the transport again
                # (hyper `on_upgrade` / `Connection::into_parts`). No idle watcher can
                # be running here: `_read_head` consumed it, and one is only re-armed
                # by `release_slot` on a reuse verdict, which this path never reaches.
                await self._teardown_writer(cancel=True)  # the request-body write is moot
                transport = self.upgrade()  # marks the hand-off and pops the transport: one step
                upgraded = H1Upgraded(transport, codec.take_body())
                self._release()
                body = H1ResponseBody(self, None, keep_alive=False, upgraded=upgraded)
                return Response(resp_head.status, resp_head.headers, body, version=_version_of(resp_head))
            decoder = H1BodyDecoder(resp_head.body_kind, resp_head.content_length or 0)
            decoder.feed(codec.take_body())  # body bytes already read alongside the head
            # The response's own keep-alive contribution; `release_slot` ANDs it with
            # "the request body was fully sent". A close-delimited body can never be
            # reused (hyper conn.rs L458-489); a request carrying `Connection: close`
            # is never reused, whatever the response says (hyper 1.11.1 `encode_head`
            # -> `connection_any_close` -> `disable_keep_alive`).
            resp_keep_alive = (
                resp_head.keep_alive and resp_head.body_kind != "close" and not codec.request_connection_close
            )
            # The response body owns the slot from here; it releases it (and resolves
            # the writer) when fully read or on aclose. A bodyless response has nothing
            # to read, so resolve it now (in this async context) instead.
            body = H1ResponseBody(self, decoder, keep_alive=resp_keep_alive)
            if body._needs_eager_finish:
                await body._finish()
            return Response(resp_head.status, resp_head.headers, body, version=_version_of(resp_head))
        except BaseException as exc:
            self._fail(exc)  # sync poison BEFORE the teardown suspension (see release_slot)
            try:
                await self._teardown_writer(cancel=True)
                # A watcher not yet consumed by `_read_head`: `_fail` closed the
                # transport, which is what ends its parked read — join it (never cancel).
                await self._join_watcher()
            finally:
                self._release()
            raise

    def _unsent_error(self):
        # The flag rides the raised copy, not the stored error: it holds for THIS
        # give-back, while a stored flag would leak onto unrelated raises.
        err = self.error
        exc = fresh_exc(err) if err is not None else ConnectionClosedError("connection closed")
        exc.request_unsent = True
        exc.__cause__ = err
        return exc

    async def _write_request(self, codec, head, body, write_error, trailers=None):
        # Write the head then the framed body. A write failure (e.g. the server
        # closed the read side after answering early) must not mask a response
        # that did arrive: record it so `_read_head` can still deliver the head,
        # and only surface it if no response is forthcoming. Cancellation
        # (BaseException) propagates so the scope can unwind cleanly.
        try:
            await self._send_head_and_body(codec, head, body, trailers)
            self.writer_done()
        except OSError as exc:
            # A TRANSPORT write failure (broken pipe / reset): the peer may have closed
            # right after sending an early response (413/redirect) that is still buffered
            # for `_read_head`, so DEFER — record it and surface it only if no head
            # arrives (EOF). Do NOT fail the connection, or we'd race away that buffered
            # response (F11).
            write_error.append(exc.with_traceback(None))  # stored past this frame — strip (exceptions.fresh_exc)
        except Exception as exc:
            # A BODY-ITERABLE / framing error (the caller's body generator raised, or a
            # length mismatch): the request can't complete and no valid response is
            # coming, so fail PROMPTLY (hyper fails the dispatcher). Record it and END the
            # head read by closing the transport — the sanctioned way to end a parked read
            # (never cancel it): `_read_head` sees EOF and surfaces this error (F12).
            write_error.append(exc.with_traceback(None))  # stored past this frame — strip (exceptions.fresh_exc)
            self._fail(exc)

    async def _read_head(self, codec, write_error):
        # hyper: conn.rs `can_read_head` (L175) + `read_head` -> role.rs
        # `Client::parse` (L1013), which loops past 1xx informational responses.
        data = None
        done = self.watcher_done()
        if done is not None:
            # An idle watcher is armed: its parked `receive_some` is the connection's
            # single reader (F55), so the response's FIRST read is its completing read —
            # join it and take the hand-off (bytes, `b""` EOF, or a transport error
            # re-raised here, exactly as if this task had done the read). Only after
            # the join does this task own the transport.
            await self._join_watcher()
            data, exc = self.take_watcher_result()
            if exc is not None:
                if write_error:  # the writer's failure ended the read: it is the cause
                    raise fresh_exc(write_error[0]) from write_error[0]
                raise exc
            if data is None:
                # The watcher exited without a hand-off: it dispatched through the IDLE
                # rules against this exchange (a racing close() tore the connection down,
                # or bytes/EOF landed concurrently with the exchange start). Surface the
                # recorded verdict — the unexpected-bytes poison or the clean close.
                err = self.error
                if err is not None:
                    raise fresh_exc(err) from err
                raise ConnectionClosedError("connection closed")
        while True:
            if data is None:
                transport = self.transport_ref()
                if transport is None:
                    data = b""
                else:
                    try:
                        data = await transport.receive_some(_READ_SIZE)
                    except Exception:
                        # The body writer failed and ended this read by closing the
                        # transport (on tonio a close re-dispatches the parked reader,
                        # which fails with EBADF rather than EOF): its error is the cause.
                        if write_error:
                            raise fresh_exc(write_error[0]) from write_error[0]
                        raise
            if not data:
                # EOF before a full head. If the body write also failed (server
                # closed both directions, or the body iterable raised), surface that
                # as the cause. A copy — the instance stays in `write_error`.
                if write_error:
                    raise fresh_exc(write_error[0]) from write_error[0]
                # hyper: `Parse::Eof` on a mid-message read -> `IncompleteMessage`
                # (conn.rs `read_head` L245-252) — the HTTP state expected a response.
                raise H1IncompleteMessageError("connection closed before message completed: no response head")
            head = codec.receive_head(data)
            # The codec caps a still-incomplete head at hyper's max_buf_size (io.rs
            # L202-207, enforcement tightened in 1.11.0 #4093): past it the connection
            # fails with `Parse::TooLarge`.
            if head is not None:
                return head
            data = None

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
        self._fail(
            H1UnexpectedMessageError(
                f"received unexpected message from connection: {nbytes} bytes on an idle HTTP/1 connection"
            )
        )

    async def release_slot(self, resp_keep_alive):
        """Free the in-flight slot once the caller has finished the response. Reuse
        the connection only if the response allowed keep-alive AND the request body
        was fully sent — hyper reuses only once both the read and write halves reach
        `KeepAlive` (conn.rs L370-400). If the writer is still running (the server
        answered early and the caller didn't wait out the upload), the request is
        incomplete on the wire, so cancel it and close; otherwise join it (instant).

        The reuse verdict is committed SYNCHRONOUSLY, before any suspension: an
        interruption there (cancellation, or tonio's one-shot GC unwind of an
        abandoned body generator) must not leave an open connection whose `closed`
        lies to a pool above. On reuse the idle watcher is armed BEFORE the writer
        join (also a suspension), so an interrupted join never leaves a reusable
        connection without read interest (H1C-9); the slot is released in `finally`."""
        fully_sent = self.writer_finished
        reuse = resp_keep_alive and fully_sent
        if not reuse:
            self._close(self.close_now())  # sync by design (backend.close_transport)
        try:
            if reuse:
                # The connection is reusable-idle again: restore hyper's idle read
                # interest. Started BEFORE the slot release below (a waiter resumed by
                # the release can run in parallel immediately, and the slot claim is
                # the happens-before edge that makes it observe the watcher and take
                # its read as the response's first read, F55).
                await self._start_watcher()
            await self._teardown_writer(cancel=not fully_sent)
        finally:
            self._release()

    async def close(self):
        # Commit `closed` + take the transport FIRST (one step in the state): the
        # transport close is what ends the idle watcher's parked read (it sees the
        # closed flags and exits quietly) — then join it and abort + join a
        # still-running background writer. The joins can be interrupted (they
        # suspend); a force-close never comes away with `closed` still False.
        transport, done, writer = self.mark_closed()
        self._close(transport)
        if done is not None:
            await done.wait()
            handle = self.take_watcher_handle()
            if handle is not None:
                await handle
        if writer is not None:
            await self._exit_scope(writer, True)

    def _fail(self, exc):
        # Sync poison + close, first writer wins. A stripped COPY is stored: the
        # caught instance keeps propagating to the caller and would accumulate
        # every frame above onto a stored traceback (exceptions.fresh_exc). A
        # `ConnectionClosedError` is not recorded (hyper's Io-less close: the
        # connection just closes).
        stored = None if isinstance(exc, ConnectionClosedError) else fresh_exc(exc)
        self._close(self.fail(stored))


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
        return self._conn.is_dead()

    @property
    def busy(self) -> bool:
        """True while a request/response exchange holds the connection's single
        in-flight slot (hyper `Conn::is_busy`). A synchronous check so a pool can
        refuse to park a connection whose last exchange never completed — e.g. a
        release interrupted mid-teardown left the slot held; parking it would make
        the next request wait forever. Pool discipline: `if conn.closed or
        conn.busy: drop`."""
        return self._conn.busy

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
