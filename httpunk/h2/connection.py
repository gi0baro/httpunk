"""HTTP/2 connection driver — the async half of h2's `proto::Connection` +
`proto::streams::Streams`.

`H2ConnectionBase` subclasses the Rust `H2Streams`, which owns EVERY piece of
connection and stream state under one mutex (the mirror of h2's
`Streams::inner`, an `Arc<Mutex<Inner>>`): the stream map, both flow-control
windows, the SETTINGS state, the reset store, the GOAWAY bookkeeping, the
error slot, the DATA-framing budget, the HPACK codec and the pending-frame
buffer. Every protocol decision is ONE call into it that performs the whole
check-and-act and returns a verdict. This file holds only the async machinery
— the transport, the read pump, the write pump, the body pumps and their
`select` races, the events — and acts on verdicts *after* each call returns:
the state is published inside the call, the event that announces it is set
after. No attribute here is shared mutable state (HTTPUNK_RUST_STATE_DESIGN.md).

The client `Connection` (client.py) and the server `ServerConnection`
(server.py) subclass this; the role lives in Rust (`H2Streams(role=...)`), and
what remains role-specific here is async glue only: the client's ready/slot
events, the server's accept queue.

Cross-reference: `h2 ...` comments cite hyperium/h2 0.4.19 (see
crates/vendor-h2), paths relative to its `src/`.
"""

import contextlib

from .. import _backend
from .._common import PUMP_ABANDONED, PUMP_DONE, aclose_body, aiter_body, event_result
from .._httpunk import (
    H2_FLAG_CONN_DONE,
    H2_FLAG_SLOT_FREED,
    H2_FLAG_STOP_ACCEPTING,
    H2_FLAG_WAKE,
    H2_HEADERS_HEAD,
    H2_HEADERS_OPENED,
    H2_HEADERS_TRAILERS,
    H2_PREFACE,
    H2Codec,
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as SettingsFrame,
    H2FrameStreamError as StreamErrorFrame,
    H2FrameWindowUpdate as WindowUpdate,
    H2Stopped,
    H2StreamError,
    H2Streams,
)
from ..exceptions import ConnectionClosedError, GoAwayError, H2Error, H2Reason, StreamResetError, fresh_exc
from ..http import HeaderMap
from .stream import Stream


_READ_SIZE = 65536
_UNSET = object()
# The HTTP/2 client connection preface (RFC 9113 §3.4), from the Rust core.
PREFACE = H2_PREFACE  # sentinel: no chunk buffered yet (send_body one-ahead lookahead)


class H2ConnectionBase(H2Streams):
    """The role-agnostic async driver over the Rust connection state. The role
    subclasses build the Rust base in `__new__` (its constructor takes the role and
    the hyper profile) and add their own async members in `__init__`."""

    def __init__(self, transport, *, backend=None):
        # `transport` is a caller-supplied, already-connected byte stream (BYO
        # transport, like hyper's `client`/`server` conn). Immutable after init: the
        # pumps read it, `close()` closes it — never reassigned.
        self.backend = _backend.resolve(backend)
        self._transport = transport
        # A scope for background request-body writers (client full-duplex send, F6) —
        # the one group that legitimately gets CANCELLED at teardown.
        self._write_scope = self.backend.scope()
        # Orders socket writes: the write pump and the inline flushes (`_flush`).
        self._send_lock = self.backend.lock()
        # Wakes the write pump. Set by the driver after any verdict that appended to
        # the pending buffer (`H2_FLAG_WAKE`); cleared by the pump BEFORE it takes the
        # buffer (a wake for bytes appended after the take survives the clear).
        self._write_evt = self.backend.event()

    # ----- role hooks (async glue only) -----

    def _on_slot_freed(self):
        """Client: a MAX_CONCURRENT slot was freed / the limit changed."""

    def _on_conn_done(self):
        """The connection failed or finished: wake the role's waiters."""

    def _on_stop_accepting(self):
        """Server: the graceful drain is complete."""

    def _on_request(self, st, frame, eof):
        """Server: a request stream was opened — publish it to the accept loop."""

    def _signal_ready(self):
        """Client: the peer's initial SETTINGS landed."""

    # ----- verdict acts -----

    def _after(self, flags):
        """Perform what a verdict's flags ask for — infallible, sync (design §4)."""
        if flags & H2_FLAG_WAKE:
            self._write_evt.set()
        if flags & H2_FLAG_SLOT_FREED:
            self._on_slot_freed()
        if flags & H2_FLAG_CONN_DONE:
            self._on_conn_done()
        if flags & H2_FLAG_STOP_ACCEPTING:
            self._on_stop_accepting()

    @staticmethod
    def _notify_stopped(handle, stop, notify_body):
        """The peer abandoned the stream (RST_STREAM / GOAWAY) or the connection
        died: publish `stop`, then wake the head waiter + end the body queue with the
        error (only if the body had not already ended), the sender, and the
        reset observer (`reset_received`)."""
        handle.stop = stop
        if notify_body:
            handle.headers_evt.set()
            handle.body_send.send(stop)
        handle.window_evt.set()
        handle.reset_evt.set()

    @staticmethod
    def _notify_reset(handle, stop):
        """A LOCAL reset (a caller cancel, or a library reset after the peer's
        violation): wake the head waiter, end the body queue (`stop` None = a clean
        EOF: the reader cancelled it itself), and the sender."""
        if stop is not None:
            handle.stop = stop
        handle.headers_evt.set()
        handle.body_send.send(stop)
        handle.window_evt.set()

    def _stopped_error(self, st, stop):
        """The exception for a stream that stopped (h2 `ensure_reason` + the stored
        error): a peer reset reports the PEER's reason; a GOAWAY that dropped it, the
        `GoAwayError`; a connection failure, the connection's error; a local cancel
        with no reason falls back to the connection error, then CANCEL. Every raise
        builds a fresh instance (`fresh_exc`: store copies, raise copies)."""
        if stop.reason is None and not stop.conn and st.stop is not None:
            stop = st.stop  # the entry is gone; the pump published the reason on the handle
        if stop.reason is None and not stop.conn and self.conn_error() is None and self.is_closed():
            info = self.goaway_info()
            if info is not None:
                return GoAwayError(*info)
        if stop.conn:
            if stop.reason is not None:
                info = self.goaway_info()
                if info is not None:
                    return GoAwayError(*info)
            err = self.conn_error()
            if err is not None:
                return fresh_exc(err)
            return ConnectionClosedError("connection closed")
        if stop.reason is not None:
            return StreamResetError(st.id, stop.reason)
        err = self.conn_error()
        if err is not None:
            return fresh_exc(err)
        return StreamResetError(st.id, int(H2Reason.CANCEL))

    def _raise_if_dead(self):
        """Raise the stored condition: the peer's GOAWAY first (the retry-relevant
        one; h2's `recv_eof` never overwrites an existing conn_error, F20), then the
        connection error. Copies per raise."""
        info = self.goaway_info()
        if info is not None:
            raise GoAwayError(*info)
        err = self.conn_error()
        if err is not None:
            raise fresh_exc(err) from err

    def _fail(self, exc):
        """The connection failed (h2 connection.rs `handle_poll2_result`): record a
        traceback-free copy (first writer wins) and fan out to every stream."""
        exc = fresh_exc(exc) if exc is not None else None
        for handle, stop in self.fail(exc):
            self._notify_stopped(handle, stop, True)
        self._on_conn_done()

    # ----- lifecycle -----

    async def _begin(self):
        # Shared handshake start (h2 client.rs/server.rs `handshake`): the preface
        # (client) + our initial SETTINGS + the initial WINDOW_UPDATE(0) are queued by
        # the state and flushed here, BEFORE the pumps start, so nothing can precede them.
        await self._write_scope.__aenter__()
        self.begin()
        await self._flush()
        # Bare tasks (`spawn_without_results`), never cancelled: every point they can
        # park at ends naturally on the transport close in `close()`. The handles live
        # in the state and are taken exactly once (a double `close()` cannot double-await).
        pump = self.backend.spawn_without_results(self._write_pump())
        read = self.backend.spawn_without_results(self._read_pump())
        self.store_task_handles(read, pump)

    async def _flush(self):
        """Write everything committed to the pending buffer, under the send lock (so
        wire order equals commit order against the pump)."""
        async with self._send_lock:
            data, _stopping = self.take_pending()
            if data:
                await self._transport.send_all(data)
        for handle in self.credit_written():
            handle.window_evt.set()

    async def close(self):
        # Stop the write pump by SIGNAL, flush-then-exit, and JOIN — never cancel it
        # (a cancellation landing between its take and its flush would lose control
        # frames). Then cancel + join the body writers, drain what their teardown
        # enqueued (h2 `codec.shutdown` = flush THEN shutdown, F23), shut the
        # transport down — `FramedWrite::shutdown` ends in the IO's `poll_shutdown`
        # (`close_notify` over TLS), which is also what ends the read pump's parked
        # read — join it, and wake every straggler. On a transport that already died
        # the alert cannot go out and the shutdown is the plain close hyper's drop
        # would be: wire-identical.
        self.stop_pump()
        self._write_evt.set()
        handle = self.take_pump_handle()
        if handle is not None:
            await handle
        self._write_scope.cancel()
        await self._write_scope.__aexit__(None, None, None)
        with contextlib.suppress(Exception):  # best-effort: the connection is closing regardless
            await self._flush()
        await self.backend.shutdown_transport(self._transport)
        handle = self.take_read_handle()
        if handle is not None:
            await handle
        self._fail(None)  # no-op when the pump already failed everyone (F44)

    # ----- the write pump (h2 `poll_complete` draining its frame queue) -----

    async def _write_pump(self):
        """Flush the pending-send buffer to the transport. Each wake takes EVERYTHING
        accumulated since the last flush and writes it as one send (control frames
        coalesce, h2 `poll_complete`). The clear precedes the take: an append landing
        after the take sets the event after our clear, so its wake survives. A write
        failure fails the whole connection (h2: a connection-task write error is fatal)."""
        while True:
            await self._write_evt.wait()
            self._write_evt.clear()
            failed = None
            async with self._send_lock:
                data, stopping = self.take_pending()
                if data:
                    try:
                        await self._transport.send_all(data)
                    except OSError as exc:
                        failed = ConnectionClosedError(f"connection closed: {exc}")
                    except Exception as exc:
                        failed = exc
            # The batch is on the wire (or the connection is dead): credit each
            # stream's send buffer back and wake its sender.
            for handle in self.credit_written():
                handle.window_evt.set()
            if failed is not None:
                self._fail(failed)
                break
            if stopping and not self.has_pending():
                break  # drain-then-exit; frames enqueued later are `close()`'s drain's

    # ----- the read pump (h2 proto/connection.rs `poll2` + `recv_frame`) -----

    async def _read_pump(self):
        server = self.is_server
        spare = Stream(self.backend) if server else None  # the next request's handle (§3.1)
        try:
            while True:
                data = await self._transport.receive_some(_READ_SIZE)
                if not data:  # EOF
                    self._fail(ConnectionClosedError("connection closed by peer"))
                    break
                for frame in self.receive(data):
                    try:
                        spare = self._dispatch(frame, spare)
                    except H2StreamError as se:
                        # Stream-level violation (`Error::Reset`): RST just that stream and
                        # keep going. A REMOTE-initiated reset is the peer's own RST_STREAM
                        # surfacing — h2 sends nothing back (F25).
                        if server:
                            spare = Stream(self.backend)  # the failing frame may have consumed it
                        if se.args[2] != "remote":
                            v = self.reset_on_error(se.args[0], se.args[1])
                            if v.handle is not None:
                                self._notify_reset(v.handle, v.stop)
                            self._after(v.flags)
                # After a peer GOAWAY, once every in-flight stream has finished, queue our
                # acknowledging GOAWAY(NO_ERROR) and stop serving (F23; h2 `go_away_now`).
                flags = self.maybe_goaway_reply()
                if flags:
                    self._after(flags)
                    if flags & H2_FLAG_CONN_DONE:
                        break
        except H2Error as exc:
            # A protocol/flow violation we detected: notify the peer with GOAWAY, then
            # tear down (h2 `go_away_now`). Best-effort: the connection is going down.
            reason = exc.args[0] if exc.args and isinstance(exc.args[0], int) else int(H2Reason.PROTOCOL_ERROR)
            self._after(self.send_goaway(reason))
            with contextlib.suppress(Exception):
                await self._flush()
            self._fail(exc)
        except OSError as exc:
            # A raw transport error (an abrupt peer RST, a broken pipe): a transport
            # failure, not a protocol violation — no GOAWAY.
            self._fail(ConnectionClosedError(f"connection closed: {exc}"))
        except Exception as exc:  # any other unexpected error (cancellation is BaseException)
            self._fail(exc)

    def _dispatch(self, frame, spare):
        # h2: the frame match in proto/connection.rs `recv_frame` (L518). Each arm is
        # one call into the state, then the acts its verdict asks for.
        if isinstance(frame, Headers):
            v = self.recv_headers(frame, spare)
            if v.kind == H2_HEADERS_OPENED:
                spare.id = v.stream_id
                self._on_request(spare, frame, v.eof)
                spare = Stream(self.backend)
            elif v.kind == H2_HEADERS_HEAD:
                handle = v.handle
                handle.status, handle.headers = frame.status, frame.headers
                handle.headers_evt.set()
                if v.eof:
                    handle.body_send.send(None)
            elif v.kind == H2_HEADERS_TRAILERS:
                handle = v.handle
                handle.trailers = frame.headers
                handle.body_send.send(None)  # EOF (trailers available via `.trailers`)
            if v.flags:
                self._after(v.flags)
        elif isinstance(frame, Data):
            v = self.recv_data(frame)
            handle = v.handle
            if handle is not None:
                if v.payload is not None:
                    handle.body_send.send((v.payload, v.budgeted))
                if v.eof:
                    handle.body_send.send(None)
            if v.flags:
                self._after(v.flags)
        elif isinstance(frame, WindowUpdate):
            for handle in self.recv_window_update(frame):
                handle.window_evt.set()
        elif isinstance(frame, SettingsFrame):
            initial, wake, flags = self.recv_settings(frame)
            for handle in wake:
                handle.window_evt.set()
            if flags:
                self._after(flags)
            if initial:
                self._signal_ready()  # connection fully established (client unblocks connect())
        elif isinstance(frame, Ping):
            self._after(self.recv_ping(frame))
        elif isinstance(frame, GoAway):
            aborted, flags = self.recv_go_away(frame)
            for handle, stop in aborted:
                self._notify_stopped(handle, stop, True)
            self._after(flags)
        elif isinstance(frame, RstStream):
            r = self.recv_reset(frame)
            if r is not None:
                handle, stop, notify_body, flags = r
                self._notify_stopped(handle, stop, notify_body)
                self._after(flags)
        elif isinstance(frame, StreamErrorFrame):
            # The codec detected a stream-level violation (malformed header block,
            # invalid dependency): RST just that stream (h2 `Error::library_reset`).
            raise H2StreamError(frame.stream_id, frame.error_code, "library")
        # Priority: accepted and ignored (we don't act on prioritization).
        return spare

    # ----- sending (h2 share.rs SendStream over send.rs, hyper PipeToSendStream) -----

    async def _send_body(self, st, body, trailers=None):
        """Stream a body, marking END_STREAM on the final DATA frame (or on the
        trailing HEADERS), then close the send half. A bodyless message never reaches
        here — its END_STREAM rode the HEADERS frame."""
        if isinstance(body, bytes):
            # hyper's `Full` body: one poll yields the whole chunk with `is_end_stream()`
            # set — one DATA carrying END_STREAM (or the trailers carry it, F45).
            if trailers is None:
                await self._send_data(st, body, end_stream=True)
                return
            if body:
                await self._send_data(st, body, end_stream=False)
            self._send_trailers(st, trailers)
            return
        if body is not None and not isinstance(body, bytearray) and hasattr(body, "__aiter__"):
            await self._send_async_body(st, body, trailers)
            return
        pending = _UNSET
        if body is not None:
            # A sync iterable can't park: hold one chunk back so END_STREAM rides the
            # final DATA frame.
            async for chunk in aiter_body(body):  # yields without suspending for these shapes
                if pending is not _UNSET:
                    await self._send_data(st, pending, end_stream=False)
                pending = bytes(chunk)
        if trailers is not None:
            # END_STREAM rides the trailing HEADERS frame, not the last DATA (F45).
            if pending is not _UNSET and pending:
                await self._send_data(st, pending, end_stream=False)
            self._send_trailers(st, trailers)
        elif pending is _UNSET:
            await self._send_data(st, b"", end_stream=True)  # nothing yielded: one empty END_STREAM DATA
        else:
            await self._send_data(st, pending, end_stream=True)

    async def _send_async_body(self, st, body, trailers):
        """Stream an ASYNC body and fail fast on a peer reset — hyper's `PipeToSendStream`
        (proto/h2/mod.rs), which polls `poll_reset` while it waits for the body's next
        chunk. ONE pump task per response queues each chunk; this caller waits once for
        "pump done" or "stream reset". On a reset the pump is CANCELLED (hyper dropping
        the body future) at the app's own `await` or the flow-control wait — DATA is
        queued for the write pump, never written by this task, so no write is
        interrupted. The scope exit does not wait for a CANCELLED child to unwind, so
        wait for the pump's own `done` (set in its `finally`, after the producer's
        cleanup ran)."""
        backend = self.backend
        done, box = backend.event(), []
        abandoned = False
        async with backend.scope() as scope:
            scope.spawn(self._pump_async_body(st, body, trailers, done, box))
            try:
                winner = await backend.select(event_result(done, PUMP_DONE), event_result(st.reset_evt, PUMP_ABANDONED))
                abandoned = winner is PUMP_ABANDONED and not done.is_set()
            finally:
                if not done.is_set():
                    scope.cancel()  # leave with the pump gone, whatever ended the wait
        await done.wait()
        if abandoned:
            raise self._stopped_error(st, st.stop)
        if box:
            raise box[0]

    async def _pump_async_body(self, st, body, trailers, done, box):
        # A bare task: never lets an exception escape (it is reported through `box`).
        try:
            async for chunk in body:
                await self._send_data(st, bytes(chunk), end_stream=False)
            if trailers is not None:
                self._send_trailers(st, trailers)
            else:
                await self._send_data(st, b"", end_stream=True)
        except Exception as exc:
            box.append(exc)
        finally:
            await aclose_body(body)
            done.set()

    async def _send_data(self, st, data, end_stream):
        """Queue `data` as DATA frame(s), each reserved against min(connection window,
        stream window, send-buffer room, peer max_frame_size) — h2 send.rs `send_data`
        behind `poll_capacity`. `send_data` is one locked step (reserve + encode +
        append — and, with END_STREAM on the last frame, the send half's close, as h2's;
        on the server the request's recv half goes with it, hyper dropping the
        `RecvStream`: `handle` is the reader to notify when its unread body was reset);
        "no window now" is awaited on `window_evt` with the fixed waiter idiom: try,
        clear, try again, wait (design §4)."""
        offset = 0
        while True:
            v = self.send_data(st.id, data, offset, end_stream)
            if v.stopped is not None:
                raise self._stopped_error(st, v.stopped)
            if v.flags:
                self._after(v.flags)
            if v.done:
                if v.handle is not None:
                    self._notify_reset(v.handle, v.reader_stop)
                return
            offset += v.sent
            if v.sent:
                continue
            st.window_evt.clear()
            v = self.send_data(st.id, data, offset, end_stream)  # re-check after the clear
            if v.stopped is not None:
                raise self._stopped_error(st, v.stopped)
            if v.flags:
                self._after(v.flags)
            if v.done:
                if v.handle is not None:
                    self._notify_reset(v.handle, v.reader_stop)
                return
            offset += v.sent
            if not v.sent:
                await st.window_evt.wait()

    def _send_trailers(self, st, trailers):
        """The trailing HEADERS (END_STREAM) closes the send half in the same step
        (h2 `send_trailers`); see `_send_data` for the server's `handle`."""
        v = self.send_trailers(st.id, trailers)
        if v.stopped is not None:
            raise self._stopped_error(st, v.stopped)
        if v.flags:
            self._after(v.flags)
        if v.handle is not None:
            self._notify_reset(v.handle, v.reader_stop)

    def _reset(self, st, reason, initiator="user"):
        """Abort a stream: RST_STREAM + full teardown in one locked step (h2
        `send_reset`), then the waiter wakes."""
        v = self.reset_stream(st.id, int(reason), initiator)
        if v.handle is not None:
            self._notify_reset(v.handle, v.stop)
        self._after(v.flags)

    def _aclose_body(self, st):
        v = self.aclose_body(st.id)
        if v.handle is not None:
            self._notify_reset(v.handle, v.stop)
        self._after(v.flags)

    @staticmethod
    def check_send_headers(fields):
        """RFC 9113 §8.2.2 (h2 send.rs `check_headers`): reject connection-specific
        fields in an outbound HEADERS block — a caller error (`H2UserError`), the
        connection and stream stay usable."""
        H2Codec.check_send_headers(fields)

    @staticmethod
    def _headermap(headers):
        if headers is None or isinstance(headers, HeaderMap):
            return headers
        return HeaderMap(headers)

    # ----- receiving a body (h2 share.rs RecvStream::data) -----

    async def _aiter_body(self, st):
        """Yield body chunks as they arrive; each consumed chunk releases its
        recv-window capacity (-> WINDOW_UPDATE) and, for a budgeted frame, its
        framing-budget charge (#935). The terminal item is `None` (a clean EOF) or an
        `H2Stopped` — the error the reader must raise."""
        while True:
            item = await st.body_recv.receive()
            if item is None:
                return
            if type(item) is H2Stopped:
                raise self._stopped_error(st, item)
            chunk, budgeted = item
            if budgeted:
                self.release_data_frame(st.id, len(chunk))
            self._after(self.release_capacity(st.id, len(chunk)))
            yield chunk
