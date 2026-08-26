"""HTTP/2 protocol driver — h2: proto/connection.rs.

The thin connection core: owns the transport + shared codec, runs the read-pump,
dispatches inbound frames to the stream manager, drives the SETTINGS handshake
(`settings.py`), and answers PING / handles GOAWAY. All per-stream logic and flow
control live in the stream manager (streams.py); the public request API lives in
`client.py`.

`H2ConnectionBase` is the **role-agnostic** driver, mirroring h2's single
`proto::Connection` (the codec is symmetric). The client `Connection` and the
server `ServerConnection` (server.py) subclass it; the role differences are three
hooks: the connection preface (`_before_frames` — the client *sends* it, the
server *consumes* it), the GOAWAY last-stream-id (`_goaway_last_stream_id`), and
the client-only readiness signal (`_signal_ready`).

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15 (see
src/h2/UPSTREAM_VERSION). This is an *adaptation*: h2 drives everything from one
polled `Connection` future; we use a coroutine read-pump.
"""

import contextlib
import threading

from .. import _backend
from .._httpunk import (
    H2Codec,
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as SettingsFrame,
    H2FrameStreamError as StreamErrorFrame,
    H2FrameWindowUpdate as WindowUpdate,
    H2StreamError,
)
from ..exceptions import ConnectionClosedError, GoAwayError, H2Error, H2Reason, fresh_exc
from .settings import Action
from .streams import _StreamError


_READ_SIZE = 65536

# The HTTP/2 client connection preface (RFC 7540 §3.5). A fixed, protocol-level
# constant (not HPACK, not runtime-specific) — it belongs with the h2 driver that
# sends it, not the transport backend.
PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

# h2 `StreamId::MAX` (u32::MAX >> 1). A GOAWAY carrying this as last-stream-id is the
# phase-1 "graceful, keep going" signal, not a real last-processed id.
_MAX_STREAM_ID = 2**31 - 1


class H2ConnectionBase:
    """The role-agnostic protocol driver for one transport (h2 `proto::Connection`).
    Subclassed by the client `Connection` and the server `ServerConnection`, which
    supply the connection state (`self.streams`) + the role hooks."""

    def __init__(self, transport, *, backend, codec_role, settings):
        # `transport` is a caller-supplied, already-connected byte stream (BYO
        # transport, like hyper's `client`/`server` conn). Subclasses set
        # `self.streams` after building their role state.
        self.backend = _backend.resolve(backend)
        self.codec = H2Codec(codec_role)
        self.error = None
        # The read pump is a bare task (`spawn_without_results`), never
        # cancelled: every point it can park at ends NATURALLY on the transport
        # close in `close()` — a parked `receive_some` returns EOF, an inline
        # ack/GOAWAY send (or a wait on the send lock behind one) errors on the
        # dead transport, and the pump's own handlers turn either into its exit
        # paths. `close()` joins it via this handle.
        self._read_handle = None
        # A scope for background request-body writers (client full-duplex send,
        # F6) — the one group that legitimately gets CANCELLED at teardown: an
        # interrupted DATA write on a closing connection is acceptable, the
        # connection dies with it.
        self._write_scope = self.backend.scope()
        # The control-frame write pump is a bare task, NOT a scope child: it
        # must never be cancelled — a cancellation landing between its buffer
        # swap and the flush kills it with control frames (a GOAWAY reply, an
        # RST_STREAM) in its local variable, and `close()`'s drain then finds
        # an empty buffer and sends nothing. It is stopped by signal
        # (`_write_stop` + `_write_evt`), flushes what remains, and `close()`
        # joins it via this handle (`spawn_without_results` — the cancel-free
        # seam primitive; scopes are for groups that DO get cancelled, like
        # the body writers above).
        self._pump_handle = None
        self._send_lock = self.backend.lock()
        # Control-frame pending-send buffer, flushed by `_write_pump` (spawned in
        # `_begin`) — h2's pending-send queue drained by its connection task.
        # `enqueue_frame` is SYNC, so bookkeeping + frame emission commit as one
        # uninterruptible step; the pump swaps the WHOLE buffer out and writes it
        # in one send, so control frames batch into single syscalls for free.
        # All `_write_evt` transitions happen UNDER `_write_buf_lock`, paired
        # with the buffer operation they signal: an unlocked `set()`/`clear()`
        # pair races on the free-threaded runtime (a `set()` landing between
        # the pump's wake and its `clear()` would be erased -> lost wakeup).
        self._write_buf = bytearray()
        self._write_buf_lock = threading.Lock()  # tiny critical section, never held across an await
        self._write_evt = self.backend.event()
        self._write_stop = False  # close() -> the pump drains the buffer and exits
        self._transport = transport
        self._settings = settings  # SETTINGS sync (proto/settings.rs)
        self._goaway_replied = False  # sent our acknowledging GOAWAY after a peer GOAWAY (F23)

    # ----- role hooks -----

    def _before_frames(self, data):
        """Transform received bytes before framing. Default: identity — the client
        *sends* the connection preface, so has none to strip. The server overrides
        to consume the 24-byte client preface, returning None until it's complete."""
        return data

    def _goaway_last_stream_id(self):
        """The last-stream-id for our GOAWAY. The client processes no peer-initiated
        streams -> 0; the server overrides with the last request it processed."""
        return 0

    def _signal_ready(self):
        """Client-only: unblock `connect()` once the peer's initial SETTINGS land
        (or the handshake fails). No-op on the server (no readiness gate)."""

    async def _on_pong(self, frame):
        """Received a PING ack (PONG). Base: ignore (we send no pings by default). The
        server overrides it to drive phase 2 of graceful shutdown — its shutdown PING's
        ack triggers the final GOAWAY with the real last-processed stream id."""

    # ----- lifecycle -----

    async def _begin(self, preface, settings):
        # Shared handshake start (h2 client.rs/server.rs `handshake`): open the
        # scope, flush the connection preface (client: 24-byte preface; server:
        # empty) + our initial SETTINGS, and spawn the read-pump.
        await self._write_scope.__aenter__()
        await self.send_frame(preface + self.codec.serialize_settings(**settings))
        # The control-frame write pump starts only AFTER the preface is on the wire,
        # so enqueued frames (starting with the initial WINDOW_UPDATE below) can
        # never precede it; the queue is FIFO, so wire order = enqueue order.
        self._pump_handle = self.backend.spawn_without_results(self._write_pump())
        # Advertise our (larger-than-default) connection recv window right after the
        # preface, before any peer data (h2 sends this initial WINDOW_UPDATE(0) as
        # part of connection setup, from `initial_connection_window_size`).
        await self.streams.raise_connection_window()
        self._read_handle = self.backend.spawn_without_results(self._read_pump())

    async def close(self):
        # Close the transport *first*: the read-pump is almost always parked in
        # `transport.receive_some`, and closing makes that return EOF so the pump
        # exits on its own — deterministic, never via cancellation: every point
        # the pump can park at ends naturally once the transport is closed (a
        # parked `receive_some` returns EOF; an inline ack/GOAWAY send — or a
        # wait on the send lock behind one — errors on the dead transport, and
        # the pump's handlers turn either into its exit paths). The EOF also
        # unblocks the peer's read loop.
        # Stop the write pump by SIGNAL, flush-then-exit, and JOIN — never
        # cancel it: a cancellation landing between the pump's buffer swap and
        # its flush would kill it with control frames (the GOAWAY reply of the
        # F23 clean close, an RST_STREAM) already swapped into its local — the
        # drain below would then find an empty buffer and the frames would
        # silently never reach the wire (that exact race flaked the
        # GOAWAY-reply tests). The stop flag is set under the buffer lock,
        # paired with the wake, so the pump observes it on that wake.
        with self._write_buf_lock:
            self._write_stop = True
            self._write_evt.set()
        handle, self._pump_handle = self._pump_handle, None
        if handle is not None:
            await handle
        # The body writers ARE cancelled (an interrupted DATA write on a closing
        # connection is acceptable — the connection dies with it), then joined.
        self._write_scope.cancel()
        await self._write_scope.__aexit__(None, None, None)
        # Flush anything enqueued after the pump exited (e.g. RST_STREAMs from
        # writer teardown above) — h2 likewise drains its pending-send queue
        # before shutdown. Unconditional on `self.error`: h2's `State::Closing`
        # runs `codec.shutdown` = flush THEN shutdown on every non-IO close
        # (connection.rs L304-310), including the GOAWAY-exchanged clean close
        # (F23). Best-effort: the connection is closing regardless, so a send
        # failure (a dead transport — h2's Io branch, which skips straight to
        # Closed) is ignored.
        if self._transport is not None:
            with contextlib.suppress(Exception):
                async with self._send_lock:
                    pending = self._take_pending()
                    if pending:
                        await self._transport.send_all(pending)
        if self._transport is not None:
            self.backend.close_transport(self._transport)
        handle, self._read_handle = self._read_handle, None
        if handle is not None:
            await handle
        # Guarantee every straggler waiter is woken. The pump calls `fail_all`
        # itself when the close-induced EOF (or send error) reaches it, and the
        # join above makes this run strictly after — so this is a no-op when the
        # pump already failed everyone (streams are popped as they're aborted),
        # and wakes any straggler a pump exit path missed (F44).
        self.streams.fail_all(self.error or ConnectionClosedError("connection closed"))

    async def send_frame(self, data):
        # Drain-then-send under the one send lock: control frames committed
        # (enqueued) before we acquired the wire go out first, so wire order =
        # commit order — h2's single-queue property (a GOAWAY thus follows the
        # RSTs committed before it, like h2 flushing its pending queue). See
        # the design note below.
        async with self._send_lock:
            await self._transport.send_all(self._take_pending() + data)

    # Design note (why three send paths, vs h2's single frame queue): in the h2
    # crate ALL socket I/O belongs to the one connection task — user handles only
    # mutate the store and wake it, frames are queued under the store mutex, and a
    # partially-written frame is resumed on the next poll. That structure makes two
    # whole failure classes unrepresentable there: (a) a state change committed
    # whose frame never goes out (the committing future was cancelled between
    # commit and write), and (b) a truncated frame left on the wire desyncing the
    # peer's framing. httpunk instead writes inline from (cancellable) user tasks
    # — deliberate: no send-data buffering (F41 relies on it) and natural TCP
    # backpressure on the uploader — so those two classes exist HERE and are
    # handled explicitly:
    #
    # - CONTROL frames whose loss silently diverges shared ledgers or strands the
    #   peer (WINDOW_UPDATE, RST_STREAM) go through `enqueue_frame`: the sync
    #   enqueue makes commit+emission one uninterruptible step, and the write pump
    #   (never user-cancelled) does the writing — the h2-queue property for the
    #   frames that need it.
    # - STREAM frames (HEADERS/DATA/trailers) stay inline via `send_frame_or_fail`,
    #   which poisons the connection on ANY interruption mid-write: a possibly
    #   half-written frame means framing integrity can no longer be proven, so we
    #   fail loudly ourselves instead of letting the peer discover the desync.
    # - Connection-lifecycle frames (preface/SETTINGS, GOAWAY, acks) keep plain
    #   `send_frame`: they run in the pump/handshake/shutdown, not under user
    #   cancellation, and GOAWAY needs flushed-before-stop semantics a queue would
    #   complicate. One exception: the idle-after-peer-GOAWAY reply
    #   (`_maybe_goaway_reply`) is queued, because its trigger can be a sync stream
    #   close in user-task context; `close()` drains the queue before the FIN, so
    #   flushed-before-stop still holds.
    #
    # ORDERING INVARIANT (the property h2's single queue gives for free, restored
    # here for the hybrid): a control frame committed to the pending buffer before
    # a sender acquires `_send_lock` reaches the wire before that sender's bytes.
    # Two pieces enforce it, and both are load-bearing:
    # - inline senders (`send_frame`, `send_frame_or_fail`) drain the buffer via
    #   `_take_pending` and write it FIRST, under the send lock they hold;
    # - the pump swaps the buffer INSIDE `_send_lock` (`_write_pump`), so a batch
    #   swapped out but not yet written can never coexist with an available lock
    #   (or an inline sender slipping in between swap and flush would still
    #   invert the order).
    # Without it, the RST/WINDOW_UPDATE a dispatch enqueues can lose the wire
    # race to a response committed AFTER it (h2 guarantees the reverse), which is
    # peer-observable: the forgotten-stream RST arriving after the next stream's
    # response flaked F17 in CI. Lock order is send -> buf everywhere; the buffer
    # lock is a sync leaf, never held across an await. NOT covered (unchanged,
    # matches the pre-existing hybrid semantics): two INLINE sends serialize in
    # lock-acquisition order, not state-commit order.
    #
    # The full-fidelity alternative — port h2's buffered-send/`reserve_capacity`
    # model and route DATA through the queue too — remains open as a future step;
    # this hybrid was chosen to keep F41's zero-buffering and the inline hot path.

    def enqueue_frame(self, data):
        """Append a control frame to the pending-send buffer and wake the write
        pump — synchronous, so a caller can commit its bookkeeping (window
        ledgers, stream state) and the frame's emission as ONE uninterruptible
        step: no cancellation can land between a committed ledger and the wire
        any more (the punkreq class of bug)."""
        with self._write_buf_lock:
            self._write_buf += data
            self._write_evt.set()

    def _take_pending(self):
        """Swap out everything committed to the pending-send buffer. MUST be
        called while holding `_send_lock` — writing what you take while still
        holding it is what makes wire order equal commit order (see the design
        note above). Deliberately does NOT clear `_write_evt`: only the pump
        clears, in the same locked section where it observes `_write_stop` — a
        drain that cleared here could erase `close()`'s paired stop+wake and
        hang the pump join. The cost is one spurious empty pump wake."""
        with self._write_buf_lock:
            data, self._write_buf = bytes(self._write_buf), bytearray()
        return data

    async def _write_pump(self):
        """Flush the pending-send buffer to the transport. Runs as a bare task
        (`_pump_handle`) and is never cancelled (see `close()` — a cancel landing between the
        swap and the flush destroys swapped-out control frames): `close()` sets
        `_write_stop` + wakes it, and the pump drains the buffer to empty
        before exiting. Each wake swaps out EVERYTHING accumulated since the
        last flush and writes it as one send — control frames coalesce into
        single syscalls, exactly h2's poll_complete draining its pending queue.
        The swap and the event `clear()` happen together under the buffer lock,
        mirroring `enqueue_frame`'s append+`set()`: an enqueue lands either
        before the swap (its bytes taken, its wake consumed — nothing pending)
        or after the clear (its wake survives for the next iteration) — an
        unlocked clear could erase a wake for bytes not yet swapped (lost
        wakeup on the free-threaded runtime). A write failure fails the whole
        connection — same as h2, where a connection-task write error is fatal.

        The swap happens INSIDE `_send_lock`: a batch swapped out but not yet
        written must not coexist with an available send lock, or an inline
        sender could slip its (later-committed) frame onto the wire ahead of
        it — the second half of the ordering invariant (the inline senders'
        `_take_pending` drain is the first). Lock order is send -> buf
        everywhere; the buffer lock is a leaf, never held across an await."""
        while True:
            await self._write_evt.wait()
            failed = None
            async with self._send_lock:
                with self._write_buf_lock:
                    data, self._write_buf = bytes(self._write_buf), bytearray()
                    self._write_evt.clear()
                    stopping = self._write_stop
                if data:
                    try:
                        await self._transport.send_all(data)
                    except OSError as exc:
                        failed = ConnectionClosedError(f"connection closed: {exc}")
                    except Exception as exc:
                        failed = exc
            if failed is not None:
                self._fail(failed)
                break
            if stopping:
                # Drain-then-exit: anything enqueued during the send above left
                # the event set (its `set()` came after our `clear()`), so loop
                # once more; exit only with the buffer empty. Frames enqueued
                # after that are `close()`'s drain's to send.
                with self._write_buf_lock:
                    if not self._write_buf:
                        break

    async def send_frame_or_fail(self, data):
        """Send a stream frame (HEADERS/DATA/trailers) inline; any interruption of
        the write itself — error, cancellation, GC unwind — POISONS the connection.
        `send_all` loops per-syscall, so an interruption may leave a truncated
        frame on the wire; every later frame would then be parsed mid-frame by the
        peer. We cannot tell "nothing written" from "half written", so fail the
        connection ourselves (h2 equivalent: write errors are connection-fatal;
        partial writes are unrepresentable — the connection task resumes them).
        Cancellation while WAITING for the send lock is safe (nothing written) and
        does not poison.

        Drains the pending control buffer first (under the lock — wire order =
        commit order, see the design note): the RST/WINDOW_UPDATE a dispatch
        committed before this frame can no longer lose the wire race to it
        (the rsts=0 flake in the forgotten-stream test). An interruption then
        also loses the drained control frames — moot, since every branch below
        poisons the connection they belonged to."""
        async with self._send_lock:
            data = self._take_pending() + data
            try:
                await self._transport.send_all(data)
            except BaseException as exc:
                if isinstance(exc, OSError):
                    self._fail(ConnectionClosedError(f"connection closed: {exc}"))
                elif isinstance(exc, Exception):
                    self._fail(exc)
                else:  # cancellation / GC unwind: the frame may be half-written
                    self._fail(ConnectionClosedError("connection poisoned: frame write interrupted"))
                if self._transport is not None:  # unblock the peer + our read pump
                    self.backend.close_transport(self._transport)
                raise

    def _maybe_goaway_reply(self):
        """If the peer has GOAWAY'd us with a REAL last-stream-id and no streams remain,
        queue our acknowledging GOAWAY(NO_ERROR, last-processed-id) exactly once and
        mark the connection DONE (F23). No-op otherwise; returns whether it fired.

        h2: proto/connection.rs `poll` (L287-295) — `go_away_now(NO_ERROR)` once
        `error.is_some() && !has_streams()`, after which `should_close_now()` makes
        the connection future resolve `Ready(Ok(()))`: sending the reply and being
        done are ONE step. The "done" half here is `_fail`, the fan-out every other
        pump exit gets (server: end the accept loop so the caller's `__aexit__`
        closes the transport; client: wake openers). `fail_all` aborts nothing
        (no streams remain) and `error` is set so later use fails as closed — the
        client still reports `GoAwayError` first (`open_stream`/`wait_until_ready`
        check `_goaway` before `error`, as h2's conn_error is never overwritten).

        Two triggers, like h2's connection task being woken both by inbound frames
        and by a stream closing (streams.rs `drop_stream_ref` L1647): the read pump
        after a batch, and `_close_stream`/`_abort_stream` when the last stream
        goes. The latter is sync user-task context (a `respond()`/`aclose()`
        finishing), so the frame goes through `enqueue_frame` — exactly h2's
        `GoAway.pending` queue; `close()` drains it before the FIN (codec.shutdown).

        A phase-1 graceful GOAWAY (`last_stream_id == 2^31-1`) is explicitly NOT a
        trigger — it means "I'm shutting down, keep your in-flight work going" and is
        followed by a shutdown PING then the real GOAWAY. Reacting to it by stopping the
        pump would skip answering that PING and stall the peer's two-phase graceful
        (h2's `should_close_on_idle` excludes `StreamId::MAX` for the same reason)."""
        if (
            self._goaway_replied
            or self.error is not None
            or self.streams._goaway is None
            or self.streams._streams
            or self.streams._goaway_last_id is None
            or self.streams._goaway_last_id >= _MAX_STREAM_ID
        ):
            return False
        self._goaway_replied = True
        self.enqueue_frame(self.codec.serialize_go_away(self._goaway_last_stream_id(), int(H2Reason.NO_ERROR)))
        self._fail(ConnectionClosedError("connection closed: GOAWAY exchanged"))
        return True

    # ----- inbound -----

    async def _read_pump(self):
        # h2: the read side of proto/connection.rs `poll2` (L318) — pull frames
        # from the transport and dispatch. (We loop in a coroutine; h2 polls.)
        try:
            while True:
                data = await self._transport.receive_some(_READ_SIZE)
                if not data:  # EOF
                    self._fail(ConnectionClosedError("connection closed by peer"))
                    break
                data = self._before_frames(data)  # server strips the client preface
                if data is None:
                    continue  # preface not complete yet
                for frame in self.codec.receive(data):
                    try:
                        await self._dispatch(frame)
                    except _StreamError as se:
                        # Stream-level violation: RST just that stream, keep going.
                        await self.streams.reset_on_error(se.stream_id, se.reason)
                    except H2StreamError as se:
                        # Stream-level error from the Rust state machine (`Error::Reset`):
                        # args = (stream_id, reason, initiator). A REMOTE-initiated reset is
                        # the peer's own RST_STREAM surfacing — h2 returns Ok and sends
                        # nothing (connection.rs L448-462); echoing it back (and counting it
                        # toward the ENHANCE_YOUR_CALM cap) is wrong. Only send library/user
                        # resets (F25).
                        if se.args[2] != "remote":
                            await self.streams.reset_on_error(se.args[0], se.args[1])
                # After a peer GOAWAY, once every in-flight stream has finished, queue our
                # acknowledging GOAWAY(NO_ERROR, last-processed-id) and stop serving —
                # h2's poll loop does exactly this (`go_away_now(NO_ERROR)` once
                # `error.is_some()` and `!has_streams()`, connection.rs L287-295), rather
                # than lingering until the peer closes the socket (F23). The connection is
                # DONE (`_fail` inside): the accept loop / openers are woken, and the
                # transport's FIN follows on the caller's `__aexit__` → `close()`, which
                # drains the queued GOAWAY first (httpunk's user-driven lifecycle).
                if self._maybe_goaway_reply():
                    break
        except H2Error as exc:
            # A protocol/flow violation we detected (bad state, HPACK/CONTINUATION,
            # flow control, bad preface): notify the peer with GOAWAY, then tear down.
            await self._send_goaway(exc)
            self._fail(exc)
        except OSError as exc:
            # A raw transport error — an abrupt peer RST (ConnectionResetError), a
            # broken pipe, etc. Surface it as a clean `ConnectionClosedError` (a
            # transport failure, not a protocol violation, so no GOAWAY) rather than a
            # bare socket errno (h2 maps Io errors this way; cf. the explicit EOF branch
            # above). It is protocol-neutral (an `HTTPunkError`, not an `H2Error`), so it
            # is NOT caught by the `except H2Error` GOAWAY branch above — correctly, since
            # you can't send GOAWAY on a dead transport.
            self._fail(ConnectionClosedError(f"connection closed: {exc}"))
        except Exception as exc:  # any other unexpected error (cancellation is BaseException)
            self._fail(exc)

    async def _send_goaway(self, exc):
        # h2: proto/connection.rs `go_away_now` (L409) / `go_away_now_data` (L415).
        # The last-stream-id is role-specific (client: 0, no peer-initiated streams;
        # server: the last request it processed). Reason comes from the error.
        reason = exc.args[0] if exc.args and isinstance(exc.args[0], int) else int(H2Reason.PROTOCOL_ERROR)
        # Best effort: the connection is going down regardless of whether this lands.
        with contextlib.suppress(Exception):
            await self.send_frame(self.codec.serialize_go_away(self._goaway_last_stream_id(), reason))

    async def _dispatch(self, frame):
        # h2: the frame match in proto/connection.rs `recv_frame` (L518).
        if isinstance(frame, SettingsFrame):
            await self._on_settings(frame)
        elif isinstance(frame, Headers):
            self.streams.recv_headers(frame)
        elif isinstance(frame, Data):
            await self.streams.recv_data(frame)
        elif isinstance(frame, WindowUpdate):
            self.streams.recv_window_update(frame)
        elif isinstance(frame, Ping):
            if frame.ack:
                await self._on_pong(frame)  # a PING ack — drives graceful phase 2 on the server
            else:
                await self.send_frame(self.codec.serialize_ping_ack(frame.data))
        elif isinstance(frame, GoAway):
            # Graceful: streams <= last_stream_id keep running; new/higher ones
            # are refused. The pump continues until the peer closes (EOF).
            self.streams.handle_go_away(
                frame.last_stream_id,
                GoAwayError(frame.last_stream_id, frame.error_code, frame.debug_data),
            )
        elif isinstance(frame, RstStream):
            await self.streams.recv_reset(frame)
        elif isinstance(frame, StreamErrorFrame):
            # The codec detected a stream-level violation (malformed header block,
            # invalid dependency): RST just that stream (h2 `Error::library_reset`).
            raise _StreamError(frame.stream_id, frame.error_code)
        # Priority: accepted and ignored (we don't act on prioritization).

    async def _on_settings(self, frame):
        # h2: proto/settings.rs `recv_settings` (L41) drives the sync; the value
        # application is delegated to the streams manager.
        action, payload = self._settings.recv_settings(frame)
        if action is Action.APPLY_LOCAL:
            self.streams.apply_local_settings(payload)
        elif action is Action.ACK_AND_APPLY:
            await self.send_frame(self.codec.serialize_settings_ack())
            peer_frame, is_initial = self._settings.take_remote()
            self.streams.apply_remote_settings(peer_frame)
            if is_initial:
                self._signal_ready()  # connection fully established (client unblocks connect())

    def _fail(self, exc):
        # h2: proto/connection.rs `handle_poll2_result` (L430) -> the connection
        # error fans out to every stream via streams.rs `Streams::handle_error` (L362).
        # Store/fan out a stripped COPY (h2 clones an Arc'd error the same way):
        # some callers keep propagating the caught instance after this, which would
        # accumulate their frames onto a stored traceback (exceptions.fresh_exc).
        # Sharing the one copy across conn + streams is fine — raise sites raise
        # copies of it, so its traceback never grows.
        exc = fresh_exc(exc)
        self.error = exc
        self.streams.fail_all(exc)
        self._signal_ready()  # unblock connect() if the handshake never completed (client); no-op (server)
