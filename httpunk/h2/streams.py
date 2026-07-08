"""Streams manager — h2: proto/streams/streams.rs (+ counts.rs, send.rs, recv.rs).

Owns the set of streams, connection- and stream-level flow control, SETTINGS
application to streams, reset handling, and per-frame dispatch to individual
streams. Uses the driver (`Connection`) only for the shared codec and the raw
send path.

`StreamManager` is the **role-agnostic** core (mirrors h2's `Streams<B, P>` +
`Counts<Dyn>`, which are shared by both roles). The client/server differences are
a handful of hooks mirroring h2's `Peer` trait + `Dyn` role discriminant:
`_ensure_not_idle` (idle-stream classification), `_recv_headers_target` (a
response head on an already-open stream vs. opening a new request stream),
`_release_slot`/`_apply_peer_stream_limit` (MAX_CONCURRENT is the initiating
side's concern), and `_on_go_away`/`_on_fail` (who to wake on teardown). The
subclasses live with their role: `client.py`'s `ClientStreamManager` initiates
streams (allocates ids, bound by the peer's MAX_CONCURRENT_STREAMS);
`server.py`'s `ServerStreamManager` accepts them.

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15 (see
src/h2/UPSTREAM_VERSION), paths relative to its `src/`. This is an *adaptation*
of h2's streams layer to a coroutine model, so refs are the logic each method
mirrors, not a line-for-line port.
"""

import contextlib
import threading

from .._common import aiter_body
from .._httpunk import H2FlowControl
from ..exceptions import H2FlowControlError, H2ProtocolError, H2Reason, StreamResetError
from .settings import PeerSettings


_DEFAULT_WINDOW = 65_535
_LOCAL_MAX_ERROR_RESETS = 1024  # h2 DEFAULT_LOCAL_RESET_COUNT_MAX
_RESET_STREAM_MAX = 50  # h2 DEFAULT_RESET_STREAM_MAX (streams kept for late frames)
_RESET_STREAM_SECS = 1.0  # h2 DEFAULT_RESET_STREAM_SECS (how long to keep them)

_INVALID_CONTENT_LENGTH = object()  # sentinel: content-length header failed to parse
_UNSET = object()  # sentinel: no chunk buffered yet (send_body one-ahead lookahead)


def _is_informational(status):
    return status is not None and 100 <= status < 200


def _parse_content_length(headers):
    """First content-length value as an int, `None` if absent, or the
    `_INVALID_CONTENT_LENGTH` sentinel if it isn't a bare decimal (h2 uses
    `frame::parse_u64`, which accepts only ASCII digits)."""
    raw = headers.get("content-length")
    if raw is None:
        return None
    if not raw or not raw.isdigit():  # bytes.isdigit(): ASCII 0-9 only, non-empty
        return _INVALID_CONTENT_LENGTH
    return int(raw)


class _StreamError(Exception):
    """Internal: a stream-level protocol violation. The driver resets just that
    stream (RST_STREAM) and keeps the connection alive (h2 `Error::Reset`)."""

    def __init__(self, stream_id, reason):
        super().__init__(f"stream {stream_id} error: reason={reason}")
        self.stream_id = stream_id
        self.reason = reason


class StreamManager:
    """Role-agnostic h2 stream manager (h2 `proto::streams::Streams` + `Counts`).
    Subclassed by `ClientStreamManager` and `ServerStreamManager`, which supply
    the role hooks (see the module docstring)."""

    def __init__(self, conn):
        self._conn = conn  # driver: provides .codec, .backend, .send_frame, .error
        self._streams = {}
        # Ids we locally reset, mapped to the time of the reset. Kept briefly
        # (h2 reset_stream_duration) so late frames the peer sent before seeing
        # our RST_STREAM are swallowed instead of treated as protocol errors.
        # Bounded to _RESET_STREAM_MAX ids; aged out lazily on access.
        self._reset_streams = {}

        # Negotiated peer limits (their limits on what we send).
        self._peer = PeerSettings()
        # Per-stream recv window we advertise. Stays at the default until the
        # peer ACKs our SETTINGS, then switches to our configured value and all
        # open streams are adjusted (h2 recv.rs: `init_window_sz` +
        # `apply_local_settings`, RFC 7540 §6.9.2). Applying it earlier would let
        # the peer legitimately overrun a stream we advertised as smaller.
        self._recv_init = _DEFAULT_WINDOW

        # Connection-level flow control (h2 keeps this in the streams layer).
        # SETTINGS_INITIAL_WINDOW_SIZE does *not* affect the connection window.
        self._conn_send = H2FlowControl()
        self._conn_send.inc_window(_DEFAULT_WINDOW)
        self._conn_recv = H2FlowControl()
        self._conn_recv.inc_window(_DEFAULT_WINDOW)
        self._conn_recv.assign_capacity(_DEFAULT_WINDOW)
        self._conn_window_evt = conn.backend.event()
        # Serializes the check-and-decrement of the shared send windows so two
        # streams can't both observe capacity and over-commit the connection
        # window (which the peer would treat as a connection FLOW_CONTROL_ERROR).
        # A `threading.Lock`: the critical section is a tiny non-blocking
        # check-and-decrement, held never across an await, so it needs real
        # cross-worker-thread mutual exclusion, not a cooperative async lock.
        self._send_window_lock = threading.Lock()

        # Set to a GoAwayError once the peer sends GOAWAY: no new streams may be
        # opened, but streams <= last_stream_id keep running. `_goaway_last_id`
        # enforces that a later GOAWAY may not raise the last-stream-id.
        self._goaway = None
        self._goaway_last_id = None

        # Count of streams we've reset due to peer-caused errors; too many means
        # the peer is misbehaving -> escalate to GOAWAY (h2 local_max_error_reset).
        self._local_error_resets = 0

    # ===== role hooks (mirror h2's `Peer` trait + `Dyn` role discriminant) =====

    def _ensure_not_idle(self, stream_id):
        """Raise a connection PROTOCOL_ERROR if `stream_id` names a stream that
        was never opened (idle). The boundary differs by role (h2 `ensure_not_idle`
        against the initiating side's next id vs. the highest id seen)."""
        raise NotImplementedError

    def _recv_headers_target(self, frame):
        """Resolve the stream a HEADERS frame applies to, or None if the frame is
        fully handled (swallowed, or — server — used to open a new request). The
        default is the client's: a HEADERS only ever targets a stream we opened."""
        return self._recv_lookup(frame.stream_id)

    def _release_slot(self, st):
        """Free a MAX_CONCURRENT_STREAMS slot on close/abort. Only the initiating
        side tracks slots; the server (which gates on `len(self._streams)`) no-ops."""

    def _apply_peer_stream_limit(self):
        """Apply the peer's SETTINGS_MAX_CONCURRENT_STREAMS. It bounds only the
        *initiating* side, so only the client acts on it."""

    def _on_go_away(self, last_stream_id, exc):
        """Extra teardown when the peer GOAWAYs (after the shared last-id check)."""

    def _on_fail(self):
        """Wake role-specific waiters when the connection fails (openers on the
        client; the accept loop on the server)."""

    # ===== opening + sending (h2: streams.rs send_request/send_response, send.rs) =====

    async def send_body(self, st, body):
        """Stream a non-empty/None body, marking END_STREAM on the final DATA
        frame (h2 share.rs `SendStream::send_data` with `end_of_stream` -> send.rs
        `send_data`), then close the send half. A bodyless message never reaches
        here — its END_STREAM rode the HEADERS frame.
        """
        # Normalize bytes / sync-iter / async-iter to a stream of chunks, holding
        # one back so END_STREAM lands on the last frame instead of a separate
        # empty DATA frame.
        pending = _UNSET
        async for chunk in aiter_body(body):
            if pending is not _UNSET:
                await self._send_data(st, pending, end_stream=False)
            pending = bytes(chunk)
        if pending is _UNSET:
            # An iterable that yielded nothing -> a single empty END_STREAM DATA
            # frame. (`b""` yields one empty chunk, so it takes the else branch;
            # either way the wire result is one empty END_STREAM DATA frame.)
            await self._send_data(st, b"", end_stream=True)
        else:
            await self._send_data(st, pending, end_stream=True)
        st.state.send_close()
        self._close_stream(st)  # may already be recv-closed (fully done)

    async def _send_data(self, st, data, end_stream):
        # h2: proto/streams/send.rs `send_data` (L297) + the flow-control-gated
        # scheduling in proto/streams/prioritize.rs (we gate inline on
        # min(conn, stream) window instead of a prioritizer). END_STREAM rides
        # the final frame of `data`.
        if len(data) == 0:
            if end_stream:
                await self._conn.send_frame(self._conn.codec.serialize_data(st.id, b"", end_stream=True))
            return
        offset = 0
        while offset < len(data):
            n = await self._reserve_send_window(st, len(data) - offset)
            piece = data[offset : offset + n]
            last = end_stream and (offset + n == len(data))
            await self._conn.send_frame(self._conn.codec.serialize_data(st.id, piece, end_stream=last))
            offset += n

    async def _reserve_send_window(self, st, want):
        """Reserve up to min(conn, stream, peer max_frame_size, want) bytes of
        send window, blocking on WINDOW_UPDATE until some is available. The
        check-and-decrement is serialized (`_send_window_lock`) so concurrent
        streams can't over-commit the shared connection window."""
        while True:
            # Bail if the stream or connection died while we were parked, so a
            # sender blocked on flow control isn't stranded forever when the
            # connection fails / GOAWAYs / the stream is reset (h2 surfaces the
            # error to the parked sender). `_abort_stream`/`reset_stream`/
            # `recv_reset` set window_evt to wake us for this re-check.
            if st.error is not None:
                raise st.error
            if self._conn.error is not None:
                raise self._conn.error
            if st.state.is_closed():
                raise StreamResetError(st.id, int(H2Reason.CANCEL))
            with self._send_window_lock:
                window = self._send_window(st)
                if window > 0:
                    n = min(window, want, self._peer.max_frame_size)
                    self._conn_send.send_data(n)
                    st.send_flow.send_data(n)
                    return n
            # Blocked on flow control: wait for a WINDOW_UPDATE (a connection
            # window update also sets every stream's window_evt).
            st.window_evt.clear()
            self._conn_window_evt.clear()
            if self._send_window(st) > 0:
                continue
            await st.window_evt.wait()

    def _send_window(self, st):
        return min(self._conn_send.window_size(), st.send_flow.window_size())

    # ===== recv-side flow control (h2: proto/streams/recv.rs) =====

    async def release_capacity(self, st, n):
        """Return `n` bytes of recv capacity and emit WINDOW_UPDATE(s) when the
        reclaimed amount crosses the aggregation threshold (h2 recv model).

        h2: proto/streams/recv.rs `release_capacity` (L458, stream) +
        `release_connection_capacity` (L435); the threshold is
        FlowControl::unclaimed_capacity (proto/streams/flow_control.rs).
        """
        st.recv_unreleased = max(0, st.recv_unreleased - n)
        st.recv_flow.assign_capacity(n)
        unclaimed = st.recv_flow.unclaimed_capacity()
        # No point re-advertising a stream the peer has already finished sending.
        if unclaimed and not st.state.is_recv_end_stream():
            st.recv_flow.inc_window(unclaimed)
            await self._conn.send_frame(self._conn.codec.serialize_window_update(st.id, unclaimed))
        self._conn_recv.assign_capacity(n)
        conn_unclaimed = self._conn_recv.unclaimed_capacity()
        if conn_unclaimed:
            self._conn_recv.inc_window(conn_unclaimed)
            await self._conn.send_frame(self._conn.codec.serialize_window_update(0, conn_unclaimed))

    async def _reclaim_stream_capacity(self, st):
        # Reclaim the connection window consumed by data the app will never read
        # (h2 recv.rs `release_closed_capacity` L493 returns `in_flight_recv_data`).
        if st.recv_unreleased:
            n, st.recv_unreleased = st.recv_unreleased, 0
            await self._release_conn_capacity(n)

    async def _release_conn_capacity(self, n):
        """Return `n` bytes of connection-level recv capacity and emit a
        WINDOW_UPDATE(0) once the reclaimed amount crosses the threshold.

        h2: proto/streams/recv.rs `release_connection_capacity` (L435) +
        `ignore_data` (used when a frame's data won't reach the app).
        """
        self._conn_recv.assign_capacity(n)
        conn_unclaimed = self._conn_recv.unclaimed_capacity()
        if conn_unclaimed:
            self._conn_recv.inc_window(conn_unclaimed)
            await self._conn.send_frame(self._conn.codec.serialize_window_update(0, conn_unclaimed))

    # ===== reset (h2: proto/streams/streams.rs send_reset / recv path) =====

    async def reset_stream(self, st, error_code, initiator="user"):
        """Abort a stream we initiated: send RST_STREAM, transition the state, and
        reclaim the connection-level window consumed by buffered-but-unconsumed
        data (so the connection recovers). `initiator` labels the reset's origin
        (h2 `Initiator`): "user" for a caller cancel, "library" for a reset we
        force after detecting a peer protocol violation (`reset_on_error`).

        h2: proto/streams/streams.rs `send_reset` / share.rs `SendStream::send_reset`.
        """
        if st.state.is_closed():
            return
        st.state.set_reset(st.id, error_code, initiator)
        await self._conn.send_frame(self._conn.codec.serialize_rst_stream(st.id, error_code))
        st.headers_evt.set()  # unblock a caller still awaiting the response head
        st.body_send.send(None)  # unblock any body reader
        st.window_evt.set()  # unblock a sender parked on flow control
        await self._reclaim_stream_capacity(st)
        self._close_stream(st)
        self._enqueue_reset_expiration(st)

    async def reset_on_error(self, stream_id, reason):
        """Reset a stream after a stream-level protocol violation by the peer.
        Escalates to a connection error (GOAWAY ENHANCE_YOUR_CALM) if the peer
        provokes too many such resets (h2 local_max_error_reset_streams — the
        Rapid-Reset / malformed-flood defence).
        """
        st = self._streams.get(stream_id)
        if st is not None:
            # Count only resets of a live stream that failed during processing
            # (h2 counts in `reset_on_recv_stream_err`, streams.rs L1679; the
            # forgotten-stream STREAM_CLOSED path below is NOT counted, so a peer
            # spraying stale frames on closed streams can't trip the cap).
            self._local_error_resets += 1
            if self._local_error_resets > _LOCAL_MAX_ERROR_RESETS:
                raise H2ProtocolError(int(H2Reason.ENHANCE_YOUR_CALM), "too many stream resets")
            st.error = StreamResetError(stream_id, reason)
            # Library-initiated (we detected the peer's violation), not a user
            # cancel — the correct h2 `Initiator` for an error reset.
            await self.reset_stream(st, reason, initiator="library")
        else:
            # A stream we've already forgotten (STREAM_CLOSED). No local object to
            # tear down; still tell the peer the stream is closed.
            # h2: streams.rs recv_headers/recv_data -> Error::library_reset(id, STREAM_CLOSED).
            with contextlib.suppress(Exception):
                await self._conn.send_frame(self._conn.codec.serialize_rst_stream(stream_id, reason))

    def _enqueue_reset_expiration(self, st):
        """Record a locally-reset stream so late frames the peer sent before
        seeing our RST_STREAM are swallowed rather than mistaken for protocol
        errors. Bounded to _RESET_STREAM_MAX ids kept for _RESET_STREAM_SECS.

        h2: proto/streams/recv.rs `enqueue_reset_expiration` (L988); counts.rs
        `can_inc_num_reset_streams`.
        """
        if not st.state.is_local_error():
            return
        self._clear_expired_reset_streams()
        if len(self._reset_streams) >= _RESET_STREAM_MAX:
            return  # over the cap: h2 drops it (transitions immediately) rather than retain
        self._reset_streams[st.id] = self._conn.backend.monotonic()

    def _clear_expired_reset_streams(self):
        """Drop reset ids older than _RESET_STREAM_SECS. h2 does this each poll
        (proto/streams/recv.rs `clear_expired_reset_streams` L1030); we do it
        lazily whenever the store is touched."""
        now = self._conn.backend.monotonic()
        for sid in [s for s, at in self._reset_streams.items() if now - at > _RESET_STREAM_SECS]:
            del self._reset_streams[sid]

    # ===== SETTINGS application (h2: proto/streams/streams.rs apply_*_settings) =====

    def apply_remote_settings(self, frame):
        # h2: proto/streams/streams.rs `apply_remote_settings` (L189) +
        # send.rs `apply_remote_settings` (L478) + counts.rs (L180).
        old_iws = self._peer.update(frame)
        self._conn.codec.set_send_header_table_size(self._peer.header_table_size)
        # The peer's SETTINGS_MAX_FRAME_SIZE bounds what we serialize per frame.
        self._conn.codec.set_send_max_frame_size(self._peer.max_frame_size)
        if old_iws is not None:
            self._adjust_send_windows(old_iws, self._peer.initial_window_size)
        self._apply_peer_stream_limit()  # MAX_CONCURRENT bounds the initiating side (client)

    def apply_local_settings(self, local):
        # h2: proto/settings.rs ACK branch + proto/streams/recv.rs
        # `apply_local_settings` (L563). On the peer's ACK of our SETTINGS, our
        # advertised values take effect for *receiving*.
        if local.header_table_size is not None:
            self._conn.codec.set_recv_header_table_size(local.header_table_size)
        if local.max_frame_size is not None:
            self._conn.codec.set_max_recv_frame_size(local.max_frame_size)
        if local.max_header_list_size is not None:
            self._conn.codec.set_max_header_list_size(local.max_header_list_size)
        if local.initial_window_size is not None:
            self._adjust_recv_windows(local.initial_window_size)

    def _adjust_recv_windows(self, target):
        # RFC 7540 §6.9.2 for the *local* (recv) window: adjust every open
        # stream's recv window by the delta. h2 proto/streams/recv.rs
        # `apply_local_settings` (L590-628).
        old = self._recv_init
        self._recv_init = target
        if target == old:
            return
        for st in list(self._streams.values()):
            if target > old:
                st.recv_flow.inc_window(target - old)
                st.recv_flow.assign_capacity(target - old)
            else:
                st.recv_flow.dec_recv_window(old - target)

    def _adjust_send_windows(self, old, new):
        # RFC 7540 §6.9.2: adjust every stream's send window by the delta.
        # h2: proto/streams/send.rs `apply_remote_settings` (L478-560).
        for st in list(self._streams.values()):
            if new >= old:
                st.send_flow.inc_window(new - old)
                st.window_evt.set()
            else:
                st.send_flow.dec_send_window(old - new)

    # ===== per-frame recv dispatch (h2: proto/streams/streams.rs recv_*) =====

    def _recv_lookup(self, stream_id):
        """Resolve the target stream for an inbound HEADERS/DATA frame, or
        classify why there isn't one.

        h2: the stream lookup at the top of streams.rs `recv_headers` (L440-475)
        / `recv_data` (L550-588), i.e. `may_have_forgotten_stream` + `recv.open`
        + the `is_pending_open` / `is_local_error` checks.

        Returns the active Stream, or None if the frame must be *ignored* (a
        stream we locally reset and are still swallowing late frames for).
        Raises H2ProtocolError (-> connection GOAWAY) for a frame on an idle or
        otherwise-invalid stream, or _StreamError(STREAM_CLOSED) (-> RST just
        that stream) for a stream we opened and have since forgotten.
        """
        st = self._streams.get(stream_id)
        if st is not None:
            if st.state.is_local_error():
                # Locally reset: ignore frames "for some time" (the peer may have
                # sent trailers/data before receiving our RST_STREAM).
                return None
            return st
        reset_at = self._reset_streams.get(stream_id)
        if reset_at is not None:
            if self._conn.backend.monotonic() - reset_at <= _RESET_STREAM_SECS:
                return None  # locally reset, still within the reset-expiration window
            del self._reset_streams[stream_id]  # expired -> fall through to STREAM_CLOSED
        # A frame on an id we never opened is a connection error; an id we opened
        # and have since forgotten -> STREAM_CLOSED.
        self._ensure_not_idle(stream_id)
        raise _StreamError(stream_id, int(H2Reason.STREAM_CLOSED))

    def recv_headers(self, frame):
        # h2: proto/streams/streams.rs `recv_headers` (L421) -> recv.rs
        # `recv_headers` (L156) / `recv_trailers` (L410) / `open` (L127). The
        # target hook resolves an existing stream (both roles: head/trailers) or,
        # server-side, opens a new request stream (returning None).
        st = self._recv_headers_target(frame)
        if st is None:
            return
        if st.state.is_recv_headers():
            self._recv_response_head(st, frame)  # client response head (server streams never here)
        else:
            self._recv_trailers(st, frame)

    def _recv_response_head(self, st, frame):
        # A response head (or an interim 1xx). recv_open fully applies END_STREAM.
        informational = _is_informational(frame.status)
        st.state.recv_open(eos=frame.end_stream, informational=informational)
        if informational:
            return  # 1xx skipped (h2 poll_response); interim responses not surfaced
        self._apply_content_length(st, frame)  # may raise _StreamError
        st.status = frame.status
        st.headers = frame.headers
        st.headers_evt.set()
        if frame.end_stream:
            # recv_open already closed the recv half; do NOT call recv_close
            # again (that double transition is itself a protocol error).
            st.body_send.send(None)  # EOF
            self._close_stream(st)

    def _recv_trailers(self, st, frame):
        # A HEADERS frame after the head = trailers (h2 recv_trailers).
        if not frame.end_stream:
            # Trailers that don't set END_STREAM are malformed -> stream error.
            raise _StreamError(frame.stream_id, int(H2Reason.PROTOCOL_ERROR))
        st.state.recv_close()
        if not st.content_length_satisfied():
            raise _StreamError(frame.stream_id, int(H2Reason.PROTOCOL_ERROR))
        st.trailers = frame.headers
        st.body_send.send(None)  # EOF (trailers available via Response.trailers)
        self._close_stream(st)

    def _apply_content_length(self, st, frame):
        # h2 recv.rs `recv_headers` (L175-201): record content-length; reject a
        # non-numeric value or END_STREAM with a non-zero length (except 204/304).
        # A response to HEAD is fully exempt (h2 guards the whole block with
        # `if !stream.content_length.is_head()`, recv.rs L175). The exemptions are
        # inert for a request (is_head is False, status is None), so this is shared.
        if st.is_head():
            return
        cl = _parse_content_length(frame.headers)
        if cl is None:
            return
        if cl is _INVALID_CONTENT_LENGTH:
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))
        st.set_content_length(cl)
        if frame.end_stream and cl > 0 and frame.status not in (204, 304):
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))

    async def recv_data(self, frame):
        # h2: proto/streams/streams.rs `recv_data` (L350) -> recv.rs `recv_data`
        # (L641): validate state, consume connection + stream recv windows,
        # check content-length, deliver payload.
        sz = len(frame.data)
        try:
            st = self._recv_lookup(frame.stream_id)
        except (_StreamError, H2ProtocolError):
            # Forgotten (STREAM_CLOSED, connection survives) or idle (connection
            # dies). Either way the peer counted these bytes against the connection
            # window on the wire, so account + reclaim it (h2 `ignore_data`).
            self._conn_recv.send_data(sz)
            await self._release_conn_capacity(sz)
            raise
        if st is None:  # locally-reset stream: swallow + reclaim (h2 `ignore_data`)
            self._conn_recv.send_data(sz)
            await self._release_conn_capacity(sz)
            return
        # DATA is only valid while the recv half is streaming (h2 recv_data L653:
        # before the head or after END_STREAM is a connection error). This is
        # checked *before* consuming the connection window (matching recv.rs
        # order): the connection is torn down, so no window accounting is needed.
        if not st.state.is_recv_streaming():
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"unexpected DATA on stream {st.id}")
        # Consume the connection window (may itself overflow -> connection
        # FLOW_CONTROL_ERROR, h2 `consume_connection_window`).
        self._conn_recv.send_data(sz)
        try:
            st.recv_flow.send_data(sz)  # peer overran the *stream* window -> RST that stream
        except H2FlowControlError as exc:
            await self._release_conn_capacity(sz)  # reset after DATA -> reclaim conn window
            raise _StreamError(st.id, int(H2Reason.FLOW_CONTROL_ERROR)) from exc
        if not st.dec_content_length(sz):  # more data than content-length declared
            await self._release_conn_capacity(sz)
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))
        # On END_STREAM, verify the declared length is fully satisfied *before*
        # delivering the final chunk (h2 checks + recv_close before pushing the
        # Data event, recv.rs L705-750).
        if frame.end_stream and not st.content_length_satisfied():  # less data than declared
            await self._release_conn_capacity(sz)
            raise _StreamError(st.id, int(H2Reason.PROTOCOL_ERROR))
        st.recv_unreleased += sz
        st.body_send.send(frame.data)
        if frame.end_stream:
            st.state.recv_close()
            st.body_send.send(None)  # EOF
            self._close_stream(st)  # no-op unless the send half is also closed

    def recv_window_update(self, frame):
        # h2: proto/streams/streams.rs `recv_window_update` (L376) -> send.rs
        # `recv_connection_window_update` (L411) / `recv_stream_window_update`
        # (L421).
        if frame.stream_id == 0:
            self._conn_send.inc_window(frame.increment)
            self._conn_window_evt.set()
            for st in list(self._streams.values()):
                st.window_evt.set()
            return
        st = self._streams.get(frame.stream_id)
        if st is None:
            if frame.stream_id in self._reset_streams:
                return  # locally reset -> ignore
            self._ensure_not_idle(frame.stream_id)  # idle -> connection error
            return  # forgotten stream -> ignore
        try:
            st.send_flow.inc_window(frame.increment)
        except H2FlowControlError as exc:
            # A stream send-window overflow is a *stream* error (RST_STREAM), not
            # a connection teardown (h2 send.rs `recv_stream_window_update`).
            raise _StreamError(frame.stream_id, int(H2Reason.FLOW_CONTROL_ERROR)) from exc
        st.window_evt.set()

    async def recv_reset(self, frame):
        # h2: proto/streams/streams.rs `recv_reset` (L355); state transition =
        # state.rs `recv_reset`.
        if frame.stream_id == 0:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "RST_STREAM on stream 0")
        st = self._streams.get(frame.stream_id)
        if st is None:
            if frame.stream_id in self._reset_streams:
                return  # locally reset -> ignore
            self._ensure_not_idle(frame.stream_id)  # idle -> connection error
            return  # forgotten stream -> ignore
        st.state.recv_reset(frame.stream_id, frame.error_code, queued=False)
        st.error = StreamResetError(frame.stream_id, frame.error_code)
        st.headers_evt.set()
        st.body_send.send(None)
        st.window_evt.set()  # wake a body sender parked on flow control (it re-checks st.error)
        # Reclaim the connection window consumed by this stream's unread data
        # (h2 recv.rs `release_closed_capacity` on `transition_after`).
        await self._reclaim_stream_capacity(st)
        self._close_stream(st)

    # ===== teardown (h2: proto/streams/streams.rs handle_error / recv_eof) =====

    def _close_stream(self, st):
        """Remove a fully-closed stream and free its concurrency slot.

        h2: proto/streams/counts.rs `transition_after` / `dec_num_streams`.
        """
        if st.state.is_closed() and self._streams.pop(st.id, None) is not None:
            self._release_slot(st)

    def _abort_stream(self, st, exc):
        """Error a stream, unblock its waiters, remove it, and free its slot."""
        if st.error is None:
            st.error = exc
        st.headers_evt.set()
        st.body_send.send(None)
        st.window_evt.set()  # unblock a sender parked on flow control (it re-checks st.error)
        if self._streams.pop(st.id, None) is not None:
            self._release_slot(st)

    def handle_go_away(self, last_stream_id, exc):
        """Peer sent GOAWAY: refuse new streams; streams > last_stream_id were
        not processed (retryable); streams <= last_stream_id keep running.

        Validates last_stream_id (h2 send.rs `recv_go_away` L447): a *later* GOAWAY
        may not raise the last-stream-id above a previous one (endpoints must not
        increase it, so peers can rely on unprocessed streams being retryable) —
        that's a connection PROTOCOL_ERROR. A first GOAWAY carrying any id is
        accepted (h2 `Send::max_stream_id` starts at `StreamId::MAX`, send.rs L55),
        so the common graceful-shutdown pattern (initial GOAWAY(2^31-1) then a
        lower one) is not rejected.

        h2: proto/go_away.rs + proto/streams/streams.rs `recv_go_away`.
        """
        if self._goaway_last_id is not None and last_stream_id > self._goaway_last_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "GOAWAY may not raise last_stream_id")
        self._goaway_last_id = last_stream_id
        self._goaway = exc
        self._on_go_away(last_stream_id, exc)

    def fail_all(self, exc):
        # h2: connection-level failure/EOF fans out to every stream —
        # streams.rs `Streams::handle_error` (L362) / `recv_eof` (L386).
        for st in list(self._streams.values()):
            self._abort_stream(st, exc)
        self._on_fail()
