"""Per-stream data — h2: proto/streams/stream.rs (the `Stream` struct).

Pure state: the vendored stream state machine, send/recv flow-control windows,
a body queue, and the events the driver/manager signal. All protocol logic
lives in the vendored Rust core; this just holds a stream's mutable pieces.

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15 (see
src/h2/UPSTREAM_VERSION), paths relative to its `src/`.
"""

from .._httpunk import H2ContentLength, H2FlowControl, H2StreamState


class Stream:
    # h2: proto/streams/stream.rs `Stream`. `state` = state.rs `State::default`;
    # `send_flow`/`recv_flow` start at the peer/local initial window sizes.
    def __init__(self, stream_id, backend, *, send_window, recv_window, is_head=False):
        self.id = stream_id
        self.state = H2StreamState()
        # Send window = what the peer lets us send; recv window = what we
        # advertise to the peer.
        self.send_flow = H2FlowControl()
        self.send_flow.inc_window(send_window)
        self.recv_flow = H2FlowControl()
        self.recv_flow.inc_window(recv_window)
        self.recv_flow.assign_capacity(recv_window)
        self.status = None
        self.headers = []
        self.trailers = None  # trailing HEADERS delivered after the body (h2 recv_trailers)
        self.headers_evt = backend.event()
        self.window_evt = backend.event()  # send window grew
        self.body_send, self.body_recv = backend.queue()
        self.error = None
        # The peer abandoned the stream (RST_STREAM, or a GOAWAY that dropped it), or the
        # connection died — observable by the SEND side independently of `error`, which
        # the body READER checks and which stays unset once END_STREAM was received
        # (the received message stood). h2 state.rs `ensure_reason` behind
        # `SendResponse::poll_reset` / `SendStream::poll_reset`: `reset_reason` is the
        # RST_STREAM / GOAWAY reason (int) when there is one, None for other closures
        # (a connection error — `error` / the connection's error carries it).
        self.reset_evt = backend.event()
        self.reset_reason = None
        # DATA payload bytes queued for this stream but not yet written by the connection's
        # write pump (h2 stream.rs `buffered_send_data`); bounded by `max_send_buf_size`.
        self.send_buffered = 0
        self.holds_slot = False  # whether this stream holds a MAX_CONCURRENT permit
        # Bytes received but not yet released to the peer (via WINDOW_UPDATE);
        # reclaimed at connection level if the stream is cancelled/reset/closed
        # (h2 recv.rs `in_flight_recv_data` / `release_closed_capacity`).
        self.recv_unreleased = 0
        # Outstanding DATA-framing budget charged for this stream's buffered-but-
        # unconsumed small budgeted frames (sum of `THRESHOLD - len` per frame).
        # h2 0.4.19 releases the budget of undelivered frames when the recv
        # buffer is cleared (recv.rs `clear_recv_buffer` L959-976 iterates the
        # buffer, `release_closed_capacity` L502); our body queue can't be
        # drained synchronously through the backend seam, so this counter
        # carries the same information (runtime-forced re-expression). Guarded
        # like `recv_unreleased` (F22): the reader's per-chunk release and the
        # teardown's bulk release can never double-credit.
        self.data_budget_charged = 0
        # Set once `_reclaim_stream_accounting` has returned this stream's in-flight recv
        # data to the connection window (on reset/abort). A later `release_capacity`
        # for the same buffered-but-unread bytes must then be a no-op, or it would
        # credit the connection window twice (F22).
        self.recv_reclaimed = False
        # Declared response body length, decremented per DATA and checked at EOS
        # (h2 proto/streams/stream.rs `ContentLength`, in Rust): `Head` for a HEAD
        # request (no body regardless of the header); `Omitted` until a header is seen.
        self.content_length = H2ContentLength(is_head=is_head)
