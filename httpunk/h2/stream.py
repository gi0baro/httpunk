"""Per-stream data — h2: proto/streams/stream.rs (the `Stream` struct).

Pure state: the vendored stream state machine, send/recv flow-control windows,
a body queue, and the events the driver/manager signal. All protocol logic
lives in the vendored Rust core; this just holds a stream's mutable pieces.

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15 (see
src/h2/UPSTREAM_VERSION), paths relative to its `src/`.
"""

from .._httpunk import H2FlowControl, H2StreamState


# Sentinels for `Stream.content_length` (h2 proto/streams/stream.rs `ContentLength`).
_CL_OMITTED = object()  # no content-length header (ContentLength::Omitted)
_CL_HEAD = object()  # response to a HEAD request — never has a body (ContentLength::Head)


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
        self.holds_slot = False  # whether this stream holds a MAX_CONCURRENT permit
        # Bytes received but not yet released to the peer (via WINDOW_UPDATE);
        # reclaimed at connection level if the stream is cancelled/reset/closed
        # (h2 recv.rs `in_flight_recv_data` / `release_closed_capacity`).
        self.recv_unreleased = 0
        # Declared response body length, decremented per DATA and checked at EOS
        # (h2 proto/streams/stream.rs `ContentLength`). `_CL_HEAD` for a HEAD
        # request (no body regardless of the header); `_CL_OMITTED` until seen.
        self.content_length = _CL_HEAD if is_head else _CL_OMITTED

    def is_head(self):
        # A response to a HEAD request is exempt from all content-length handling
        # (h2 stream.rs `ContentLength::is_head`).
        return self.content_length is _CL_HEAD

    def set_content_length(self, value):
        # Record a parsed content-length (h2 recv_headers). No-op for HEAD.
        if self.content_length is not _CL_HEAD:
            self.content_length = value

    def dec_content_length(self, n):
        """Consume `n` body bytes; return False on overflow (more data than the
        declared content-length). h2 stream.rs `dec_content_length`."""
        cl = self.content_length
        if cl is _CL_HEAD or cl is _CL_OMITTED:
            return True
        if n > cl:
            return False
        self.content_length = cl - n
        return True

    def content_length_satisfied(self):
        """True unless a declared content-length is still non-zero at end of
        stream (underflow). h2 stream.rs `ensure_content_length_zero`."""
        cl = self.content_length
        if cl is _CL_HEAD or cl is _CL_OMITTED:
            return True
        return cl == 0
