"""The per-stream handle the driver keeps in Python — h2: the `RecvStream` /
`SendStream` / `ResponseFuture` handles' *async* halves.

Every piece of protocol state a stream has (the vendored state machine, both
flow-control windows, the content-length, the recv ledger, the error) lives in the
Rust `H2Streams` entry keyed by `id`; this object holds only what the async runtime
needs: the events the read pump signals, the body queue it feeds, and three fields
written exactly once *before* the event that announces them (`status`/`headers`
before `headers_evt`, `trailers` before the body's EOF, `stop` before `reset_evt`
or the body's terminal item). Nothing here is ever read by two tasks without one
of those events in between (HTTPUNK_RUST_STATE_DESIGN.md §3.1).
"""


class Stream:
    __slots__ = (
        "id",
        "headers_evt",
        "window_evt",
        "reset_evt",
        "body_send",
        "body_recv",
        "status",
        "headers",
        "trailers",
        "stop",
    )

    def __init__(self, backend):
        self.id = None  # assigned by the driver from the open/recv verdict, before publication
        self.headers_evt = backend.event()  # the response head arrived (or the stream stopped)
        self.window_evt = backend.event()  # the send window / send-buffer room grew, or the stream stopped
        # The peer abandoned the stream (RST_STREAM, or a GOAWAY that dropped it), or the
        # connection died — the SEND side's observation (h2 `poll_reset`); `stop` is set first.
        self.reset_evt = backend.event()
        # Body chunks `(payload, is_budgeted)` (h2 0.4.19 `DataEvent`), then a terminal
        # item: `None` = a clean EOF, an `H2Stopped` = the error the reader must raise.
        self.body_send, self.body_recv = backend.queue()
        self.status = None
        self.headers = None
        self.trailers = None  # trailing HEADERS delivered after the body (h2 recv_trailers)
        self.stop = None  # an `H2Stopped` once the stream stopped (see `reset_evt`)

    def __repr__(self):
        return f"Stream(id={self.id})"
