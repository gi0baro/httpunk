"""httpunk's error taxonomy.

Everything derives from `HTTPunkError`, the neutral root. `ConnectionClosedError`
is protocol-neutral (raised on both HTTP/1 and HTTP/2); every other subtype is
HTTP/2-specific and shares the `H2Error` sub-base (defined in Rust, so the
state-machine / flow-control / API-misuse errors raised by the extension share it):

    HTTPunkError
    ├── ConnectionClosedError     transport closed/IO error with work in flight (HTTP/1 + HTTP/2; Rust)
    └── H2Error                   base for HTTP/2 protocol errors (Rust)
        ├── H2ProtocolError       connection-level protocol violation (-> GOAWAY; Rust)
        ├── H2StreamError         stream-level protocol violation (-> RST_STREAM; Rust)
        ├── H2UserError           local API misuse (from the Rust state machine)
        ├── H2FlowControlError    flow-control window over/underflow (Rust)
        ├── GoAwayError           peer sent GOAWAY
        └── StreamResetError      peer sent RST_STREAM for a stream

`error_code` attributes are `H2Reason` members for known codes (an `IntEnum`, so
they compare equal to ints), or a plain int for codes outside the RFC set.

An HTTP/1 `send_request` failure raised before the request was handed to the
writer carries `request_unsent = True` on the exception instance (whatever its
type — `ConnectionClosedError`, or the unexpected-bytes poison error): nothing
reached the wire, so the request is safe to retry with any body, streamed
included. The mirror of hyper's `TrySendError { message: Some(request) }`
give-back (client/conn/http1.rs L247-263; proto/h1/dispatch.rs L711-733). Read
it with `getattr(exc, "request_unsent", False)` — it is absent once the write
may have begun.
"""

from __future__ import annotations

import copy as _copy

from ._httpunk import (
    ConnectionClosedError as ConnectionClosedError,
    H2Error as H2Error,
    H2FlowControlError as H2FlowControlError,
    H2ProtocolError as H2ProtocolError,
    H2Reason as H2Reason,
    H2StreamError as H2StreamError,
    H2UserError as H2UserError,
    HTTPunkError as HTTPunkError,
)


def _reason(code):
    try:
        return H2Reason(code)
    except ValueError:
        return code  # unknown/experimental error code — keep the raw int


def fresh_exc(exc: BaseException) -> BaseException:
    """A fresh, traceback-free copy of `exc`, for storing or re-raising a saved
    connection/stream error.

    Runtime-forced divergence from hyper: hyper shares a connection's fatal error
    by cloning an `Arc<Error>`; a Python exception instance instead accumulates
    every propagation's frames onto its `__traceback__`, and a traceback's frames
    keep their locals alive — so a caught exception stored on a long-lived object
    (conn, stream, backend transport) and re-raised as the same shared instance
    pins the entire frame graph of every propagation in a reference cycle only
    cyclic GC can free (observed as stalled-until-gen-2-GC connections downstream).
    The rule throughout httpunk: **store copies, raise copies** — store
    `fresh_exc(exc)` (or strip a consumed-once hand-off in place), and re-raise
    stored errors as `raise fresh_exc(err) from err`, so no stored instance ever
    carries frames.

    Copies via `copy.copy`: `BaseException.__reduce__` rebuilds from type + args
    and restores `__dict__` extras (e.g. `request_unsent`); `GoAwayError` /
    `StreamResetError` define `__reduce__` because their args don't roundtrip
    through their constructors. The copy carries no `__traceback__` / `__cause__` /
    `__context__`. An uncopyable exception is returned as-is — degraded (shared
    instance) but functional."""
    try:
        return _copy.copy(exc)
    except Exception:
        return exc


class GoAwayError(H2Error):
    """The peer sent GOAWAY. Streams with id > `last_stream_id` were not
    processed and are safe to retry on a new connection."""

    last_stream_id: int
    error_code: H2Reason | int
    debug_data: bytes

    def __init__(self, last_stream_id: int, error_code: int, debug_data: bytes = b"") -> None:
        self.last_stream_id = last_stream_id
        self.error_code = _reason(error_code)
        self.debug_data = debug_data
        super().__init__(f"GOAWAY(last_stream_id={last_stream_id}, error_code={self.error_code!r})")

    def __reduce__(self):
        # `args` holds the formatted message, not the constructor params — rebuild
        # from the real params so `copy.copy`/pickle (and `fresh_exc`) work.
        return (type(self), (self.last_stream_id, int(self.error_code), self.debug_data), self.__dict__)


class StreamResetError(H2Error):
    """The peer sent RST_STREAM for this stream."""

    stream_id: int
    error_code: H2Reason | int

    def __init__(self, stream_id: int, error_code: int) -> None:
        self.stream_id = stream_id
        self.error_code = _reason(error_code)
        super().__init__(f"RST_STREAM(stream_id={stream_id}, error_code={self.error_code!r})")

    def __reduce__(self):
        # See GoAwayError.__reduce__.
        return (type(self), (self.stream_id, int(self.error_code)), self.__dict__)
