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
"""

from __future__ import annotations

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


class StreamResetError(H2Error):
    """The peer sent RST_STREAM for this stream."""

    stream_id: int
    error_code: H2Reason | int

    def __init__(self, stream_id: int, error_code: int) -> None:
        self.stream_id = stream_id
        self.error_code = _reason(error_code)
        super().__init__(f"RST_STREAM(stream_id={stream_id}, error_code={self.error_code!r})")
