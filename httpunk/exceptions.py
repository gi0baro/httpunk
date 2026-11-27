"""httpunk's error taxonomy — one class per public error kind of the upstream
crates, so a caller can match on what hyper / h2 would have reported.

Everything derives from `HTTPunkError`, the neutral root. What a connection
reports — its transport failing, a body that could not be read or written, a
request it did not send, its own closure — is an `HTTPConnectionError`, on either
protocol; local misuse the stack detects is a `UserError`. `H1Error` / `H2Error`
hold only what the other protocol has no counterpart for. The classes are
defined in Rust (`src/errors.rs`, `src/h1/errors.rs`, `src/h2/errors.rs`), except
`GoAwayError` and `StreamResetError`:

    HTTPunkError
    ├── HTTPConnectionError           the connection reported a failure; `errno`
    │   ├── TransportError            the transport failed or ended with work in flight (hyper `Io`, h2 `Io`)
    │   ├── BodyError                 a body could not be read (hyper `Body`)
    │   │   ├── H1BodyError           `io_kind` = the io cause
    │   │   └── H2BodyError           its cause is what stopped the stream
    │   ├── BodyWriteError            a head or body could not be written (hyper `BodyWrite`)
    │   ├── RequestNotSentError       the request never reached the wire: safe to retry (hyper `Canceled`)
    │   ├── ConnectionClosedError     the connection is closed (locally, or asked for after its end)
    │   ├── H1IncompleteMessageError  EOF while a message was still expected (also an H1Error)
    │   ├── H2KeepAliveTimeoutError   keep-alive PING unanswered (also an H2Error)
    │   └── GoAwayError               peer sent GOAWAY (also an H2Error)
    ├── UserError                     local misuse the stack detects
    │   ├── H1UserError               hyper `User`; `kind`
    │   └── H2UserError               h2 `UserError`; `kind`
    ├── H1Error                       HTTP/1 only (hyper `Error` kinds)
    │   ├── H1IncompleteMessageError
    │   ├── H1ParseError              malformed message head (hyper `Parse`; `kind` = the variant)
    │   └── H1UnexpectedMessageError  bytes on an idle client connection (hyper `UnexpectedMessage`)
    └── H2Error                       HTTP/2 only
        ├── GoAwayError
        ├── H2KeepAliveTimeoutError
        ├── StreamResetError          peer sent RST_STREAM for a stream
        ├── H2ProtocolError           connection-level protocol violation (-> GOAWAY)
        ├── H2StreamError             stream-level protocol violation (-> RST_STREAM)
        └── H2FlowControlError        flow-control window over/underflow

`HTTPConnectionError.errno` is the number of the OS error behind the failure:
the transport's `OSError` where it was raised, or the connection's stored error
when the failure is a consequence of it; `None` otherwise (a clean close, a
transport error that is not an `OSError`, a GOAWAY). An exception raised where
a transport operation failed has that operation's exception as `__cause__`; a
stored error raised again carries the stored copy as `__cause__`.

`error_code` attributes are `H2Reason` members for known codes (a Rust-defined enum
whose members compare equal to, hash like, and convert to their int code), or a
plain int for codes outside the RFC set. The
Rust-defined classes carry their fields positionally in `args`, e.g.
`H1ParseError.args == (kind, message)`, `H1BodyError.args == (io_kind, message)`,
`H2UserError.args == (kind, message)`.

What stays a plain Python exception: caller-argument validation (an invalid
method / URL / status / header name, an out-of-range option — the `http` crate's
constructor errors, which hyper never sees) is a `ValueError`; misuse of
httpunk's own API (responding twice, reading the next request before answering
the current one) is a `RuntimeError`. Neither is a hyper error kind. An
exception raised by the caller's own body iterable propagates as itself: hyper
wraps it as `User::Body` because Rust must; Python carries the instance.
Cancellation is not an error kind: it propagates as itself and closes the
connection it interrupted.
"""

from __future__ import annotations

import copy as _copy

from ._httpunk import (
    BodyError as BodyError,
    BodyWriteError as BodyWriteError,
    ConnectionClosedError as ConnectionClosedError,
    H1BodyError as H1BodyError,
    H1Error as H1Error,
    H1IncompleteMessageError as H1IncompleteMessageError,
    H1ParseError as H1ParseError,
    H1UnexpectedMessageError as H1UnexpectedMessageError,
    H1UserError as H1UserError,
    H2BodyError as H2BodyError,
    H2Error as H2Error,
    H2FlowControlError as H2FlowControlError,
    H2KeepAliveTimeoutError as H2KeepAliveTimeoutError,
    H2ProtocolError as H2ProtocolError,
    H2Reason as H2Reason,
    H2StreamError as H2StreamError,
    H2UserError as H2UserError,
    HTTPConnectionError as HTTPConnectionError,
    HTTPunkError as HTTPunkError,
    RequestNotSentError as RequestNotSentError,
    TransportError as TransportError,
    UserError as UserError,
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
    and restores `__dict__` extras (e.g. `errno`); `GoAwayError` /
    `StreamResetError` define `__reduce__` because their args don't roundtrip
    through their constructors. The copy carries no `__traceback__` / `__cause__` /
    `__context__`. An uncopyable exception is returned as-is — degraded (shared
    instance) but functional."""
    try:
        return _copy.copy(exc)
    except Exception:
        return exc


class GoAwayError(HTTPConnectionError, H2Error):
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
