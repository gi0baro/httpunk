//! HTTP/2 error taxonomy — one Python exception class per h2 error kind the
//! vendored state machine / codec can produce (the h2 twin of `h1::errors`), and
//! the mappings from the h2 crate's `UserError` / `proto::Error` / `Reason` to
//! them. `args` carry the fields positionally (documented per class).

use pyo3::create_exception;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use vendor_h2::codec::UserError;
use vendor_h2::frame::Reason;
use vendor_h2::proto::{Error, Initiator};

use crate::errors::{ConnectionClosedError, HTTPunkError};

create_exception!(
    _httpunk,
    H2Error,
    HTTPunkError,
    "Base class for every httpunk HTTP/2 protocol error."
);
create_exception!(
    _httpunk,
    H2ProtocolError,
    H2Error,
    "Connection-level protocol violation (-> GOAWAY). args = (reason: int|None, message)."
);
create_exception!(
    _httpunk,
    H2StreamError,
    H2Error,
    "Stream-level protocol violation (-> RST_STREAM, connection survives). \
     args = (stream_id: int, reason: int, initiator: str)."
);
create_exception!(
    _httpunk,
    H2UserError,
    H2Error,
    "Local API misuse (h2 UserError). args = (kind: str, message: str)."
);
create_exception!(
    _httpunk,
    H2FlowControlError,
    H2Error,
    "Flow-control window over/underflow. args = (reason: int,)."
);

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("H2Error", py.get_type::<H2Error>())?;
    m.add("H2ProtocolError", py.get_type::<H2ProtocolError>())?;
    m.add("H2StreamError", py.get_type::<H2StreamError>())?;
    m.add("H2UserError", py.get_type::<H2UserError>())?;
    m.add("H2FlowControlError", py.get_type::<H2FlowControlError>())?;
    Ok(())
}

/// Stable discriminant tag for each `h2::UserError` variant, so Python callers
/// can distinguish them programmatically (e.g. `OverflowedStreamId` =
/// retry-on-a-new-connection) instead of matching the display string.
fn user_err_kind(e: &UserError) -> &'static str {
    match e {
        UserError::InactiveStreamId => "inactive_stream_id",
        UserError::UnexpectedFrameType => "unexpected_frame_type",
        UserError::PayloadTooBig => "payload_too_big",
        UserError::Rejected => "rejected",
        UserError::ReleaseCapacityTooBig => "release_capacity_too_big",
        UserError::OverflowedStreamId => "overflowed_stream_id",
        UserError::MalformedHeaders => "malformed_headers",
        UserError::MissingUriSchemeAndAuthority => "missing_uri_scheme_and_authority",
        UserError::PollResetAfterSendResponse => "poll_reset_after_send_response",
        UserError::SendPingWhilePending => "send_ping_while_pending",
        UserError::SendSettingsWhilePending => "send_settings_while_pending",
        UserError::PeerDisabledServerPush => "peer_disabled_server_push",
        UserError::InvalidInformationalStatusCode => "invalid_informational_status_code",
    }
}

pub(crate) fn map_user_err(e: UserError) -> PyErr {
    H2UserError::new_err((user_err_kind(&e), e.to_string()))
}

/// A `PayloadTooBig` user error (h2 `UserError::PayloadTooBig`), for the codec's
/// DATA size guard.
pub(crate) fn user_payload_too_big(len: usize, max: usize) -> PyErr {
    H2UserError::new_err((
        "payload_too_big",
        format!("DATA payload {len} exceeds SETTINGS_MAX_FRAME_SIZE {max}"),
    ))
}

fn initiator_str(i: Initiator) -> &'static str {
    match i {
        Initiator::User => "user",
        Initiator::Library => "library",
        Initiator::Remote => "remote",
    }
}

// h2 proto/error.rs:48-50: retain opaque GOAWAY data separately from the
// diagnostic message. The internal attribute leaves the public args unchanged;
// fresh_exc copies it with the exception's __dict__.
fn with_debug_data(error: PyErr, data: &[u8]) -> PyErr {
    Python::attach(|py| {
        error
            .value(py)
            .setattr(intern!(py, "_debug_data"), PyBytes::new(py, data))
            .expect("H2ProtocolError supports instance attributes");
    });
    error
}

pub(crate) fn protocol_err_with_debug(reason: Reason, msg: &str, data: &[u8]) -> PyErr {
    with_debug_data(
        H2ProtocolError::new_err((Some(u32::from(reason)), msg.to_string())),
        data,
    )
}

/// Map an h2 `proto::Error` to a Python exception, **preserving the
/// stream-vs-connection distinction** (h2 `proto/error.rs`):
/// - `Reset(id, reason, initiator)` is a *stream-level* error -> `H2StreamError`;
///   the driver RSTs just that stream and the connection survives.
/// - `GoAway(_, reason, initiator)` is a *connection-level* error -> the driver
///   sends GOAWAY and tears down.
/// - `Io` is a transport error, NOT a protocol violation, so it surfaces as a
///   `ConnectionClosedError` (transport-closed) rather than a PROTOCOL_ERROR
///   GOAWAY. It is still unreachable at the state-machine call sites wired
///   today (they only return `library_go_away`/stored `Reset`), but a future
///   call site that can produce `Io` now maps correctly.
pub(crate) fn map_proto_err(e: &Error) -> PyErr {
    match e {
        Error::Reset(id, r, initiator) => {
            H2StreamError::new_err((u32::from(*id), u32::from(*r), initiator_str(*initiator)))
        }
        Error::GoAway(data, r, _) => protocol_err_with_debug(*r, &e.to_string(), data.as_ref()),
        Error::Io(..) => ConnectionClosedError::new_err((e.to_string(),)),
    }
}

/// The RST_STREAM code for a stream whose body failed with `exc` — hyper error.rs:339-359
/// (`find_source` + `h2_reason`): the reason of the first HTTP/2 error on the cause
/// chain (Rust's `source()`, Python's `__cause__`), else INTERNAL_ERROR, also when that
/// error carries none (h2 `Error::reason` is `None` for a user error).
pub(crate) fn h2_reason(exc: &Bound<'_, PyAny>) -> u32 {
    let mut cause = Some(exc.clone());
    while let Some(e) = cause {
        if e.is_instance_of::<H2Error>() {
            return reason_of(&e).unwrap_or(u32::from(Reason::INTERNAL_ERROR));
        }
        cause = e
            .getattr(intern!(e.py(), "__cause__"))
            .ok()
            .filter(|c| !c.is_none());
    }
    u32::from(Reason::INTERNAL_ERROR)
}

/// Where an HTTP/2 error keeps its reason: positionally in `args` for the classes
/// defined here, as `error_code` for `GoAwayError` / `StreamResetError`
/// (httpunk/exceptions.py).
fn reason_of(e: &Bound<'_, PyAny>) -> Option<u32> {
    let py = e.py();
    let code = if e.is_instance_of::<H2StreamError>() {
        e.getattr(intern!(py, "args")).ok()?.get_item(1).ok()?
    } else if e.is_instance_of::<H2ProtocolError>() || e.is_instance_of::<H2FlowControlError>() {
        e.getattr(intern!(py, "args")).ok()?.get_item(0).ok()?
    } else {
        e.getattr(intern!(py, "error_code")).ok()?
    };
    code.extract().ok()
}

pub(crate) fn map_reason(r: Reason) -> PyErr {
    H2FlowControlError::new_err((u32::from(r),))
}
