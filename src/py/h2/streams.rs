//! PyO3 adapters exposing the vendored, synchronous h2 stream-state and
//! flow-control logic to Python. Thin wrappers — all behaviour lives in the
//! vendored `vendor_h2::proto::streams::{state, flow_control}` (byte-for-byte
//! h2, aside from the documented `recv_open` shim).
//!
//! Both classes are `frozen` with a `std::sync::Mutex` guarding the vendored
//! value, so they are `Sync` and safe to share across worker threads.

use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::sync::Mutex;

use vendor_h2::codec::UserError;
use vendor_h2::frame::{Reason, Reset, StreamId};
use vendor_h2::proto::streams::{FlowControl, State};
use vendor_h2::proto::{Error, Initiator};

use crate::py::errors::{ConnectionClosedError, HTTPunkError};

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

fn map_user_err(e: UserError) -> PyErr {
    H2UserError::new_err((user_err_kind(&e), e.to_string()))
}

/// A `PayloadTooBig` user error (h2 `UserError::PayloadTooBig`), for the codec's
/// DATA size guard. Crate-visible so `codec.rs` can share the taxonomy.
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

/// Map an h2 `proto::Error` to a Python exception, **preserving the
/// stream-vs-connection distinction** (h2 `proto/error.rs`):
/// - `Reset(id, reason, initiator)` is a *stream-level* error -> `H2StreamError`;
///   the driver RSTs just that stream and the connection survives.
/// - `GoAway(_, reason, initiator)` is a *connection-level* error -> the driver
///   sends GOAWAY and tears down.
/// - `Io` is a transport error, NOT a protocol violation, so it surfaces as a
///   `ConnectionClosedError` (transport-closed) rather than a PROTOCOL_ERROR
///   GOAWAY (G41). It is still unreachable at the state-machine call sites wired
///   today (they only return `library_go_away`/stored `Reset`), but a future
///   call site that can produce `Io` now maps correctly.
fn map_proto_err(e: &Error) -> PyErr {
    match e {
        Error::Reset(id, r, initiator) => {
            H2StreamError::new_err((u32::from(*id), u32::from(*r), initiator_str(*initiator)))
        }
        Error::GoAway(_, r, _) => H2ProtocolError::new_err((Some(u32::from(*r)), e.to_string())),
        Error::Io(..) => ConnectionClosedError::new_err((e.to_string(),)),
    }
}

fn map_reason(r: Reason) -> PyErr {
    H2FlowControlError::new_err((u32::from(r),))
}

fn parse_initiator(s: &str) -> PyResult<Initiator> {
    match s {
        "user" => Ok(Initiator::User),
        "library" => Ok(Initiator::Library),
        "remote" => Ok(Initiator::Remote),
        other => Err(PyValueError::new_err(format!(
            "initiator must be 'user' | 'library' | 'remote', got {other:?}"
        ))),
    }
}

#[pyclass(module = "httpunk._httpunk", name = "H2StreamState", frozen)]
pub struct H2StreamState {
    inner: Mutex<State>,
}

#[pymethods]
impl H2StreamState {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(State::default()),
        }
    }

    // ----- transitions -----
    fn send_open(&self, eos: bool) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .send_open(eos)
            .map_err(map_user_err)
    }

    fn recv_open(&self, eos: bool, informational: bool) -> PyResult<bool> {
        self.inner
            .lock()
            .unwrap()
            .recv_open(eos, informational)
            .map_err(|e| map_proto_err(&e))
    }

    fn reserve_remote(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .reserve_remote()
            .map_err(|e| map_proto_err(&e))
    }

    fn reserve_local(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .reserve_local()
            .map_err(map_user_err)
    }

    fn recv_close(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .recv_close()
            .map_err(|e| map_proto_err(&e))
    }

    #[pyo3(signature = (stream_id, reason, queued))]
    fn recv_reset(&self, stream_id: u32, reason: u32, queued: bool) {
        let rst = Reset::new(StreamId::from(stream_id), Reason::from(reason));
        self.inner.lock().unwrap().recv_reset(rst, queued);
    }

    fn recv_eof(&self) {
        self.inner.lock().unwrap().recv_eof();
    }

    fn send_close(&self) {
        // h2 panics on an invalid state; the driver only calls this when the
        // state permits, mirroring h2's own invariant.
        self.inner.lock().unwrap().send_close();
    }

    #[pyo3(signature = (stream_id, reason, initiator))]
    fn set_reset(&self, stream_id: u32, reason: u32, initiator: &str) -> PyResult<()> {
        self.inner.lock().unwrap().set_reset(
            StreamId::from(stream_id),
            Reason::from(reason),
            parse_initiator(initiator)?,
        );
        Ok(())
    }

    fn set_scheduled_reset(&self, reason: u32) {
        self.inner
            .lock()
            .unwrap()
            .set_scheduled_reset(Reason::from(reason));
    }

    // ----- queries -----
    fn get_scheduled_reset(&self) -> Option<u32> {
        self.inner
            .lock()
            .unwrap()
            .get_scheduled_reset()
            .map(u32::from)
    }

    fn ensure_recv_open(&self) -> PyResult<bool> {
        self.inner
            .lock()
            .unwrap()
            .ensure_recv_open()
            .map_err(|e| map_proto_err(&e))
    }

    fn is_scheduled_reset(&self) -> bool {
        self.inner.lock().unwrap().is_scheduled_reset()
    }
    fn is_local_error(&self) -> bool {
        self.inner.lock().unwrap().is_local_error()
    }
    fn is_remote_reset(&self) -> bool {
        self.inner.lock().unwrap().is_remote_reset()
    }
    fn is_reset(&self) -> bool {
        self.inner.lock().unwrap().is_reset()
    }
    fn is_send_streaming(&self) -> bool {
        self.inner.lock().unwrap().is_send_streaming()
    }
    fn is_recv_headers(&self) -> bool {
        self.inner.lock().unwrap().is_recv_headers()
    }
    fn is_recv_streaming(&self) -> bool {
        self.inner.lock().unwrap().is_recv_streaming()
    }
    fn is_recv_end_stream(&self) -> bool {
        self.inner.lock().unwrap().is_recv_end_stream()
    }
    fn is_closed(&self) -> bool {
        self.inner.lock().unwrap().is_closed()
    }
    fn is_send_closed(&self) -> bool {
        self.inner.lock().unwrap().is_send_closed()
    }
    fn is_idle(&self) -> bool {
        self.inner.lock().unwrap().is_idle()
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner.lock().unwrap())
    }
}

#[pyclass(module = "httpunk._httpunk", name = "H2FlowControl", frozen)]
pub struct H2FlowControl {
    inner: Mutex<FlowControl>,
}

#[pymethods]
impl H2FlowControl {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(FlowControl::new()),
        }
    }

    fn window_size(&self) -> u32 {
        self.inner.lock().unwrap().window_size()
    }

    fn available(&self) -> i64 {
        isize::from(self.inner.lock().unwrap().available()) as i64
    }

    fn has_unavailable(&self) -> bool {
        self.inner.lock().unwrap().has_unavailable()
    }

    fn unclaimed_capacity(&self) -> Option<u32> {
        self.inner.lock().unwrap().unclaimed_capacity()
    }

    fn claim_capacity(&self, capacity: u32) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .claim_capacity(capacity)
            .map_err(map_reason)
    }

    fn assign_capacity(&self, capacity: u32) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .assign_capacity(capacity)
            .map_err(map_reason)
    }

    fn inc_window(&self, sz: u32) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .inc_window(sz)
            .map_err(map_reason)
    }

    fn dec_send_window(&self, sz: u32) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .dec_send_window(sz)
            .map_err(map_reason)
    }

    fn dec_recv_window(&self, sz: u32) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .dec_recv_window(sz)
            .map_err(map_reason)
    }

    fn send_data(&self, sz: u32) -> PyResult<()> {
        let mut fc = self.inner.lock().unwrap();
        // Guard h2's debug assert so a driver bug raises instead of aborting
        // (release builds use panic=abort).
        if sz > 0 && u64::from(fc.window_size()) < u64::from(sz) {
            return Err(map_reason(Reason::FLOW_CONTROL_ERROR));
        }
        fc.send_data(sz).map_err(map_reason)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner.lock().unwrap())
    }
}
