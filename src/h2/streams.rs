//! PyO3 adapters exposing the vendored, synchronous h2 stream-state and
//! flow-control logic to Python. Thin wrappers — all behaviour lives in the
//! vendored `vendor_h2::proto::streams::{state, flow_control}` (byte-for-byte
//! h2, aside from the documented `recv_open` shim).
//!
//! Both classes are `frozen` with a `std::sync::Mutex` guarding the vendored
//! value, so they are `Sync` and safe to share across worker threads.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::sync::Mutex;

use vendor_h2::frame::{Reason, Reset, StreamId};
use vendor_h2::proto::Initiator;
use vendor_h2::proto::streams::{FlowControl, State};

use super::errors::{map_proto_err, map_reason, map_user_err};

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

    /// Close the send half IF it is still streaming; returns whether it did. The
    /// check and the transition run under ONE acquisition of the state mutex: the
    /// read pump's `recv_reset` may move the state on another thread, and a Python
    /// `is_send_streaming()` check followed by a separate `send_close()` call could
    /// land on a Closed state — where the vendored `send_close` panics, and with
    /// `panic = "abort"` that takes the process down. A `false` return is the
    /// driver's cue to surface the reset instead.
    fn send_close(&self) -> bool {
        let mut state = self.inner.lock().unwrap();
        if state.is_send_streaming() {
            state.send_close();
            true
        } else {
            false
        }
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
