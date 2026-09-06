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

use super::errors::{H2ProtocolError, map_proto_err, map_reason, map_user_err};

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

// ===== h2 per-stream / per-connection numeric state kept in async files upstream =====
//
// `proto/streams/stream.rs` (`ContentLength`) and `proto/streams/counts.rs` (the
// DATA-frame budget) are not vendored — they are the async stream store and its
// bookkeeping, rewritten in Python. The two pure pieces of numeric state they own
// are mirrored here verbatim (h2 0.4.19 line references), so the driver does no
// per-frame arithmetic itself.

/// h2 `proto/mod.rs` L38-40 (0.4.19): the DATA-framing budget constants.
pub const DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD: usize = 256;
pub const DEFAULT_DATA_FRAME_BUDGET: usize = DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD * 100;
pub const MAX_RECV_EMPTY_DATA_FRAMES: usize = 100;

/// h2 stream.rs `ContentLength` (L121-125): the declared body length of a message,
/// decremented per DATA frame and checked at END_STREAM.
#[derive(Clone, Copy)]
enum ContentLength {
    Omitted,
    Head,
    Remaining(u64),
}

/// `ContentLength` for one stream. `frozen` + `Mutex`: shared across worker threads.
#[pyclass(module = "httpunk._httpunk", name = "H2ContentLength", frozen)]
pub struct H2ContentLength {
    inner: Mutex<ContentLength>,
}

#[pymethods]
impl H2ContentLength {
    /// `is_head`: the response to a HEAD request — never has a body, whatever the
    /// header says (`ContentLength::Head`); else `Omitted` until a header is seen.
    #[new]
    #[pyo3(signature = (is_head=false))]
    fn new(is_head: bool) -> Self {
        Self {
            inner: Mutex::new(if is_head {
                ContentLength::Head
            } else {
                ContentLength::Omitted
            }),
        }
    }

    /// h2 `ContentLength::is_head` (stream.rs L597).
    fn is_head(&self) -> bool {
        matches!(*self.inner.lock().unwrap(), ContentLength::Head)
    }

    /// Record a parsed `content-length` (h2 recv.rs `recv_headers` L175-201): a no-op
    /// for a HEAD response, whose block h2 skips entirely.
    fn set(&self, value: u64) {
        let mut cl = self.inner.lock().unwrap();
        if !matches!(*cl, ContentLength::Head) {
            *cl = ContentLength::Remaining(value);
        }
    }

    /// h2 stream.rs `dec_content_length` (L338-353): consume `len` body bytes. `False`
    /// = more data than declared, or any data on a HEAD response.
    fn dec(&self, len: u64) -> bool {
        let mut cl = self.inner.lock().unwrap();
        match *cl {
            ContentLength::Remaining(rem) => match rem.checked_sub(len) {
                Some(val) => {
                    *cl = ContentLength::Remaining(val);
                    true
                }
                None => false,
            },
            ContentLength::Head => len == 0,
            ContentLength::Omitted => true,
        }
    }

    /// h2 stream.rs `ensure_content_length_zero` (L355-361): `False` = a declared
    /// length still unsatisfied at END_STREAM.
    fn is_satisfied(&self) -> bool {
        !matches!(*self.inner.lock().unwrap(), ContentLength::Remaining(n) if n != 0)
    }
}

/// h2 counts.rs `Budget` (0.4.19): `consume` fails once exhausted, `replenish`
/// saturates at the resolved maximum.
struct Budget {
    current: usize,
    max: usize,
}

impl Budget {
    fn consume(&mut self, n: usize) -> bool {
        if n > self.current {
            return false;
        }
        self.current -= n;
        true
    }

    fn replenish(&mut self, n: usize) {
        self.current = (self.current + n).min(self.max);
    }
}

struct DataFrameCounts {
    data_frame_budget: Budget,
    num_recv_empty_data_frames: usize,
}

/// The connection's DATA-framing budget — h2 0.4.19 counts.rs `record_data_frame` /
/// `release_data_frame` (L99-125) over `DataFrameBudget::resolve` (proto/connection.rs
/// L96-106). Flow control bounds payload BYTES, not frame COUNT: a peer fragmenting
/// data into tiny frames bloats the buffered chunk queue while staying inside every
/// window; non-final frames below the threshold consume the overhead they impose,
/// larger ones earn it back, empty ones count against their own lifetime cap.
#[pyclass(module = "httpunk._httpunk", name = "H2DataFrameBudget", frozen)]
pub struct H2DataFrameBudget {
    inner: Mutex<DataFrameCounts>,
}

fn budget_exhausted() -> PyErr {
    // streams.rs L644-647: `BudgetExhausted` -> connection ENHANCE_YOUR_CALM.
    H2ProtocolError::new_err((
        Some(u32::from(Reason::ENHANCE_YOUR_CALM)),
        "too_many_data_frames".to_string(),
    ))
}

#[pymethods]
impl H2DataFrameBudget {
    /// `DataFrameBudget::resolve`: a configured budget as-is; otherwise ("Auto") half
    /// the connection recv window (`connection_window`, default
    /// DEFAULT_INITIAL_WINDOW_SIZE), floored at DEFAULT_DATA_FRAME_BUDGET.
    #[new]
    #[pyo3(signature = (configured=None, connection_window=None))]
    fn new(configured: Option<usize>, connection_window: Option<u32>) -> Self {
        let max = configured.unwrap_or_else(|| {
            let window = connection_window.unwrap_or(vendor_h2::frame::DEFAULT_INITIAL_WINDOW_SIZE);
            (window as usize / 2).max(DEFAULT_DATA_FRAME_BUDGET)
        });
        Self {
            inner: Mutex::new(DataFrameCounts {
                data_frame_budget: Budget { current: max, max },
                num_recv_empty_data_frames: 0,
            }),
        }
    }

    /// `record_data_frame` for a received NON-FINAL DATA frame (the caller guards:
    /// a final frame is never budgeted, streams.rs L640-649). Returns the charge taken
    /// from the budget (0 for an empty or a large frame) — what `release` gives back
    /// once the chunk leaves the buffer. Raises `H2ProtocolError(ENHANCE_YOUR_CALM)`
    /// on exhaustion of the byte budget or the empty-frame cap.
    fn record(&self, payload_len: usize) -> PyResult<usize> {
        let mut c = self.inner.lock().unwrap();
        if payload_len == 0 {
            c.num_recv_empty_data_frames = c
                .num_recv_empty_data_frames
                .checked_add(1)
                .ok_or_else(budget_exhausted)?;
            if c.num_recv_empty_data_frames > MAX_RECV_EMPTY_DATA_FRAMES {
                return Err(budget_exhausted());
            }
            Ok(0)
        } else if payload_len < DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD {
            let cost = DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD - payload_len;
            if !c.data_frame_budget.consume(cost) {
                return Err(budget_exhausted());
            }
            Ok(cost)
        } else {
            c.data_frame_budget
                .replenish(payload_len - DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD);
            Ok(0)
        }
    }

    /// `release_data_frame`'s replenish, for a charge previously taken by `record`
    /// (saturating at the resolved budget).
    fn release(&self, charge: usize) {
        self.inner
            .lock()
            .unwrap()
            .data_frame_budget
            .replenish(charge);
    }

    /// The budget currently available (tests / diagnostics).
    #[getter]
    fn available(&self) -> usize {
        self.inner.lock().unwrap().data_frame_budget.current
    }

    /// Empty non-final DATA frames received over the connection's lifetime (counts.rs
    /// `num_recv_empty_data_frames`, capped at MAX_RECV_EMPTY_DATA_FRAMES).
    #[getter]
    fn empty_frames(&self) -> usize {
        self.inner.lock().unwrap().num_recv_empty_data_frames
    }
}
