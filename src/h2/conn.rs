//! `H2Streams` — the connection-and-stream state of one HTTP/2 connection under ONE
//! mutex, the mirror of h2's `proto::streams::Streams` (`Arc<Mutex<Inner>>`,
//! streams.rs) merged with the *state* half of `proto::Connection` (the error slot,
//! GOAWAY and SETTINGS bookkeeping, the pending-frame queue). Every method is one
//! critical section that performs a whole check-and-act and returns a **verdict**;
//! the Python driver (`httpunk/h2/connection.py`, a subclass) owns only the async
//! machinery — transports, pumps, events, queues — and acts on the verdict *after*
//! the call returns (events are set after the state that announces them is
//! published; nothing Python-side is ever read by two tasks without an event in
//! between). See HTTPUNK_RUST_STATE_DESIGN.md.
//!
//! Rules this file keeps (design §2): the mutex is taken exactly once per method
//! and never held across a call back into Python; no `Py<T>` is dropped while the
//! guard is held (handles are returned to the caller); the HPACK encoder and the
//! pending buffer live inside the state so encode order == wire order by
//! construction; verdicts are owned values.
//!
//! Cross-reference: `h2 ...` comments cite hyperium/h2 0.4.19 (see
//! crates/vendor-h2), paths relative to its `src/`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{PyTraverseError, PyVisit};

use vendor_h2::frame::{self, Reason, StreamId};
use vendor_h2::proto::streams::{FlowControl, State};
use vendor_h2::proto::{Error as ProtoError, Initiator, PollReset};

use super::codec::{
    self, Data, Decoder, Encoder, GoAway, Headers, Ping, RstStream, Settings as SettingsFrame,
    SettingsValues, WindowUpdate,
};
use super::errors::{H2ProtocolError, H2StreamError, map_proto_err, map_reason, map_user_err};
use super::settings::{PeerSettings, Settings};
use super::streams::{ContentLength, DataFrameCounts, parse_initiator};
use crate::errors::ConnectionClosedError;
use crate::http::HeaderMap;

// ===== constants (h2 proto/mod.rs, proto/streams/recv.rs, hyper profiles) =====

/// h2 `DEFAULT_RESET_STREAM_MAX`: locally-reset ids kept for late frames.
const RESET_STREAM_MAX: usize = 50;
/// h2 `DEFAULT_RESET_STREAM_SECS`: how long they are kept.
const RESET_STREAM_SECS: Duration = Duration::from_secs(1);
/// h2 `StreamId::MAX` (u32::MAX >> 1): the phase-1 graceful GOAWAY last-id.
const MAX_STREAM_ID: u32 = u32::MAX >> 1;
/// Opaque payload of the graceful-shutdown PING (h2 `Ping::SHUTDOWN`).
const SHUTDOWN_PING: [u8; 8] = *b"SHUTDOWN";
const DEFAULT_INITIAL_WINDOW_SIZE: u32 = frame::DEFAULT_INITIAL_WINDOW_SIZE;

// ===== verdict flags: what the Python caller must do now, after the call =====

/// Bytes were appended to the pending-send buffer: wake the write pump.
pub const FLAG_WAKE: u8 = 1;
/// Client: a MAX_CONCURRENT_STREAMS slot was freed (or the limit changed): wake
/// the slot waiters so they re-check.
pub const FLAG_SLOT_FREED: u8 = 2;
/// The connection is done (failed, or the GOAWAY exchange completed): wake the
/// role waiters — the client's ready/slot events, the server's accept loop.
pub const FLAG_CONN_DONE: u8 = 4;
/// Server: the graceful drain reached phase 2 and the last stream is gone: end
/// the accept loop.
pub const FLAG_STOP_ACCEPTING: u8 = 8;

/// `H2RecvHeadersVerdict.kind` values.
pub const HEADERS_IGNORED: u8 = 0;
pub const HEADERS_OPENED: u8 = 1;
pub const HEADERS_HEAD: u8 = 2;
pub const HEADERS_TRAILERS: u8 = 3;

// ===== verdict objects =====

/// Why a stream stopped, for the send side and the body readers (h2 state.rs
/// `ensure_reason` + the stream's stored error). `reason` is the RST_STREAM /
/// GOAWAY reason when there is one; `conn` means a connection-level error applies
/// (the driver raises the connection's error, or a `GoAwayError` when `reason` is
/// set). Neither: a local cancel — the driver falls back to the connection error
/// or `StreamResetError(CANCEL)`.
#[pyclass(module = "httpunk._httpunk", name = "H2Stopped", frozen)]
pub struct Stopped {
    #[pyo3(get)]
    pub reason: Option<u32>,
    #[pyo3(get)]
    pub conn: bool,
}

#[pymethods]
impl Stopped {
    fn __repr__(&self) -> String {
        format!("H2Stopped(reason={:?}, conn={})", self.reason, self.conn)
    }
}

#[pyclass(module = "httpunk._httpunk", name = "H2RecvHeadersVerdict", frozen)]
pub struct RecvHeadersVerdict {
    #[pyo3(get)]
    pub kind: u8,
    #[pyo3(get)]
    pub handle: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub eof: bool,
    #[pyo3(get)]
    pub flags: u8,
}

#[pyclass(module = "httpunk._httpunk", name = "H2RecvDataVerdict", frozen)]
pub struct RecvDataVerdict {
    #[pyo3(get)]
    pub handle: Option<Py<PyAny>>,
    /// The payload to deliver (`None`: nothing to deliver — swallowed, or an empty
    /// budgeted frame).
    #[pyo3(get)]
    pub payload: Option<Py<PyBytes>>,
    #[pyo3(get)]
    pub budgeted: bool,
    #[pyo3(get)]
    pub eof: bool,
    #[pyo3(get)]
    pub flags: u8,
}

#[pyclass(module = "httpunk._httpunk", name = "H2SendVerdict", frozen)]
pub struct SendVerdict {
    /// Bytes of `data[offset:]` framed and queued (0 with `done=False`: wait for window).
    #[pyo3(get)]
    pub sent: usize,
    /// The last byte (and END_STREAM, if requested) is queued — and with END_STREAM the
    /// send half is closed in this same step (h2 `send_data` / `send_trailers`).
    #[pyo3(get)]
    pub done: bool,
    #[pyo3(get)]
    pub stopped: Option<Py<Stopped>>,
    #[pyo3(get)]
    pub flags: u8,
    /// Server, the response complete: the request's recv half went with it (hyper drops
    /// the `RecvStream`) — the handle to notify when an unread body was RST_STREAMed,
    /// with the stop its readers must see (`None`: nothing to notify).
    #[pyo3(get)]
    pub handle: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub reader_stop: Option<Py<Stopped>>,
}

#[pyclass(module = "httpunk._httpunk", name = "H2ResetVerdict", frozen)]
pub struct ResetVerdict {
    /// The stream's handle to notify (`None`: no live stream, nothing to do).
    #[pyo3(get)]
    pub handle: Option<Py<PyAny>>,
    /// The error the body readers must see (`None`: a clean EOF — a local cancel).
    #[pyo3(get)]
    pub stop: Option<Py<Stopped>>,
    #[pyo3(get)]
    pub flags: u8,
}

// ===== state =====

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Client,
    Server,
}

/// h2 stream.rs `Stream`: one entry of the store. Stays stored after `Closed`
/// while it still holds recv-window bytes the reader has not released
/// (`finished`), exactly as h2 keeps a closed stream in the store while a
/// `RecvStream` handle references it (`release_capacity` on a closed stream must
/// still credit the connection window).
struct StreamEntry {
    /// The Python `Stream`: id + events + body queue, nothing else.
    handle: Py<PyAny>,
    state: State,
    send_flow: FlowControl,
    recv_flow: FlowControl,
    content_length: ContentLength,
    recv_unreleased: u32,
    recv_reclaimed: bool,
    send_buffered: usize,
    data_budget_charged: usize,
    holds_slot: bool,
    /// Left the active set (slot freed, counted out); kept only for the recv ledger.
    finished: bool,
}

impl StreamEntry {
    fn new(handle: Py<PyAny>, send_window: u32, recv_window: u32, is_head: bool) -> Self {
        // Send side: `available` is kept equal to the window (see `send_budget`), so
        // it is assigned here and on every later increment, never left at zero.
        let mut send_flow = FlowControl::new();
        let _ = send_flow.inc_window(send_window);
        let _ = send_flow.assign_capacity(send_window);
        let mut recv_flow = FlowControl::new();
        let _ = recv_flow.inc_window(recv_window);
        let _ = recv_flow.assign_capacity(recv_window);
        StreamEntry {
            handle,
            state: State::default(),
            send_flow,
            recv_flow,
            content_length: ContentLength::new(is_head),
            recv_unreleased: 0,
            recv_reclaimed: false,
            send_buffered: 0,
            data_budget_charged: 0,
            holds_slot: false,
            finished: false,
        }
    }

    /// Why the stream stopped, read off the vendored state — the ONE source of truth
    /// (no shadow fields). While the recv half is open the closure's cause is what
    /// h2's `poll_data` surfaces (`ensure_recv_open`); once the message stood
    /// (`Closed(ErrorAfterEndStream)`), h2 `ensure_reason` (state.rs L461-480, the
    /// `poll_reset` verdict) still reports the RST_STREAM / GOAWAY reason to the
    /// send side, and a non-reason closure (a broken pipe) is a connection error.
    fn stopped(&self) -> Stopped {
        match self.state.ensure_recv_open() {
            Err(e) => stop_from_proto(&e),
            Ok(_) => match self.state.ensure_reason(PollReset::Streaming) {
                Ok(Some(reason)) => Stopped {
                    reason: Some(u32::from(reason)),
                    conn: false,
                },
                Ok(None) => Stopped {
                    reason: None,
                    conn: false,
                },
                Err(e) => Stopped {
                    reason: e.reason().map(u32::from),
                    conn: true,
                },
            },
        }
    }

    /// The reader-facing error (`None`: a clean EOF) — h2 recv.rs `poll_data` ->
    /// state.rs `ensure_recv_open`: an error only while the message had not stood.
    fn stop_for_reader(&self) -> Option<Stopped> {
        self.state
            .ensure_recv_open()
            .err()
            .map(|e| stop_from_proto(&e))
    }
}

/// A closure's cause (h2 proto/error.rs) as the driver's stop: a reset carries its
/// reason; a GOAWAY carries its reason AND is connection-level; an I/O closure is
/// connection-level with no reason.
fn stop_from_proto(e: &ProtoError) -> Stopped {
    match e {
        ProtoError::Reset(_, reason, _) => Stopped {
            reason: Some(u32::from(*reason)),
            conn: false,
        },
        ProtoError::GoAway(_, reason, _) => Stopped {
            reason: Some(u32::from(*reason)),
            conn: true,
        },
        ProtoError::Io(..) => Stopped {
            reason: None,
            conn: true,
        },
    }
}

enum ConnError {
    /// A traceback-free copy handed in by Python (`fresh_exc`).
    Py(Py<PyAny>),
    /// A closure decided here: built as a fresh `ConnectionClosedError` per query.
    Closed(String),
}

struct GoAwayInfo {
    last_stream_id: u32,
    reason: u32,
    debug_data: Bytes,
}

struct Inner {
    role: Role,
    /// The outbound half of the codec (HPACK encoder + the peer's frame-size limit):
    /// under THIS mutex, so encode order == wire order. The inbound half lives in
    /// `H2Streams::decoder`, its own mutex (design §3.1).
    encoder: Encoder,
    /// The pending-send buffer (h2's single frame queue drained by the connection
    /// task). Every frame goes through it; wire order == commit order.
    pending: BytesMut,
    /// `close()` asked the write pump to drain and exit.
    stop: bool,
    /// DATA payload bytes queued per stream since the last flush; credited back to
    /// `send_buffered` once the pump wrote them (`credit_written`). Consecutive frames
    /// of one stream merge into one entry, and `credit_written` wakes each stream ONCE
    /// per batch: a wake per frame is a spurious cross-thread wake per frame for a
    /// sender already running (it re-checks, finds nothing new, parks again).
    credit: Vec<(u32, usize)>,
    inflight_credit: Vec<(u32, usize)>,
    /// Bare task handles (`spawn_without_results`), taken exactly once by `close()`.
    read_handle: Option<Py<PyAny>>,
    pump_handle: Option<Py<PyAny>>,

    streams: HashMap<u32, StreamEntry>,
    /// Streams opened and not yet closed (h2 counts.rs `num_recv_streams` /
    /// `num_send_streams`) — the MAX_CONCURRENT bound on the server, the
    /// "no streams" test of the GOAWAY reply.
    num_active: usize,
    /// Ids we locally reset, kept briefly so late frames are swallowed (h2
    /// `reset_stream_duration`); bounded to RESET_STREAM_MAX, aged out lazily.
    reset_streams: HashMap<u32, Instant>,

    /// The peer's values (settings.rs `PeerSettings`) and our SETTINGS handshake
    /// (settings.rs `Settings`, the mirror of h2 `proto/settings.rs`).
    peer: PeerSettings,
    settings: Settings,
    /// Per-stream recv window we advertise (h2 recv.rs `init_window_sz`).
    recv_init: u32,
    conn_send: FlowControl,
    conn_recv: FlowControl,
    conn_recv_target: u32,
    max_send_buf_size: usize,

    goaway: Option<GoAwayInfo>,
    goaway_last_id: Option<u32>,
    goaway_replied: bool,
    error: Option<ConnError>,
    local_error_resets: usize,
    max_local_error_resets: Option<usize>,
    data_budget: DataFrameCounts,

    // client
    next_id: u32,
    num_open_streams: usize,
    stream_limit: Option<usize>,

    // server
    last_recv_id: u32,
    last_processed_id: u32,
    max_concurrent: usize,
    max_pending_accept_reset: usize,
    graceful: bool,
    max_stream_id: u32,
    shutdown_final: bool,
    pending_accept: HashSet<u32>,
    remote_reset_pending: HashSet<u32>,
    auto_date: bool,
}

/// `FlowControl::send_data` with h2's debug assert guarded: a peer overrun (or a
/// driver bug) raises FLOW_CONTROL_ERROR instead of aborting the process (release
/// builds are `panic = "abort"`).
fn fc_send_data(fc: &mut FlowControl, sz: u32) -> Result<(), Reason> {
    if sz > 0 && fc.window_size() < sz {
        return Err(Reason::FLOW_CONTROL_ERROR);
    }
    fc.send_data(sz)
}

fn stream_err(stream_id: u32, reason: Reason) -> PyErr {
    H2StreamError::new_err((stream_id, u32::from(reason), "library"))
}

fn protocol_err(reason: Reason, msg: &str) -> PyErr {
    H2ProtocolError::new_err((Some(u32::from(reason)), msg.to_string()))
}

impl Inner {
    #[inline]
    fn wake_flag(&self, before: usize) -> u8 {
        if self.pending.len() > before {
            FLAG_WAKE
        } else {
            0
        }
    }

    fn goaway_last_stream_id(&self) -> u32 {
        match self.role {
            Role::Client => 0,
            Role::Server => self.last_processed_id,
        }
    }

    fn is_dead(&self) -> bool {
        self.error.is_some() || self.goaway.is_some()
    }

    // ----- lookup classification (h2 streams.rs recv_headers / recv_data top) -----

    /// h2 streams.rs `ensure_not_idle` (client: L1714; server: peer.rs `ensure_can_open`).
    fn ensure_not_idle(&self, sid: u32) -> PyResult<()> {
        let client_initiated = StreamId::from(sid).is_client_initiated();
        let idle = match self.role {
            Role::Client => !client_initiated || sid >= self.next_id,
            Role::Server => !client_initiated || sid > self.last_recv_id,
        };
        if idle {
            return Err(protocol_err(
                Reason::PROTOCOL_ERROR,
                &format!("frame on idle stream {sid}"),
            ));
        }
        Ok(())
    }

    /// A stream above the last-stream-id of a GOAWAY we've sent: late frames are
    /// silently ignored (h2 `id > max_stream_id` -> `ignore_data`). Only the server
    /// refuses peer-initiated streams by GOAWAY.
    fn above_goaway(&self, sid: u32) -> bool {
        self.role == Role::Server && sid > self.max_stream_id
    }

    /// The reset store: `Some(true)` = swallow (recently reset), `Some(false)` /
    /// `None` = not there (an expired entry is dropped).
    fn in_reset_store(&mut self, sid: u32) -> bool {
        if let Some(at) = self.reset_streams.get(&sid) {
            if at.elapsed() <= RESET_STREAM_SECS {
                return true;
            }
            self.reset_streams.remove(&sid);
        }
        false
    }

    /// Resolve the target stream for an inbound DATA / (client) HEADERS frame:
    /// `Ok(Some(id))` = live stream, `Ok(None)` = ignore, `Err` = stream error
    /// (STREAM_CLOSED -> RST just that stream) or connection error (idle stream).
    fn recv_lookup(&mut self, sid: u32) -> PyResult<Option<u32>> {
        if let Some(e) = self.streams.get(&sid) {
            if e.state.is_local_error() {
                return Ok(None); // locally reset: swallow late frames "for some time"
            }
            if e.state.is_closed() {
                // Completed normally but still stored (the recv ledger): give the peer
                // exactly the forgotten answer — release and classify STREAM_CLOSED.
                self.drop_closed_stored(sid);
                return Err(stream_err(sid, Reason::STREAM_CLOSED));
            }
            return Ok(Some(sid));
        }
        if self.in_reset_store(sid) {
            return Ok(None);
        }
        if self.above_goaway(sid) {
            return Ok(None);
        }
        self.ensure_not_idle(sid)?;
        Err(stream_err(sid, Reason::STREAM_CLOSED))
    }

    // ----- store maintenance (h2 counts.rs transition_after / drop_stream_ref) -----

    /// h2 counts.rs `transition_after` / `dec_num_streams`: a stream that reached
    /// Closed leaves the active set (slot freed, counted out) and is removed unless
    /// its reader still owes recv-window credit.
    fn close_stream(&mut self, sid: u32) -> u8 {
        let mut flags = 0;
        let Some(e) = self.streams.get_mut(&sid) else {
            return 0;
        };
        if !e.state.is_closed() || e.finished {
            return 0;
        }
        e.finished = true;
        self.num_active -= 1;
        if e.holds_slot {
            e.holds_slot = false;
            self.num_open_streams -= 1;
            flags |= FLAG_SLOT_FREED;
        }
        if e.recv_unreleased == 0 && e.data_budget_charged == 0 {
            self.streams.remove(&sid);
        }
        flags | self.on_stream_gone()
    }

    /// The send half just closed (the END_STREAM frame is queued and `send_close`
    /// ran, in the caller's step): the stream leaves the active set, and on the
    /// SERVER the request's recv half goes with it — hyper drops the request's
    /// `RecvStream` once the response is sent: an unread body is RST_STREAM(NO_ERROR)ed
    /// while the client is still sending (`maybe_cancel`, streams.rs L1601 — the
    /// nginx-compat rule) and the in-flight body's connection window is returned
    /// (`release_closed_capacity`). Returns the handle to notify when a reset was
    /// sent, the stop its readers must see, and the flags.
    fn finish_stream(&mut self, sid: u32) -> (Option<Py<PyAny>>, Option<Stopped>, u8) {
        let flags = self.close_stream(sid);
        if self.role == Role::Server {
            match self.streams.get(&sid) {
                Some(e) if !e.state.is_closed() => {
                    let (handle, stop, more) =
                        self.reset_stream(sid, u32::from(Reason::NO_ERROR), Initiator::User);
                    return (handle, stop, flags | more);
                }
                Some(_) => self.reclaim_stream_accounting(sid),
                None => {}
            }
        }
        (None, None, flags)
    }

    /// A stream left the active set: the server's phase-2 drain check, and the
    /// idle-after-peer-GOAWAY reply (h2 streams.rs `drop_stream_ref` L1647 wakes the
    /// connection task so `Connection::poll` re-evaluates `go_away_now(NO_ERROR)`).
    fn on_stream_gone(&mut self) -> u8 {
        let mut flags = 0;
        if self.role == Role::Server && self.shutdown_final && self.num_active == 0 {
            flags |= FLAG_STOP_ACCEPTING;
        }
        flags | self.maybe_goaway_reply()
    }

    /// If the peer has GOAWAY'd us with a REAL last-stream-id and no streams remain,
    /// queue our acknowledging GOAWAY(NO_ERROR, last-processed-id) exactly once and
    /// mark the connection DONE (F23; h2 proto/connection.rs `poll` L287-295).
    fn maybe_goaway_reply(&mut self) -> u8 {
        if self.goaway_replied || self.error.is_some() || self.num_active != 0 {
            return 0;
        }
        let Some(last) = self.goaway_last_id else {
            return 0;
        };
        if self.goaway.is_none() || last >= MAX_STREAM_ID {
            return 0;
        }
        self.goaway_replied = true;
        let last_id = self.goaway_last_stream_id();
        codec::encode_go_away(&mut self.pending, last_id, u32::from(Reason::NO_ERROR), &[]);
        self.error = Some(ConnError::Closed(
            "connection closed: GOAWAY exchanged".to_string(),
        ));
        FLAG_WAKE | FLAG_CONN_DONE
    }

    /// Release a normally-completed stream observed while still stored (the recv
    /// ledger window): reclaim its unread bytes and drop it.
    fn drop_closed_stored(&mut self, sid: u32) -> u8 {
        self.reclaim_stream_accounting(sid);
        self.close_stream(sid)
    }

    /// Remove a stream whose ledger is settled: called after a reclaim / a release
    /// on a `finished` entry.
    fn maybe_remove_settled(&mut self, sid: u32) {
        if let Some(e) = self.streams.get(&sid)
            && e.finished
            && e.recv_unreleased == 0
            && e.data_budget_charged == 0
        {
            self.streams.remove(&sid);
        }
    }

    // ----- recv-side flow control (h2 recv.rs) -----

    /// Return `n` bytes to the *connection* recv window and queue the WINDOW_UPDATE(0)
    /// once the reclaimed amount crosses the aggregation threshold
    /// (`FlowControl::unclaimed_capacity`).
    fn reclaim_conn(&mut self, n: u32) {
        if n == 0 {
            return;
        }
        let _ = self.conn_recv.assign_capacity(n);
        if let Some(unclaimed) = self.conn_recv.unclaimed_capacity() {
            let _ = self.conn_recv.inc_window(unclaimed);
            codec::encode_window_update(&mut self.pending, 0, unclaimed);
        }
    }

    /// h2 recv.rs `release_closed_capacity` (L493) + `clear_recv_buffer` (L959-976):
    /// return the connection window and the framing budget consumed by data the app
    /// will never read. Idempotent (`recv_reclaimed`, F22).
    fn reclaim_stream_accounting(&mut self, sid: u32) {
        let Some(e) = self.streams.get_mut(&sid) else {
            return;
        };
        let charge = std::mem::take(&mut e.data_budget_charged);
        if charge != 0 {
            self.data_budget.release(charge);
        }
        let n = std::mem::take(&mut e.recv_unreleased);
        e.recv_reclaimed = true;
        self.reclaim_conn(n);
        self.maybe_remove_settled(sid);
    }

    /// Consume `sz` from the connection recv window; a peer overrun is a connection
    /// FLOW_CONTROL_ERROR (h2 `consume_connection_window`).
    fn consume_conn_recv(&mut self, sz: u32) -> PyResult<()> {
        fc_send_data(&mut self.conn_recv, sz).map_err(map_reason)
    }

    // ----- DATA-framing budget (h2 counts.rs, 0.4.19) -----

    fn record_data_frame(&mut self, sid: u32, payload_len: usize) -> PyResult<()> {
        let charge = self.data_budget.record(payload_len)?;
        if let Some(e) = self.streams.get_mut(&sid) {
            e.data_budget_charged += charge;
        }
        Ok(())
    }

    // ----- reset store (h2 recv.rs enqueue_reset_expiration / clear_expired_reset_streams) -----

    fn clear_expired_reset_streams(&mut self) {
        self.reset_streams
            .retain(|_, at| at.elapsed() <= RESET_STREAM_SECS);
    }

    fn enqueue_reset_expiration(&mut self, sid: u32) {
        self.clear_expired_reset_streams();
        if self.reset_streams.len() >= RESET_STREAM_MAX {
            return; // over the cap: h2 drops it (transitions immediately) rather than retain
        }
        self.reset_streams.insert(sid, Instant::now());
    }

    // ----- SETTINGS application (h2 streams.rs apply_*_settings) -----

    fn apply_remote_settings(&mut self, f: &SettingsFrame) -> PyResult<(Vec<Py<PyAny>>, u8)> {
        let mut flags = 0;
        if let Some(v) = f.header_table_size {
            self.peer.header_table_size = v;
            self.encoder.set_send_header_table_size(v);
        }
        if let Some(v) = f.max_frame_size {
            self.encoder.set_send_max_frame_size(v)?;
            self.peer.max_frame_size = v;
        }
        if let Some(v) = f.max_concurrent_streams {
            self.peer.max_concurrent_streams = Some(v);
            if self.role == Role::Client {
                self.stream_limit = Some(v as usize);
                flags |= FLAG_SLOT_FREED; // wake waiters to re-check against the new limit
            }
        }
        let mut wake = Vec::new();
        if let Some(new) = f.initial_window_size
            && new != self.peer.initial_window_size
        {
            let old = self.peer.initial_window_size;
            self.peer.initial_window_size = new;
            // RFC 7540 §6.9.2: adjust every stream's send window by the delta (h2
            // send.rs `apply_remote_settings` L478-560). A send-closed stream is
            // skipped (its window will never be used; an increase could overflow).
            for e in self.streams.values_mut() {
                if e.state.is_send_closed() {
                    continue;
                }
                if new >= old {
                    e.send_flow.inc_window(new - old).map_err(map_reason)?;
                    e.send_flow.assign_capacity(new - old).map_err(map_reason)?;
                    wake.push(e.handle.clone_ref_unchecked());
                } else {
                    e.send_flow.dec_send_window(old - new).map_err(map_reason)?;
                    e.send_flow.claim_capacity(old - new).map_err(map_reason)?;
                }
            }
        }
        Ok((wake, flags))
    }

    /// Our own SETTINGS take effect for receiving (h2 proto/settings.rs ACK branch +
    /// recv.rs `apply_local_settings`): the decoder's limits and every stream's recv
    /// window, in one step — `dec` is the decoder locked by the caller (state → decoder).
    fn apply_local_settings(&mut self, dec: &mut Decoder) -> PyResult<()> {
        let local = *self.settings.local();
        if let Some(v) = local.header_table_size {
            dec.set_recv_header_table_size(v);
        }
        if let Some(v) = local.max_frame_size {
            dec.set_max_recv_frame_size(v)?;
        }
        if let Some(v) = local.max_header_list_size {
            dec.set_max_header_list_size(v);
        }
        if let Some(target) = local.initial_window_size {
            // h2 recv.rs `apply_local_settings` (L590-628): the *local* (recv) window
            // of every open stream moves by the delta.
            let old = self.recv_init;
            self.recv_init = target;
            if target != old {
                for e in self.streams.values_mut() {
                    if target > old {
                        e.recv_flow.inc_window(target - old).map_err(map_reason)?;
                        e.recv_flow
                            .assign_capacity(target - old)
                            .map_err(map_reason)?;
                    } else {
                        e.recv_flow
                            .dec_recv_window(old - target)
                            .map_err(map_reason)?;
                    }
                }
            }
        }
        Ok(())
    }

    // ----- send-side flow control (h2 send.rs / prioritize.rs) -----

    /// Bytes a stream may queue now: within the flow-control windows AND the
    /// per-stream send buffer cap (hyper `max_send_buf_size` -> h2 `poll_capacity`).
    ///
    /// Runtime-forced divergence from h2: h2 hands a stream send capacity from the
    /// connection's pool on demand (prioritize.rs `try_assign_capacity`, claimed back
    /// on a SETTINGS decrease), and its senders wait on that `available`. httpunk has
    /// no prioritize queue — a sender reserves against the windows right here — so
    /// each send window's `available` is simply kept EQUAL to its `window_size`:
    /// assigned wherever the window is opened or incremented, claimed where it is
    /// decremented. Without that, `FlowControl::send_data` (which decrements both)
    /// drives `available` negative by every byte sent and underflows after 2 GiB.
    fn send_budget(&self, e: &StreamEntry) -> usize {
        let window = self.conn_send.window_size().min(e.send_flow.window_size()) as usize;
        window.min(self.max_send_buf_size.saturating_sub(e.send_buffered))
    }

    // ----- resets (h2 streams.rs send_reset / recv_reset) -----

    /// h2 streams.rs `send_reset` / share.rs `SendStream::send_reset`: the ENTIRE
    /// reset commits here — state transition, window reclaim, slot free, reset-store
    /// entry, RST_STREAM + WINDOW_UPDATE queued. Returns the handle to notify and the
    /// reader-facing error (`None` = clean EOF).
    fn reset_stream(
        &mut self,
        sid: u32,
        reason: u32,
        initiator: Initiator,
    ) -> (Option<Py<PyAny>>, Option<Stopped>, u8) {
        let Some(e) = self.streams.get_mut(&sid) else {
            return (None, None, 0);
        };
        let handle = e.handle.clone_ref_unchecked();
        if e.state.is_closed() {
            // Already Closed (F18: e.g. a bad content-length on a final HEADERS). h2
            // send.rs `send_reset`: "transition the state to reset no matter what" —
            // unless already reset — so the waiters learn the reason; but a closed
            // stream with nothing queued gets no RST_STREAM frame. Wake every waiter,
            // reclaim and drop the stream; also the idempotent REPAIR path a retried
            // aclose lands on.
            if !e.state.is_reset() {
                e.state
                    .set_reset(StreamId::from(sid), Reason::from(reason), initiator);
            }
            let stop = e.stop_for_reader();
            let local = e.state.is_local_error();
            self.reclaim_stream_accounting(sid);
            let flags = self.close_stream(sid);
            if local {
                self.enqueue_reset_expiration(sid);
            }
            return (Some(handle), stop, flags);
        }
        e.state
            .set_reset(StreamId::from(sid), Reason::from(reason), initiator);
        let stop = e.stop_for_reader();
        self.reclaim_stream_accounting(sid);
        codec::encode_rst_stream(&mut self.pending, sid, reason);
        let flags = self.close_stream(sid);
        self.enqueue_reset_expiration(sid);
        (Some(handle), stop, flags)
    }

    /// Error a stream on a connection-level failure (h2 `recv_eof` /
    /// `handle_error`): Closed(BrokenPipe), waiters woken, removed. `reason`: the
    /// GOAWAY reason when one dropped it. `None` for a closed (already EOF'd) entry.
    fn abort_stream(
        &mut self,
        sid: u32,
        reason: Option<u32>,
    ) -> (Option<(Py<PyAny>, Stopped)>, u8) {
        let Some(e) = self.streams.get_mut(&sid) else {
            return (None, 0);
        };
        let was_closed = e.state.is_closed();
        match reason {
            // h2 streams.rs `recv_go_away` -> state.rs `handle_error(GoAway)`.
            Some(r) => e
                .state
                .handle_error(&ProtoError::remote_go_away(Bytes::new(), Reason::from(r))),
            // h2 `recv_eof`: Closed(BrokenPipe).
            None => e.state.recv_eof(),
        }
        let out = if was_closed {
            None
        } else {
            Some((e.handle.clone_ref_unchecked(), e.stopped()))
        };
        // Drop it regardless of the recv ledger: the connection is gone.
        e.recv_unreleased = 0;
        e.data_budget_charged = 0;
        e.recv_reclaimed = true;
        // The close may fire the idle-after-GOAWAY reply (the last stream dropped by
        // the peer's GOAWAY): its flags travel with the verdict.
        let flags = self.close_stream(sid);
        self.streams.remove(&sid);
        (out, flags)
    }

    /// h2 streams.rs `Streams::handle_error` (L362) / `recv_eof` (L386): fan the
    /// connection failure out to every stream.
    fn fail_all(&mut self) -> Vec<(Py<PyAny>, Stopped)> {
        let ids: Vec<u32> = self.streams.keys().copied().collect();
        let mut out = Vec::new();
        for sid in ids {
            // The error slot is set, so no GOAWAY reply can fire here: flags are moot.
            if let (Some(x), _) = self.abort_stream(sid, None) {
                out.push(x);
            }
        }
        out
    }

    fn apply_content_length(&mut self, sid: u32, f: &Headers) -> PyResult<()> {
        // h2 recv.rs `recv_headers` (L175-201): record content-length; reject a
        // non-numeric value or END_STREAM with a non-zero length (except 204/304). A
        // response to HEAD is fully exempt.
        let Some(e) = self.streams.get_mut(&sid) else {
            return Ok(());
        };
        if e.content_length.is_head() {
            return Ok(());
        }
        if f.content_length_invalid {
            return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
        }
        let Some(cl) = f.content_length else {
            return Ok(());
        };
        e.content_length.set(cl);
        if f.end_stream && cl > 0 && !matches!(f.status, Some(204 | 304)) {
            return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
        }
        Ok(())
    }
}

/// `Py<T>::clone_ref` without a `Python` token: an incref is a single atomic
/// operation on the free-threaded build (and GIL-protected otherwise), never a
/// call into Python — safe under the guard (rule 3 forbids *drops*, not increfs).
trait CloneRefUnchecked {
    fn clone_ref_unchecked(&self) -> Self;
}

impl CloneRefUnchecked for Py<PyAny> {
    fn clone_ref_unchecked(&self) -> Self {
        // SAFETY: every `H2Streams` method runs attached to the interpreter (it is a
        // `#[pymethods]` entry point), so a `Python` token exists for this thread.
        Python::attach(|py| self.clone_ref(py))
    }
}

// ===== the pyclass =====

/// The connection + stream state of one HTTP/2 connection (h2 `Streams::inner`
/// merged with `Connection`'s state), `frozen` + `subclass`: the Python driver
/// subclasses it and adds only async machinery.
#[pyclass(module = "httpunk._httpunk", name = "H2Streams", frozen, subclass)]
pub struct H2Streams {
    inner: Mutex<Inner>,
    /// The inbound half of the codec (HPACK decoder, frame read buffer, receive
    /// limits, the server-role preface): its own mutex, so a batch's HPACK decode
    /// never makes an app task wait for the stream windows. Lock order: `inner` →
    /// `decoder` (only `recv_settings`, applying our own SETTINGS on the peer's ACK,
    /// takes both); nothing takes them in reverse.
    decoder: Mutex<Decoder>,
}

impl H2Streams {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }
}

#[pymethods]
impl H2Streams {
    /// `role`: "client" | "server". The window / frame / header-list profile is the
    /// role's (hyper's, passed by the Python subclass); `max_concurrent_streams` is
    /// the server's advertised limit; `enable_push` is the client's SETTINGS_ENABLE_PUSH.
    #[new]
    #[pyo3(signature = (role, *, initial_window_size, connection_window, max_frame_size,
                        max_header_list_size, max_send_buf_size, max_concurrent_streams=None,
                        max_pending_accept_reset_streams=20, max_local_error_reset_streams=Some(1024),
                        data_frame_budget=None, auto_date_header=true, enable_push=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        role: &str,
        initial_window_size: u32,
        connection_window: u32,
        max_frame_size: u32,
        max_header_list_size: u32,
        max_send_buf_size: usize,
        max_concurrent_streams: Option<u32>,
        max_pending_accept_reset_streams: usize,
        max_local_error_reset_streams: Option<usize>,
        data_frame_budget: Option<usize>,
        auto_date_header: bool,
        enable_push: Option<bool>,
    ) -> PyResult<Self> {
        let role = match role {
            "client" => Role::Client,
            "server" => Role::Server,
            other => {
                return Err(PyValueError::new_err(format!(
                    "role must be 'client' or 'server', got {other:?}"
                )));
            }
        };
        // Fail at construction, not at the first SETTINGS write (h2 frame/settings.rs
        // asserts the RFC ranges).
        codec::check_max_frame_size(max_frame_size)?;
        if initial_window_size > codec::MAX_WINDOW_SIZE
            || connection_window > codec::MAX_WINDOW_SIZE
        {
            return Err(PyValueError::new_err(format!(
                "window sizes must be <= {} (RFC 9113 6.9.1)",
                codec::MAX_WINDOW_SIZE
            )));
        }
        // h2 prioritize.rs `Prioritize::new`: the connection send window is opened AND
        // its capacity assigned; `FlowControl::send_data` decrements both counters, so an
        // unassigned `available` would drift negative by every byte sent and underflow
        // (FLOW_CONTROL_ERROR) after 2 GiB on one connection.
        let mut conn_send = FlowControl::new();
        let _ = conn_send.inc_window(DEFAULT_INITIAL_WINDOW_SIZE);
        let _ = conn_send.assign_capacity(DEFAULT_INITIAL_WINDOW_SIZE);
        let mut conn_recv = FlowControl::new();
        let _ = conn_recv.inc_window(DEFAULT_INITIAL_WINDOW_SIZE);
        let _ = conn_recv.assign_capacity(DEFAULT_INITIAL_WINDOW_SIZE);
        // Grant a larger-than-default per-stream recv window IMMEDIATELY, before the
        // peer ACKs our SETTINGS: the peer only sends more than the 65535 default once
        // it has processed our SETTINGS, so accepting up to the advertised window can
        // never over-accept — whereas waiting for the ACK leaves a window where the
        // peer uses the new size before we grant it (a spurious FLOW_CONTROL_ERROR). A
        // *smaller* window still waits for the ACK (RFC 7540 §6.9.2).
        let recv_init = initial_window_size.max(DEFAULT_INITIAL_WINDOW_SIZE);
        let local = SettingsValues {
            header_table_size: None,
            enable_push,
            max_concurrent_streams: if role == Role::Server {
                max_concurrent_streams
            } else {
                None
            },
            initial_window_size: Some(initial_window_size),
            max_frame_size: Some(max_frame_size),
            max_header_list_size: Some(max_header_list_size),
        };
        Ok(H2Streams {
            decoder: Mutex::new(Decoder::new(role == Role::Server)),
            inner: Mutex::new(Inner {
                role,
                encoder: Encoder::new(),
                pending: BytesMut::new(),
                stop: false,
                credit: Vec::new(),
                inflight_credit: Vec::new(),
                read_handle: None,
                pump_handle: None,
                streams: HashMap::new(),
                num_active: 0,
                reset_streams: HashMap::new(),
                peer: PeerSettings::new(),
                settings: Settings::new(local),
                recv_init,
                conn_send,
                conn_recv,
                conn_recv_target: connection_window,
                max_send_buf_size,
                goaway: None,
                goaway_last_id: None,
                goaway_replied: false,
                error: None,
                local_error_resets: 0,
                max_local_error_resets: max_local_error_reset_streams,
                // h2 0.4.19 `DataFrameBudget::resolve` at handshake from the target
                // connection window.
                data_budget: DataFrameCounts::new(data_frame_budget, Some(connection_window)),
                next_id: 1,
                num_open_streams: 0,
                stream_limit: None,
                last_recv_id: 0,
                last_processed_id: 0,
                max_concurrent: max_concurrent_streams.map_or(usize::MAX, |v| v as usize),
                max_pending_accept_reset: max_pending_accept_reset_streams,
                graceful: false,
                max_stream_id: MAX_STREAM_ID,
                shutdown_final: false,
                pending_accept: HashSet::new(),
                remote_reset_pending: HashSet::new(),
                auto_date: auto_date_header,
            }),
        })
    }

    // ===== GC visibility (design §4): the Python objects held here =====

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        // GC runs only at safe points no Rust method contains, so the lock is free;
        // `try_lock` is belt-and-braces (a contended traverse visits nothing).
        if let Ok(me) = self.inner.try_lock() {
            for e in me.streams.values() {
                visit.call(&e.handle)?;
            }
            if let Some(ConnError::Py(err)) = &me.error {
                visit.call(err)?;
            }
            if let Some(h) = &me.read_handle {
                visit.call(h)?;
            }
            if let Some(h) = &me.pump_handle {
                visit.call(h)?;
            }
        }
        Ok(())
    }

    fn __clear__(&self) {
        // Take the fields under the lock, drop them outside it (rule 3).
        let taken = {
            let mut me = self.lock();
            let streams = std::mem::take(&mut me.streams);
            let err = me.error.take();
            let rh = me.read_handle.take();
            let ph = me.pump_handle.take();
            (streams, err, rh, ph)
        };
        drop(taken);
    }

    // ===== lifecycle =====

    #[getter]
    fn is_server(&self) -> bool {
        self.lock().role == Role::Server
    }

    /// The handshake start (h2 client.rs/server.rs `handshake`): queue the connection
    /// preface (client: the 24-byte preface; server: nothing) + our initial SETTINGS,
    /// then the initial WINDOW_UPDATE(0) advertising the larger-than-default
    /// connection recv window (h2 `initial_connection_window_size`). The caller
    /// flushes, then starts the pumps.
    fn begin(&self) -> PyResult<()> {
        let mut me = self.lock();
        if me.role == Role::Client {
            me.pending.extend_from_slice(vendor_hyper::H2_PREFACE);
        }
        let local = *me.settings.local();
        codec::encode_settings(&mut me.pending, &local)?;
        me.settings.mark_sent();
        let delta = me
            .conn_recv_target
            .saturating_sub(DEFAULT_INITIAL_WINDOW_SIZE);
        if delta > 0 {
            me.reclaim_conn(delta);
        }
        Ok(())
    }

    /// Store the two bare task handles (`spawn_without_results`); taken exactly once
    /// by `take_*` (`close()`), so a double `close()` cannot double-await them.
    fn store_task_handles(&self, read: Py<PyAny>, pump: Py<PyAny>) {
        let old = {
            let mut me = self.lock();
            (me.read_handle.replace(read), me.pump_handle.replace(pump))
        };
        drop(old);
    }

    fn take_read_handle(&self) -> Option<Py<PyAny>> {
        self.lock().read_handle.take()
    }

    fn take_pump_handle(&self) -> Option<Py<PyAny>> {
        self.lock().pump_handle.take()
    }

    // ===== inbound (h2 proto/connection.rs recv_frame -> streams.rs recv_*) =====

    /// Feed inbound bytes; return the complete frames now decodable (server role: the
    /// client's 24-byte preface is consumed first). Takes ONLY the decoder's mutex —
    /// the HPACK decode of a batch never contends with the stream state.
    fn receive(&self, py: Python<'_>, data: &[u8]) -> PyResult<Vec<Py<PyAny>>> {
        self.decoder.lock().unwrap().receive_frames(py, data)
    }

    /// Seed the decoder with bytes another reader already took off the transport
    /// (`util.auto`'s protocol sniff: the client preface) — hyper-util's `Rewind`
    /// without a wrapper transport, so every later read is the raw transport's. `None`
    /// = the preface matches so far but is incomplete, `False` = a mismatch.
    fn prime(&self, data: &[u8]) -> Option<bool> {
        self.decoder.lock().unwrap().prime(data)
    }

    /// h2 streams.rs `recv_headers` (L421) -> recv.rs `recv_headers` / `recv_trailers`
    /// / `open`. `spare`: a Python-built `Stream` handle the server role consumes
    /// when the frame opens a new request stream (`HEADERS_OPENED`); the client
    /// passes `None`.
    #[pyo3(signature = (frame, spare=None))]
    fn recv_headers(
        &self,
        frame: &Headers,
        spare: Option<Py<PyAny>>,
    ) -> PyResult<RecvHeadersVerdict> {
        let mut me = self.lock();
        let before = me.pending.len();
        let sid = frame.stream_id;
        let mut flags = 0u8;
        // ----- target resolution -----
        let mut existing = None;
        if let Some(e) = me.streams.get(&sid) {
            if e.state.is_local_error() {
                return Ok(ignored(sid));
            }
            if e.state.is_closed() {
                // Completed normally but still stored: drop it and classify exactly as
                // if already forgotten (client: STREAM_CLOSED; server: the decreased-id
                // connection error below, h2 recv.rs `open` L127).
                flags |= me.drop_closed_stored(sid);
                if me.role == Role::Client {
                    return Err(stream_err(sid, Reason::STREAM_CLOSED));
                }
            } else {
                existing = Some(sid);
            }
        }
        let Some(target) = existing else {
            if me.in_reset_store(sid) {
                return Ok(ignored(sid));
            }
            return match me.role {
                Role::Client => {
                    me.ensure_not_idle(sid)?;
                    Err(stream_err(sid, Reason::STREAM_CLOSED))
                }
                Role::Server => {
                    {
                        // A stream opened after our final (phase-2) graceful GOAWAY is
                        // silently IGNORED (h2 recv_headers L431). Through phase 1
                        // `max_stream_id` is still 2^31-1: in-flight requests are served.
                        if sid > me.max_stream_id {
                            return Ok(ignored(sid));
                        }
                        // A new request: a strictly-increasing client-initiated (odd) id.
                        if !StreamId::from(sid).is_client_initiated() || sid <= me.last_recv_id {
                            return Err(protocol_err(
                                Reason::PROTOCOL_ERROR,
                                &format!("invalid new stream id {sid}"),
                            ));
                        }
                        me.last_recv_id = sid;
                        if me.num_active >= me.max_concurrent {
                            // Over the limit we advertised: REFUSED_STREAM (h2 recv.rs
                            // `open` L145 -> counts.rs `can_inc_num_recv_streams`).
                            return Err(stream_err(sid, Reason::REFUSED_STREAM));
                        }
                        let Some(handle) = spare else {
                            return Err(PyValueError::new_err(
                                "recv_headers: the server role needs a spare stream handle",
                            ));
                        };
                        let mut e = StreamEntry::new(
                            handle,
                            me.peer.initial_window_size,
                            me.recv_init,
                            false,
                        );
                        // state.rs `recv_open`: receiving the request HEADERS opens it.
                        e.state
                            .recv_open(frame.end_stream, false)
                            .map_err(|err| map_proto_err(&err))?;
                        let handle = e.handle.clone_ref_unchecked();
                        me.streams.insert(sid, e);
                        me.num_active += 1;
                        // Processed (h2 recv.rs L167 `last_processed_id`): only accepted
                        // streams count toward the GOAWAY last-stream-id.
                        me.last_processed_id = sid;
                        me.apply_content_length(sid, frame)?; // may raise a stream error
                        // Queued, not yet pulled by the app (the Rapid-Reset cap, H2-4).
                        me.pending_accept.insert(sid);
                        Ok(RecvHeadersVerdict {
                            kind: HEADERS_OPENED,
                            handle: Some(handle),
                            stream_id: sid,
                            eof: frame.end_stream,
                            flags: flags | me.wake_flag(before),
                        })
                    }
                }
            };
        };
        let e = me.streams.get_mut(&target).expect("resolved");
        let handle = e.handle.clone_ref_unchecked();
        if e.state.is_recv_headers() {
            // A response head (or an interim 1xx); recv_open fully applies END_STREAM.
            let informational = frame.is_informational;
            if informational && frame.end_stream {
                // A 1xx cannot carry END_STREAM (RFC 9113 §8.1): reset the stream (F38).
                return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
            }
            e.state
                .recv_open(frame.end_stream, informational)
                .map_err(|err| map_proto_err(&err))?;
            if informational {
                return Ok(ignored(sid)); // interim responses are not surfaced (F38)
            }
            me.apply_content_length(sid, frame)?;
            if frame.end_stream {
                flags |= me.close_stream(sid); // recv_open already closed the recv half
            }
            return Ok(RecvHeadersVerdict {
                kind: HEADERS_HEAD,
                handle: Some(handle),
                stream_id: sid,
                eof: frame.end_stream,
                flags: flags | me.wake_flag(before),
            });
        }
        // A HEADERS frame after the head = trailers (h2 recv_trailers).
        if !frame.end_stream {
            return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
        }
        e.state.recv_close().map_err(|err| map_proto_err(&err))?;
        if !e.content_length.is_satisfied() {
            return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
        }
        flags |= me.close_stream(sid);
        Ok(RecvHeadersVerdict {
            kind: HEADERS_TRAILERS,
            handle: Some(handle),
            stream_id: sid,
            eof: true,
            flags: flags | me.wake_flag(before),
        })
    }

    /// h2 streams.rs `recv_data` (L350) -> recv.rs `recv_data` (L641): validate
    /// state, consume the connection + stream recv windows, check content-length,
    /// charge the DATA-framing budget, deliver the payload.
    fn recv_data(&self, py: Python<'_>, frame: &Data) -> PyResult<RecvDataVerdict> {
        let mut me = self.lock();
        let before = me.pending.len();
        // Flow control counts padding: `sz` is the flow-controlled length (h2 recv.rs
        // L643); content-length counts the payload only.
        let sz = frame.flow_controlled_len as u32;
        let payload_len = frame.data.bind(py).len()?;
        let sid = match me.recv_lookup(frame.stream_id) {
            Ok(Some(sid)) => sid,
            Ok(None) => {
                // Swallow + reclaim (h2 `ignore_data`): the peer counted these bytes.
                me.consume_conn_recv(sz)?;
                me.reclaim_conn(sz);
                return Ok(RecvDataVerdict {
                    handle: None,
                    payload: None,
                    budgeted: false,
                    eof: false,
                    flags: me.wake_flag(before),
                });
            }
            Err(err) => {
                me.consume_conn_recv(sz)?;
                me.reclaim_conn(sz);
                return Err(err);
            }
        };
        // DATA is only valid while the recv half is streaming (h2 recv_data L653),
        // checked BEFORE consuming the window: the connection is torn down.
        if !me.streams[&sid].state.is_recv_streaming() {
            return Err(protocol_err(
                Reason::PROTOCOL_ERROR,
                &format!("unexpected DATA on stream {sid}"),
            ));
        }
        // Consume the connection + stream windows atomically: a connection-window
        // overrun raises (connection FLOW_CONTROL_ERROR); a stream-window overrun
        // RSTs just that stream after reclaiming the connection window.
        me.consume_conn_recv(sz)?;
        let e = me.streams.get_mut(&sid).expect("live");
        if fc_send_data(&mut e.recv_flow, sz).is_err() {
            me.reclaim_conn(sz);
            return Err(stream_err(sid, Reason::FLOW_CONTROL_ERROR));
        }
        if !e.content_length.dec(payload_len as u64) {
            // More data than declared (or any on a HEAD response).
            me.reclaim_conn(sz);
            return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
        }
        if frame.end_stream && !e.content_length.is_satisfied() {
            // Less data than declared (h2 checks before pushing the Data event).
            me.reclaim_conn(sz);
            return Err(stream_err(sid, Reason::PROTOCOL_ERROR));
        }
        e.recv_unreleased += sz;
        let handle = e.handle.clone_ref_unchecked();
        // The app only ever sees the payload and can release only that much:
        // auto-release the padding overhead now (h2 recv.rs L740-750).
        if frame.padding > 0 {
            me.release_capacity_inner(sid, frame.padding as u32, false);
        }
        // DATA-framing budget (h2 0.4.19 counts.rs): a final frame is never budgeted;
        // an empty non-final frame counts against its own cap and is dropped.
        let is_budgeted = !frame.end_stream;
        if is_budgeted {
            me.record_data_frame(sid, payload_len)?;
            if payload_len == 0 {
                return Ok(RecvDataVerdict {
                    handle: None,
                    payload: None,
                    budgeted: false,
                    eof: false,
                    flags: me.wake_flag(before),
                });
            }
        }
        let mut flags = 0;
        if frame.end_stream {
            let e = me.streams.get_mut(&sid).expect("live");
            e.state.recv_close().map_err(|err| map_proto_err(&err))?;
            flags |= me.close_stream(sid); // no-op unless the send half is also closed
        }
        Ok(RecvDataVerdict {
            handle: Some(handle),
            payload: Some(frame.data.clone_ref(py)),
            budgeted: is_budgeted,
            eof: frame.end_stream,
            flags: flags | me.wake_flag(before),
        })
    }

    /// h2 streams.rs `recv_window_update` (L376) -> send.rs. Returns the handles
    /// whose senders must be woken (`window_evt`).
    fn recv_window_update(&self, frame: &WindowUpdate) -> PyResult<Vec<Py<PyAny>>> {
        let mut me = self.lock();
        if frame.stream_id == 0 {
            // h2 prioritize.rs `recv_connection_window_update`: `inc_window` then
            // `assign_connection_capacity` — the window and its capacity move together.
            me.conn_send
                .inc_window(frame.increment)
                .and_then(|()| me.conn_send.assign_capacity(frame.increment))
                .map_err(map_reason)?;
            return Ok(me
                .streams
                .values()
                .filter(|e| !e.finished)
                .map(|e| e.handle.clone_ref_unchecked())
                .collect());
        }
        let sid = frame.stream_id;
        let Some(e) = me.streams.get_mut(&sid) else {
            if me.reset_streams.contains_key(&sid) {
                return Ok(Vec::new()); // locally reset -> ignore
            }
            me.ensure_not_idle(sid)?; // idle -> connection error
            return Ok(Vec::new()); // forgotten stream -> ignore
        };
        // A stream send-window overflow is a *stream* error (h2 send.rs
        // `recv_stream_window_update`), not a connection teardown.
        if e.send_flow
            .inc_window(frame.increment)
            .and_then(|()| e.send_flow.assign_capacity(frame.increment))
            .is_err()
        {
            return Err(stream_err(sid, Reason::FLOW_CONTROL_ERROR));
        }
        Ok(vec![e.handle.clone_ref_unchecked()])
    }

    /// h2 streams.rs `recv_reset` (L355). Returns `(handle, stop, notify_body, flags)`
    /// for a live stream: the driver publishes `stop` on the handle, then sets
    /// `reset_evt` + `window_evt`, and — only when the body had not ended
    /// (`notify_body`) — wakes the head waiter and ends the body queue with `stop`.
    fn recv_reset(&self, frame: &RstStream) -> PyResult<Option<(Py<PyAny>, Stopped, bool, u8)>> {
        let mut me = self.lock();
        let before = me.pending.len();
        let sid = frame.stream_id;
        if sid == 0 {
            return Err(protocol_err(
                Reason::PROTOCOL_ERROR,
                "RST_STREAM on stream 0",
            ));
        }
        if !me.streams.contains_key(&sid) {
            if me.reset_streams.contains_key(&sid) {
                return Ok(None);
            }
            me.ensure_not_idle(sid)?;
            return Ok(None);
        }
        // h2 recv.rs L886 (hyper#2877): a peer resetting a stream the app hasn't
        // accepted yet no longer counts as concurrent, so a separate, smaller cap
        // bounds a HEADERS+RST flood -> GOAWAY(ENHANCE_YOUR_CALM).
        if me.role == Role::Server && me.pending_accept.contains(&sid) {
            if me.remote_reset_pending.len() >= me.max_pending_accept_reset {
                return Err(protocol_err(Reason::ENHANCE_YOUR_CALM, "too_many_resets"));
            }
            me.remote_reset_pending.insert(sid);
        }
        let e = me.streams.get_mut(&sid).expect("live");
        if e.finished {
            return Ok(None); // already closed: the response stood, nothing to notify
        }
        // A reset AFTER we already received the full message is benign: the message
        // stands (h2 0.4.16 `Closed(ErrorAfterEndStream)` preserves `is_recv_end_stream`).
        let recv_ended = e.state.is_recv_end_stream();
        e.state.recv_reset(
            frame::Reset::new(StreamId::from(sid), Reason::from(frame.error_code)),
            false,
        );
        // The reason now lives in the state: the SEND side observes every reset,
        // post-END_STREAM included (`ensure_reason`); the reader only if the message
        // had not stood (`ensure_recv_open`).
        let handle = e.handle.clone_ref_unchecked();
        let stop = e.stopped();
        // Reclaim the connection window consumed by this stream's unread data (h2
        // recv.rs `release_closed_capacity` on `transition_after`).
        me.reclaim_stream_accounting(sid);
        let flags = me.close_stream(sid) | me.wake_flag(before);
        Ok(Some((handle, stop, !recv_ended, flags)))
    }

    /// h2 proto/settings.rs `recv_settings` (L41-88) + the value application. An
    /// ACK applies our own settings; a peer SETTINGS is ACKed (queued) then applied.
    /// Returns `(initial, wake_windows, flags)`: `initial` = the peer's first
    /// SETTINGS landed (the client is now ready); `wake_windows` = handles whose
    /// send window grew.
    fn recv_settings(&self, frame: &SettingsFrame) -> PyResult<(bool, Vec<Py<PyAny>>, u8)> {
        let mut me = self.lock();
        let before = me.pending.len();
        if frame.ack {
            me.settings.recv_ack()?;
            let mut dec = self.decoder.lock().unwrap(); // state → decoder: one step for both
            me.apply_local_settings(&mut dec)?;
            return Ok((false, Vec::new(), 0));
        }
        // ACK first, then apply (h2 `poll_send` orders the ACK before the application).
        codec::encode_settings_ack(&mut me.pending);
        let initial = me.settings.recv_remote();
        let (wake, flags) = me.apply_remote_settings(frame)?;
        let flags = flags | me.wake_flag(before);
        Ok((initial, wake, flags))
    }

    /// A PING: answer it (queued PONG), or — its ACK — drive phase 2 of the server's
    /// graceful shutdown (h2 connection.rs L558-560): the shutdown PING's ack means
    /// the client has processed everything it sent before our GOAWAY(2^31-1), so the
    /// highest stream we accepted is the true last-processed id; send the final
    /// GOAWAY with it, and end the accept loop if already drained.
    fn recv_ping(&self, py: Python<'_>, frame: &Ping) -> PyResult<u8> {
        let mut me = self.lock();
        let before = me.pending.len();
        let data = frame.data.bind(py).as_bytes().to_vec();
        if !frame.ack {
            codec::encode_ping(&mut me.pending, &data, true)?;
            return Ok(me.wake_flag(before));
        }
        if me.role != Role::Server || data != SHUTDOWN_PING || !me.graceful || me.shutdown_final {
            return Ok(0);
        }
        me.shutdown_final = true;
        me.max_stream_id = me.last_processed_id;
        let last = me.last_processed_id;
        codec::encode_go_away(&mut me.pending, last, u32::from(Reason::NO_ERROR), &[]);
        let mut flags = me.wake_flag(before);
        if me.num_active == 0 {
            flags |= FLAG_STOP_ACCEPTING;
        }
        Ok(flags)
    }

    /// Peer sent GOAWAY (h2 proto/go_away.rs + streams.rs `recv_go_away`): refuse new
    /// streams; streams > last_stream_id were not processed (retryable, aborted with
    /// the GOAWAY reason); streams <= last_stream_id keep running. A later GOAWAY may
    /// not raise the last-stream-id (h2 send.rs `recv_go_away` L447). Returns the
    /// aborted `(handle, stop)` pairs and flags (the client's slot waiters wake).
    fn recv_go_away(
        &self,
        py: Python<'_>,
        frame: &GoAway,
    ) -> PyResult<(Vec<(Py<PyAny>, Stopped)>, u8)> {
        let mut me = self.lock();
        let last = frame.last_stream_id;
        if let Some(prev) = me.goaway_last_id
            && last > prev
        {
            return Err(protocol_err(
                Reason::PROTOCOL_ERROR,
                "GOAWAY may not raise last_stream_id",
            ));
        }
        me.goaway_last_id = Some(last);
        me.goaway = Some(GoAwayInfo {
            last_stream_id: last,
            reason: frame.error_code,
            debug_data: Bytes::copy_from_slice(frame.debug_data.bind(py).as_bytes()),
        });
        let mut aborted = Vec::new();
        let mut flags = 0;
        if me.role == Role::Client {
            let ids: Vec<u32> = me.streams.keys().copied().filter(|id| *id > last).collect();
            for sid in ids {
                let (x, f) = me.abort_stream(sid, Some(frame.error_code));
                flags |= f;
                if let Some(x) = x {
                    aborted.push(x);
                }
            }
            flags |= FLAG_SLOT_FREED; // wake open/ready waiters (they re-check the GOAWAY)
        }
        Ok((aborted, flags))
    }

    /// The idle-after-peer-GOAWAY reply check the read pump runs after each batch
    /// (see `Inner::maybe_goaway_reply`).
    fn maybe_goaway_reply(&self) -> u8 {
        self.lock().maybe_goaway_reply()
    }

    /// Queue our GOAWAY(reason) for a protocol violation we detected (h2
    /// `go_away_now`); the last-stream-id is the role's.
    fn send_goaway(&self, reason: u32) -> u8 {
        let mut me = self.lock();
        let before = me.pending.len();
        let last = me.goaway_last_stream_id();
        codec::encode_go_away(&mut me.pending, last, reason, &[]);
        me.wake_flag(before)
    }

    /// The connection failed (h2 connection.rs `handle_poll2_result` -> streams.rs
    /// `handle_error`): record the error (first writer wins; `exc` is a
    /// traceback-free copy, or `None` for a closure described by `message`) and fan
    /// it out to every stream. Returns the `(handle, stop)` pairs to notify; the
    /// caller then wakes the role waiters (FLAG_CONN_DONE semantics).
    #[pyo3(signature = (exc=None, message="connection closed"))]
    fn fail(&self, exc: Option<Py<PyAny>>, message: &str) -> Vec<(Py<PyAny>, Stopped)> {
        let mut me = self.lock();
        let unused = if me.error.is_none() {
            me.error = Some(match exc {
                Some(e) => ConnError::Py(e),
                None => ConnError::Closed(message.to_string()),
            });
            None
        } else {
            exc // not stored: dropped below, outside the guard
        };
        let out = me.fail_all();
        drop(me);
        drop(unused);
        out
    }

    /// The stored connection error, if any (a `clone_ref` of the stored copy, or a
    /// fresh `ConnectionClosedError` for a closure decided here). The caller raises
    /// `fresh_exc(err) from err`.
    fn conn_error(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let msg = {
            let me = self.lock();
            match &me.error {
                None => return None,
                Some(ConnError::Py(e)) => return Some(e.clone_ref(py)),
                Some(ConnError::Closed(m)) => m.clone(),
            }
        };
        Some(
            ConnectionClosedError::new_err((msg,))
                .into_value(py)
                .into_any(),
        )
    }

    /// The peer's GOAWAY as `(last_stream_id, error_code, debug_data)`, if any.
    fn goaway_info(&self, py: Python<'_>) -> Option<(u32, u32, Py<PyBytes>)> {
        let me = self.lock();
        me.goaway.as_ref().map(|g| {
            (
                g.last_stream_id,
                g.reason,
                PyBytes::new(py, &g.debug_data).unbind(),
            )
        })
    }

    /// The connection can serve no more requests: failed, or the peer sent GOAWAY.
    fn is_closed(&self) -> bool {
        self.lock().is_dead()
    }

    fn is_failed(&self) -> bool {
        self.lock().error.is_some()
    }

    // ===== opening (client: h2 streams.rs send_request / counts.rs) =====

    /// Atomically claim a MAX_CONCURRENT_STREAMS slot (h2 counts.rs
    /// `inc_num_send_streams`, gated by `can_inc_num_send_streams`).
    fn try_claim_slot(&self) -> bool {
        let mut me = self.lock();
        if me.stream_limit.is_none_or(|lim| me.num_open_streams < lim) {
            me.num_open_streams += 1;
            return true;
        }
        false
    }

    /// A slot is free (non-reserving; h2 `poll_ready` is likewise non-reserving).
    fn can_open(&self) -> bool {
        let me = self.lock();
        me.stream_limit.is_none_or(|lim| me.num_open_streams < lim)
    }

    /// Undo a claimed slot that did not become a live stream.
    fn release_slot_count(&self) {
        let mut me = self.lock();
        me.num_open_streams = me.num_open_streams.saturating_sub(1);
    }

    /// Set MAX_CONCURRENT_STREAMS (authoritative: below the current open count,
    /// further opens block until enough streams close).
    #[pyo3(signature = (limit))]
    fn apply_stream_limit(&self, limit: Option<usize>) {
        self.lock().stream_limit = limit;
    }

    /// Open a request stream with a claimed slot: allocate the id, `send_open`,
    /// insert, HPACK-encode + queue the HEADERS — one step, so ids are strictly
    /// increasing on the wire. `None`: the connection failed or GOAWAY'd (the slot
    /// is released; the caller raises the stored condition). Raises
    /// `H2ProtocolError(REFUSED_STREAM)` once the id space is exhausted (F43).
    #[pyo3(signature = (method, target, headers, end_stream, is_head, handle, *, scheme=None, authority=None))]
    #[allow(clippy::too_many_arguments)]
    fn open_stream(
        &self,
        method: &str,
        target: &str,
        headers: Option<&HeaderMap>,
        end_stream: bool,
        is_head: bool,
        handle: Py<PyAny>,
        scheme: Option<&str>,
        authority: Option<&str>,
    ) -> PyResult<Option<u32>> {
        // Snapshot + parse BEFORE the lock (no nested HeaderMap lock, no failure
        // half-way through the critical section).
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        let mut me = self.lock();
        let before = me.pending.len();
        if me.is_dead() {
            me.num_open_streams = me.num_open_streams.saturating_sub(1);
            drop(me);
            drop(handle);
            return Ok(None);
        }
        let sid = me.next_id;
        if sid > MAX_STREAM_ID {
            me.num_open_streams = me.num_open_streams.saturating_sub(1);
            drop(me);
            drop(handle);
            return Err(protocol_err(
                Reason::REFUSED_STREAM,
                "stream ID space exhausted (OverflowedStreamId)",
            ));
        }
        let hframe = match codec::build_request_headers(
            sid, method, target, fields, end_stream, scheme, authority,
        ) {
            Ok(h) => h,
            Err(e) => {
                me.num_open_streams = me.num_open_streams.saturating_sub(1);
                drop(me);
                drop(handle);
                return Err(e);
            }
        };
        me.next_id += 2;
        let mut e = StreamEntry::new(handle, me.peer.initial_window_size, me.recv_init, is_head);
        e.holds_slot = true;
        e.state.send_open(end_stream).map_err(map_user_err)?;
        me.streams.insert(sid, e);
        me.num_active += 1;
        let mut pending = std::mem::take(&mut me.pending);
        me.encoder.encode_headers(hframe, &mut pending);
        me.pending = pending;
        debug_assert!(me.pending.len() > before);
        Ok(Some(sid))
    }

    // ===== sending (h2 send.rs send_data / send_trailers, share.rs) =====

    /// Frame and queue up to one DATA frame of `data[offset:]` — reserve
    /// min(connection window, stream window, send-buffer room, peer max_frame_size),
    /// encode, append, credit `send_buffered`: one step (h2 `poll_capacity` +
    /// `send_data`). `sent == 0` with `done == False` means "no window now": the
    /// caller waits on the stream's `window_evt` and retries. An empty `data` is sent
    /// as an empty DATA frame (h2 prioritize.rs L202-213).
    fn send_data(
        &self,
        sid: u32,
        data: &[u8],
        offset: usize,
        end_stream: bool,
    ) -> PyResult<SendVerdict> {
        let mut me = self.lock();
        let before = me.pending.len();
        let Some(e) = me.streams.get(&sid) else {
            return Ok(stopped_send(Stopped {
                reason: None,
                conn: false,
            }));
        };
        // h2 `send_data` first checks `is_send_streaming()` and errors if not: never
        // frame on a stream the peer reset.
        if !e.state.is_send_streaming() {
            return Ok(stopped_send(e.stopped()));
        }
        if data.is_empty() {
            let mut pending = std::mem::take(&mut me.pending);
            let r = me.encoder.encode_data(sid, &[], end_stream, &mut pending);
            me.pending = pending;
            r?;
            let tail = if end_stream {
                // h2 `send_data`: the END_STREAM frame closes the send half in the same step.
                me.streams.get_mut(&sid).expect("live").state.send_close();
                me.finish_stream(sid)
            } else {
                (None, None, 0)
            };
            let flags = me.wake_flag(before);
            return Ok(completed_send(0, flags, tail));
        }
        let budget = me.send_budget(e);
        if budget == 0 {
            return Ok(SendVerdict {
                sent: 0,
                done: false,
                stopped: None,
                flags: 0,
                handle: None,
                reader_stop: None,
            });
        }
        let want = data.len() - offset;
        let n = budget.min(want).min(me.peer.max_frame_size as usize);
        let last = end_stream && n == want;
        fc_send_data(&mut me.conn_send, n as u32).map_err(map_reason)?;
        let e = me.streams.get_mut(&sid).expect("live");
        fc_send_data(&mut e.send_flow, n as u32).map_err(map_reason)?;
        e.send_buffered += n;
        let mut pending = std::mem::take(&mut me.pending);
        let r = me
            .encoder
            .encode_data(sid, &data[offset..offset + n], last, &mut pending);
        me.pending = pending;
        r?;
        match me.credit.last_mut() {
            Some((last, m)) if *last == sid => *m += n,
            _ => me.credit.push((sid, n)),
        }
        if last {
            // h2 `send_data`: the END_STREAM frame closes the send half in the same step
            // (`is_send_streaming` held above, under this same lock: no reset in between).
            me.streams.get_mut(&sid).expect("live").state.send_close();
            let tail = me.finish_stream(sid);
            let flags = me.wake_flag(before);
            return Ok(completed_send(n, flags, tail));
        }
        Ok(SendVerdict {
            sent: n,
            done: n == want,
            stopped: None,
            flags: me.wake_flag(before),
            handle: None,
            reader_stop: None,
        })
    }

    /// A trailing HEADERS frame (END_STREAM) after the body (h2 `send_trailers`): the
    /// send half closes in this same step, as h2's does.
    fn send_trailers(&self, sid: u32, trailers: &HeaderMap) -> SendVerdict {
        let hframe = codec::build_trailers(sid, trailers.snapshot());
        let mut me = self.lock();
        let before = me.pending.len();
        let Some(e) = me.streams.get_mut(&sid) else {
            return stopped_send(Stopped {
                reason: None,
                conn: false,
            });
        };
        if !e.state.is_send_streaming() {
            return stopped_send(e.stopped());
        }
        e.state.send_close();
        let mut pending = std::mem::take(&mut me.pending);
        me.encoder.encode_headers(hframe, &mut pending);
        me.pending = pending;
        let tail = me.finish_stream(sid);
        let flags = me.wake_flag(before);
        completed_send(0, flags, tail)
    }

    /// Server: send the response HEADERS (h2 server.rs `SendResponse::send_response`;
    /// state.rs `send_open`). A stream the peer reset (or the connection dropped)
    /// while the handler was computing is hyper's first `poll_reset` window
    /// (proto/h2/server.rs L458): returns the stop instead (a bare stop for a stream
    /// closed with no reason to report here).
    #[pyo3(signature = (sid, status, headers, end_stream))]
    fn send_response_head(
        &self,
        sid: u32,
        status: u16,
        headers: Option<&HeaderMap>,
        end_stream: bool,
    ) -> PyResult<SendVerdict> {
        let fields = headers.map(HeaderMap::snapshot).unwrap_or_default();
        // RFC 9113 §8.2.2 rejection BEFORE the state transition: a rejected call
        // leaves the stream untouched and still able to send a valid response.
        codec::check_send_fields(&fields)?;
        let mut me = self.lock();
        let before = me.pending.len();
        let auto_date = me.auto_date;
        // A stream no longer stored (closed and settled) or Closed with no reason here:
        // the stop the pump published on the handle decides (a post-END_STREAM reset,
        // hyper's `poll_reset` window) — else the caller reports "already closed".
        let Some(e) = me.streams.get_mut(&sid) else {
            return Ok(stopped_send(Stopped {
                reason: None,
                conn: false,
            }));
        };
        if e.state.is_closed() {
            return Ok(stopped_send(e.stopped()));
        }
        let hframe = codec::build_response_headers(sid, status, fields, end_stream, auto_date)?;
        e.state.send_open(end_stream).map_err(map_user_err)?;
        let mut pending = std::mem::take(&mut me.pending);
        me.encoder.encode_headers(hframe, &mut pending);
        me.pending = pending;
        // A bodiless response: HEADERS closed the send half — the response is complete
        // in this step (the request's recv half goes with it, `finish_stream`).
        let tail = if end_stream {
            me.finish_stream(sid)
        } else {
            (None, None, 0)
        };
        let flags = me.wake_flag(before);
        Ok(completed_send(0, flags, tail))
    }

    /// Abort a stream: RST_STREAM(reason) + full teardown (h2 `send_reset`).
    /// `initiator`: "user" (a caller cancel) | "library" (a reset we force after a
    /// peer violation). Returns the handle to notify (`None`: no live stream).
    #[pyo3(signature = (sid, reason, initiator="user"))]
    fn reset_stream(&self, sid: u32, reason: u32, initiator: &str) -> PyResult<ResetVerdict> {
        let initiator = parse_initiator(initiator)?;
        let mut me = self.lock();
        let before = me.pending.len();
        let (handle, stop, flags) = me.reset_stream(sid, reason, initiator);
        let flags = flags | me.wake_flag(before);
        Ok(ResetVerdict {
            handle,
            stop: None,
            flags,
        }
        .with_stop_py(stop))
    }

    /// Reset a stream after a stream-level protocol violation by the peer, counting
    /// toward the ENHANCE_YOUR_CALM cap (h2 `max_local_error_reset_streams`, the
    /// Rapid-Reset / malformed-flood defence; F17). A stream we've already forgotten
    /// enters the reset store and draws one RST_STREAM.
    fn reset_on_error(&self, sid: u32, reason: u32) -> PyResult<ResetVerdict> {
        let mut me = self.lock();
        let before = me.pending.len();
        me.local_error_resets += 1;
        if let Some(max) = me.max_local_error_resets
            && me.local_error_resets > max
        {
            return Err(protocol_err(
                Reason::ENHANCE_YOUR_CALM,
                "too many stream resets",
            ));
        }
        if me.streams.contains_key(&sid) {
            let (handle, stop, flags) = me.reset_stream(sid, reason, Initiator::Library);
            let flags = flags | me.wake_flag(before);
            return Ok(ResetVerdict {
                handle,
                stop: None,
                flags,
            }
            .with_stop_py(stop));
        }
        me.clear_expired_reset_streams();
        if me.reset_streams.len() < RESET_STREAM_MAX {
            me.reset_streams.insert(sid, Instant::now());
        }
        codec::encode_rst_stream(&mut me.pending, sid, reason);
        let flags = me.wake_flag(before);
        Ok(ResetVerdict {
            handle: None,
            stop: None,
            flags,
        })
    }

    /// The body's `aclose`: a fully-received body needs no RST (h2 guards its
    /// Drop-reset on `!eos`) but still releases every buffered-but-unread byte's
    /// connection window (`release_closed_capacity`, streams.rs L1670-1676); an
    /// unfinished one is cancelled (RST_STREAM(CANCEL)).
    fn aclose_body(&self, sid: u32) -> ResetVerdict {
        let mut me = self.lock();
        let before = me.pending.len();
        let Some(e) = me.streams.get(&sid) else {
            return ResetVerdict {
                handle: None,
                stop: None,
                flags: 0,
            };
        };
        if e.state.is_recv_end_stream() {
            me.reclaim_stream_accounting(sid);
            let flags = me.close_stream(sid) | me.wake_flag(before);
            return ResetVerdict {
                handle: None,
                stop: None,
                flags,
            };
        }
        let (handle, stop, flags) =
            me.reset_stream(sid, u32::from(Reason::CANCEL), Initiator::User);
        let flags = flags | me.wake_flag(before);
        ResetVerdict {
            handle,
            stop: None,
            flags,
        }
        .with_stop_py(stop)
    }

    // ===== recv-side flow control (h2 recv.rs release_capacity) =====

    /// Return `n` bytes of recv capacity and queue WINDOW_UPDATE(s) when the reclaimed
    /// amount crosses the aggregation threshold (stream + connection). A no-op for a
    /// stream whose in-flight capacity was already reclaimed (F22).
    fn release_capacity(&self, sid: u32, n: u32) -> u8 {
        let mut me = self.lock();
        let before = me.pending.len();
        me.release_capacity_inner(sid, n, true);
        me.wake_flag(before)
    }

    /// Return a consumed BUDGETED chunk's buffering charge (h2 `release_data_frame`).
    fn release_data_frame(&self, sid: u32, payload_len: usize) {
        let mut me = self.lock();
        let Some(e) = me.streams.get_mut(&sid) else {
            return;
        };
        if payload_len == 0 || payload_len >= super::streams::DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD
        {
            return;
        }
        // Clamped to the stream's outstanding charge: never double-credit against a
        // concurrent teardown's bulk release.
        let charge = (super::streams::DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD - payload_len)
            .min(e.data_budget_charged);
        e.data_budget_charged -= charge;
        me.data_budget.release(charge);
        me.maybe_remove_settled(sid);
    }

    /// Server: a queued request was pulled by the app (h2 `dec_num_remote_reset_streams`).
    fn accepted(&self, sid: u32) {
        let mut me = self.lock();
        me.pending_accept.remove(&sid);
        me.remote_reset_pending.remove(&sid);
    }

    // ===== the write pump =====

    /// `close()`: the pump drains the buffer and exits.
    fn stop_pump(&self) {
        self.lock().stop = true;
    }

    /// Swap out everything committed to the pending-send buffer as `(bytes,
    /// stopping)`. The per-stream credit batch moves to `credit_written`'s.
    fn take_pending(&self, py: Python<'_>) -> (Py<PyBytes>, bool) {
        let mut me = self.lock();
        let data = PyBytes::new(py, &me.pending).unbind();
        me.pending.clear();
        let batch = std::mem::take(&mut me.credit);
        me.inflight_credit.extend(batch);
        (data, me.stop)
    }

    /// The batch is on the wire (or the connection is dead): credit each stream's
    /// `send_buffered` back and return the handles whose senders must be woken —
    /// one handle per stream, however many of its frames the batch carried.
    fn credit_written(&self) -> Vec<Py<PyAny>> {
        let mut me = self.lock();
        let batch = std::mem::take(&mut me.inflight_credit);
        let mut wake = Vec::new();
        let mut woken: HashSet<u32> = HashSet::new();
        for (sid, n) in batch {
            if let Some(e) = me.streams.get_mut(&sid) {
                e.send_buffered = e.send_buffered.saturating_sub(n);
                if woken.insert(sid) {
                    wake.push(e.handle.clone_ref_unchecked());
                }
            }
        }
        wake
    }

    fn has_pending(&self) -> bool {
        !self.lock().pending.is_empty()
    }

    // ===== graceful shutdown (server; h2 Connection::graceful_shutdown) =====

    /// PHASE 1: GOAWAY(2^31-1, NO_ERROR) + the shutdown PING, keep serving. Idempotent.
    fn begin_graceful_shutdown(&self) -> PyResult<u8> {
        let mut me = self.lock();
        if me.graceful {
            return Ok(0);
        }
        me.graceful = true;
        let before = me.pending.len();
        codec::encode_go_away(
            &mut me.pending,
            MAX_STREAM_ID,
            u32::from(Reason::NO_ERROR),
            &[],
        );
        codec::encode_ping(&mut me.pending, &SHUTDOWN_PING, false)?;
        Ok(me.wake_flag(before))
    }

    // ===== observability (tests / diagnostics / the facade) =====

    #[getter]
    fn last_processed_id(&self) -> u32 {
        self.lock().last_processed_id
    }

    #[getter]
    fn max_stream_id(&self) -> u32 {
        self.lock().max_stream_id
    }

    #[getter]
    fn graceful(&self) -> bool {
        self.lock().graceful
    }

    #[getter]
    fn shutdown_final(&self) -> bool {
        self.lock().shutdown_final
    }

    #[getter]
    fn num_streams(&self) -> usize {
        self.lock().num_active
    }

    fn has_stream(&self, sid: u32) -> bool {
        self.lock().streams.contains_key(&sid)
    }

    fn is_recv_end_stream(&self, sid: u32) -> bool {
        self.lock()
            .streams
            .get(&sid)
            .is_some_and(|e| e.state.is_recv_end_stream())
    }

    fn stream_state(&self, sid: u32) -> Option<String> {
        self.lock()
            .streams
            .get(&sid)
            .map(|e| format!("{:?}", e.state))
    }

    fn stream_recv_unreleased(&self, sid: u32) -> Option<u32> {
        self.lock().streams.get(&sid).map(|e| e.recv_unreleased)
    }

    fn stream_recv_reclaimed(&self, sid: u32) -> Option<bool> {
        self.lock().streams.get(&sid).map(|e| e.recv_reclaimed)
    }

    fn stream_data_budget_charged(&self, sid: u32) -> Option<usize> {
        self.lock().streams.get(&sid).map(|e| e.data_budget_charged)
    }

    fn stream_send_buffered(&self, sid: u32) -> Option<usize> {
        self.lock().streams.get(&sid).map(|e| e.send_buffered)
    }

    fn stream_send_window(&self, sid: u32) -> Option<u32> {
        self.lock()
            .streams
            .get(&sid)
            .map(|e| e.send_flow.window_size())
    }

    #[getter]
    fn conn_send_window(&self) -> u32 {
        self.lock().conn_send.window_size()
    }

    #[getter]
    fn conn_recv_available(&self) -> i64 {
        isize::from(self.lock().conn_recv.available()) as i64
    }

    #[getter]
    fn data_frame_budget_available(&self) -> usize {
        self.lock().data_budget.available()
    }

    #[getter]
    fn data_frame_budget_empty_frames(&self) -> usize {
        self.lock().data_budget.empty_frames()
    }

    #[getter]
    fn peer_initial_window_size(&self) -> u32 {
        self.lock().peer.initial_window_size
    }

    #[getter]
    fn peer_max_frame_size(&self) -> u32 {
        self.lock().peer.max_frame_size
    }

    #[getter]
    fn peer_max_concurrent_streams(&self) -> Option<u32> {
        self.lock().peer.max_concurrent_streams
    }

    #[getter]
    fn stream_limit(&self) -> Option<usize> {
        self.lock().stream_limit
    }

    #[getter]
    fn num_open_streams(&self) -> usize {
        self.lock().num_open_streams
    }

    #[getter]
    fn max_pending_accept_reset_streams(&self) -> usize {
        self.lock().max_pending_accept_reset
    }

    #[getter]
    fn max_local_error_reset_streams(&self) -> Option<usize> {
        self.lock().max_local_error_resets
    }

    #[getter]
    fn local_error_resets(&self) -> usize {
        self.lock().local_error_resets
    }

    #[getter]
    fn goaway_replied(&self) -> bool {
        self.lock().goaway_replied
    }

    fn in_reset_store(&self, sid: u32) -> bool {
        self.lock().reset_streams.contains_key(&sid)
    }

    /// Test hook: the next client stream id (e.g. to exhaust the id space, F43).
    fn set_next_stream_id(&self, sid: u32) {
        self.lock().next_id = sid;
    }

    /// Test hook (server): the phase-2 last-stream-id as if graceful lowered it.
    fn set_max_stream_id(&self, sid: u32) {
        self.lock().max_stream_id = sid;
    }

    /// Test hook (server): the last-processed id as if requests up to it were accepted.
    fn set_last_processed_id(&self, sid: u32) {
        self.lock().last_processed_id = sid;
    }

    fn __repr__(&self) -> String {
        let me = self.lock();
        format!(
            "H2Streams(role={}, streams={}, active={}, failed={}, goaway={})",
            if me.role == Role::Client {
                "client"
            } else {
                "server"
            },
            me.streams.len(),
            me.num_active,
            me.error.is_some(),
            me.goaway.is_some(),
        )
    }
}

impl Inner {
    /// h2 recv.rs `release_capacity` (L458, stream) + `release_connection_capacity`
    /// (L435). `stream_side`: also re-advertise the stream window (not for the
    /// padding auto-release of a frame that just arrived... it is, in h2 — both go
    /// through `release_capacity`).
    fn release_capacity_inner(&mut self, sid: u32, n: u32, _stream_side: bool) {
        let Some(e) = self.streams.get_mut(&sid) else {
            return;
        };
        if e.recv_reclaimed {
            return; // already returned in bulk (F22): never credit twice
        }
        e.recv_unreleased = e.recv_unreleased.saturating_sub(n);
        let _ = e.recv_flow.assign_capacity(n);
        // No point re-advertising a stream the peer has already finished sending.
        if let Some(unclaimed) = e.recv_flow.unclaimed_capacity()
            && !e.state.is_recv_end_stream()
        {
            let _ = e.recv_flow.inc_window(unclaimed);
            codec::encode_window_update(&mut self.pending, sid, unclaimed);
        }
        self.reclaim_conn(n);
        self.maybe_remove_settled(sid);
    }
}

fn ignored(sid: u32) -> RecvHeadersVerdict {
    RecvHeadersVerdict {
        kind: HEADERS_IGNORED,
        handle: None,
        stream_id: sid,
        eof: false,
        flags: 0,
    }
}

fn stopped_send(stop: Stopped) -> SendVerdict {
    SendVerdict {
        sent: 0,
        done: false,
        stopped: Some(stop.into_py_unchecked()),
        flags: 0,
        handle: None,
        reader_stop: None,
    }
}

/// The verdict of a completed send step: `flags`, plus the server's end-of-response
/// notification when the request's recv half was reset (`finish_stream`).
fn completed_send(
    sent: usize,
    flags: u8,
    tail: (Option<Py<PyAny>>, Option<Stopped>, u8),
) -> SendVerdict {
    let (handle, stop, tail_flags) = tail;
    SendVerdict {
        sent,
        done: true,
        stopped: None,
        flags: flags | tail_flags,
        handle,
        reader_stop: stop.map(Stopped::into_py_unchecked),
    }
}

impl Stopped {
    fn into_py_unchecked(self) -> Py<Stopped> {
        Python::attach(|py| Py::new(py, self).expect("alloc"))
    }
}

impl ResetVerdict {
    fn with_stop_py(mut self, stop: Option<Stopped>) -> Self {
        self.stop = stop.map(Stopped::into_py_unchecked);
        self
    }
}
