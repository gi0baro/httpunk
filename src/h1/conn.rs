//! `H1ServerState` / `H1ClientState` — the connection state of one HTTP/1 server /
//! client connection under ONE mutex. Server first: the connection + current-request state of one HTTP/1 server
//! connection under ONE mutex: the mirror of hyper's single-owner `Conn` (the
//! `State { reading, writing, keep_alive, ... }` in proto/h1/conn.rs) for a runtime
//! where several tasks — the accept loop, the app's body reader, the responder,
//! the mid-message watcher, a host's `close()`/`graceful_shutdown()` — touch it
//! in parallel. Every method is one critical section that performs a whole
//! check-and-act and returns a verdict; the Python driver (`httpunk/h1/server.py`,
//! a subclass) owns only the async machinery and acts on the verdict after the
//! call returns. See HTTPUNK_RUST_STATE_DESIGN.md §3.2.
//!
//! Lock order (design rule 4): this state → `H1Codec` → `HeaderMap`; this state →
//! `H1BodyDecoder`. The codec and the decoder are never held together; nothing
//! takes any of them in reverse. The nested locks are taken only where a byte
//! move must be atomic with a state change (the arm decision, the drain, the
//! next-head hand-off, the detach); the per-chunk byte work of a response stays
//! on the codec alone, from the single-owner write path.
//!
//! Requests are identified by a sequence number (`seq`): the state holds ONE
//! current request, and a `ServerRequest` outliving its exchange gets a
//! deterministic `STALE` answer (§3.2 "Request identity").

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{PyTraverseError, PyVisit};

use super::codec::{H1BodyDecoder, H1Codec, RequestHead};
use crate::http::HeaderMap;

// ===== verdict codes =====

/// `begin_read()` / `drain_done()`: first the between-requests verdicts, then — once
/// positioned at the next head — how to read it.
pub const NEXT_NONE: u8 = 0; // the connection can serve no more: `next_request` returns None
pub const NEXT_RAISE: u8 = 1; // the current request was not answered (a caller error)
pub const NEXT_CLOSE: u8 = 2; // the unread body cannot be drained: closed (take the transport)
pub const NEXT_DRAIN: u8 = 3; // drain the unread body (one poll), then `drain_done`
pub const READ_PARSE: u8 = 4; // bytes of the next request are already buffered: parse first
pub const READ_SHUTDOWN: u8 = 5; // a graceful shutdown was requested: no idle read (hyper `KA::Disabled`)
pub const READ_WATCHER: u8 = 6; // the parked watcher's read is the idle read (its done event)
pub const READ_TRANSPORT: u8 = 7; // read the transport (the idle park is flagged)
pub const READ_EOF: u8 = 8; // the transport is gone: EOF

/// `respond_head()` / `detach()`.
pub const REQ_OK: u8 = 0;
pub const REQ_ALREADY: u8 = 1; // already responded / detached
pub const REQ_STALE: u8 = 2; // not the current request
pub const REQ_PARKED: u8 = 3; // detach: a mid-message read is parked on the transport
pub const REQ_PEER_CLOSED: u8 = 4; // respond: the client closed mid-request (`IncompleteMessage`)
pub const REQ_CONTINUE: u8 = 5; // respond: claimed; a `100 Continue` is being written — wait, call again

pub const CONSTANTS: [(&str, u8); 15] = [
    ("H1_NEXT_NONE", NEXT_NONE),
    ("H1_NEXT_RAISE", NEXT_RAISE),
    ("H1_NEXT_CLOSE", NEXT_CLOSE),
    ("H1_NEXT_DRAIN", NEXT_DRAIN),
    ("H1_READ_PARSE", READ_PARSE),
    ("H1_READ_SHUTDOWN", READ_SHUTDOWN),
    ("H1_READ_WATCHER", READ_WATCHER),
    ("H1_READ_TRANSPORT", READ_TRANSPORT),
    ("H1_READ_EOF", READ_EOF),
    ("H1_REQ_OK", REQ_OK),
    ("H1_REQ_ALREADY", REQ_ALREADY),
    ("H1_REQ_STALE", REQ_STALE),
    ("H1_REQ_PARKED", REQ_PARKED),
    ("H1_REQ_PEER_CLOSED", REQ_PEER_CLOSED),
    ("H1_REQ_CONTINUE", REQ_CONTINUE),
];

// ===== state =====

/// The mid-message watcher (hyper conn.rs `mid_message_detect_eof`): ONE parked
/// read in a bare task, never cancelled, dispatched by state when it completes.
struct Watcher {
    /// The bare task handle (`spawn_without_results`), stored by the arm path once
    /// the spawn returned; taken exactly once (the hand-off read, or `close()`).
    handle: Option<Py<PyAny>>,
    /// The Python event the watcher sets when its read completed.
    done: Py<PyAny>,
    /// Its bytes (`b""` = EOF); `None` on error / not yet.
    data: Option<Py<PyBytes>>,
    /// Its transport error, if any (traceback-stripped by the watcher).
    error: Option<Py<PyAny>>,
    completed: bool,
}

/// The current request (hyper's `Reading`/`Writing` halves + the head-time
/// decisions applied at completion).
struct Current {
    seq: u64,
    decoder: Option<Py<H1BodyDecoder>>,
    expect_continue: bool,
    http10: bool,
    /// The request was keep-alive (hyper `wants_keep_alive`'s request half).
    request_keep_alive: bool,
    /// The request asked for an upgrade (`Upgrade` header — hyper `wants_upgrade`).
    is_upgrade: bool,
    /// The request method is CONNECT (a 2xx makes the connection a tunnel).
    is_connect: bool,
    /// The request announced an upgrade (`Upgrade` / CONNECT — hyper `wants_upgrade`).
    upgrade_request: bool,
    /// The response head was sent (or the request detached).
    responded: bool,
    /// `respond_head` claimed the response but returned `REQ_CONTINUE` (a `100
    /// Continue` write was in flight): the claimant's next call finishes the head.
    head_pending: bool,
    /// The response is complete: body ended, connection state settled.
    response_done: bool,
    body_done: bool,
    /// A body reader is between its reads (the drain must not touch the transport).
    reading_body: bool,
    continue_sent: bool,
    /// The client closed its side (or the transport failed) mid-message.
    peer_closed: bool,
    /// `is_switch` is decided: the head is being sent.
    head_negotiated: bool,
    is_switch: bool,
    keep_alive: bool,
    /// `peer_closed()` / a streamed or push response asked for the read.
    watch_wanted: bool,
}

impl Current {
    fn none() -> Self {
        Current {
            seq: 0,
            decoder: None,
            expect_continue: false,
            http10: false,
            request_keep_alive: false,
            is_upgrade: false,
            is_connect: false,
            upgrade_request: false,
            responded: false,
            head_pending: false,
            response_done: true,
            body_done: true,
            reading_body: false,
            continue_sent: false,
            peer_closed: false,
            head_negotiated: false,
            is_switch: false,
            keep_alive: false,
            watch_wanted: false,
        }
    }
}

struct Inner {
    codec: Py<H1Codec>,
    /// The caller-supplied transport; `None` once closed or handed off as a tunnel.
    transport: Option<Py<PyAny>>,
    closed: bool,
    /// keep-alive: may we read another request after this one?
    reusable: bool,
    /// Handed off as a raw tunnel (101 / CONNECT / detach): the transport belongs
    /// to the caller — never close it.
    upgraded: bool,
    /// hyper `http1::Builder::half_close`: a client FIN mid-request is not a disconnect.
    half_close: bool,
    /// hyper `http1::Builder::keep_alive(false)` -> `KA::Disabled` from the start.
    keep_alive_enabled: bool,
    /// `graceful_shutdown()` was called (hyper `disable_keep_alive`).
    shutdown_requested: bool,
    /// True only while parked in the idle between-requests read.
    idle_read_parked: bool,
    watcher: Option<Watcher>,
    current: Current,
    next_seq: u64,
}

impl Inner {
    fn is_current(&self, seq: u64) -> bool {
        self.current.seq == seq && seq != 0
    }

    /// hyper conn.rs `mid_message_detect_eof`'s precondition, re-expressed lazily
    /// (see `ServerConnection._arm_watcher`): park ONE read for the mid-message
    /// window of the current request, if the observation can be consumed
    /// (`watch_wanted`), no read is parked, the body is consumed, the response is
    /// not done, and — runtime-forced — the request does not announce an upgrade
    /// still able to be detached or switched. Skipped when hyper would not read
    /// either: `half_close`, or bytes already buffered (its `read_buf` is non-empty
    /// -> Pending). Armed: `done` is the watcher's and `None` comes back — the caller
    /// must spawn the watcher. Not armed: `done` comes back, to be dropped once the
    /// guard is released (no `Py` drop under the lock).
    fn arm_decision(&mut self, py: Python<'_>, done: Py<PyAny>) -> Option<Py<PyAny>> {
        let c = &self.current;
        if !c.watch_wanted {
            return Some(done);
        }
        if self.half_close || self.closed || self.transport.is_none() || self.watcher.is_some() {
            return Some(done);
        }
        if !c.body_done || c.response_done {
            return Some(done);
        }
        if c.upgrade_request && (!c.head_negotiated || c.is_switch) {
            return Some(done);
        }
        // state → codec: a probe already read the next request (hyper's `read_buf`).
        if self.codec.get().buffered() > 0 {
            return Some(done);
        }
        // state → decoder: the next request is already here. Back into the codec's
        // buffer, where hyper would have kept it all along.
        if let Some(dec) = &c.decoder
            && dec.get().buffered() > 0
        {
            let leftover = dec.get().take_buffered(py);
            self.codec.get().feed(leftover.as_bytes(py));
            return Some(done);
        }
        self.watcher = Some(Watcher {
            handle: None,
            done,
            data: None,
            error: None,
            completed: false,
        });
        None
    }

    /// Positioned at the next head: how to read it (hyper `poll_read_head` over its
    /// persistent `read_buf`, then the idle read under `can_read_head`). One step
    /// with the idle park flag: a `graceful_shutdown()` landing after this either saw
    /// the park (and wakes THIS read) or is seen by it (`READ_SHUTDOWN`).
    fn read_verdict(&mut self, py: Python<'_>) -> (u8, Option<Py<PyAny>>) {
        // state → codec: bytes of the next request already here (pipelined, or a
        // partial head) — parse before touching the transport; a mid-head read is
        // never interruptible, so no park.
        if self.codec.get().buffered() > 0 {
            return (READ_PARSE, None);
        }
        if self.shutdown_requested {
            return (READ_SHUTDOWN, None);
        }
        if let Some(w) = &self.watcher {
            // The parked watcher's read IS the idle read (the hand-off: never a second
            // reader on the transport).
            self.idle_read_parked = true;
            return (READ_WATCHER, Some(w.done.clone_ref(py)));
        }
        match &self.transport {
            Some(t) => {
                self.idle_read_parked = true;
                (READ_TRANSPORT, Some(t.clone_ref(py)))
            }
            None => (READ_EOF, None), // a concurrent close: the connection is over
        }
    }

    /// A request head was parsed: it becomes the current request. Returns its `seq`
    /// and the previous request's decoder (dropped by the caller, after the guard).
    fn begin_request(
        &mut self,
        decoder: Py<H1BodyDecoder>,
        head: &RequestHead,
        body_complete: bool,
    ) -> (u64, Option<Py<H1BodyDecoder>>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let is_connect = head.method == "CONNECT";
        let old = std::mem::replace(
            &mut self.current,
            Current {
                seq,
                decoder: Some(decoder),
                expect_continue: head.expect_continue,
                http10: head.http10,
                request_keep_alive: head.keep_alive,
                is_upgrade: head.is_upgrade,
                is_connect,
                upgrade_request: head.is_upgrade || is_connect,
                responded: false,
                head_pending: false,
                response_done: false,
                body_done: body_complete,
                reading_body: false,
                continue_sent: false,
                peer_closed: false,
                head_negotiated: false,
                is_switch: false,
                keep_alive: false,
                watch_wanted: false,
            },
        );
        (seq, old.decoder)
    }

    /// `respond_head` under the guard; the second value is `done` handed back when
    /// it was not stored (dropped by the caller after the guard).
    #[allow(clippy::too_many_arguments)]
    fn respond_head_locked(
        &mut self,
        py: Python<'_>,
        seq: u64,
        status: u16,
        headers: Option<&HeaderMap>,
        done: Py<PyAny>,
        content_length: Option<u64>,
        chunked: bool,
        want: bool,
    ) -> (PyResult<(u8, Option<Py<PyBytes>>, bool)>, Option<Py<PyAny>>) {
        if !self.is_current(seq) {
            return (Ok((REQ_STALE, None, false)), Some(done));
        }
        if self.current.head_pending {
            self.current.head_pending = false; // the claimant, back from the continue wait
        } else {
            if self.current.responded {
                return (Ok((REQ_ALREADY, None, false)), Some(done));
            }
            self.current.responded = true;
            if self.current.continue_sent {
                self.current.head_pending = true;
                return (Ok((REQ_CONTINUE, None, false)), Some(done));
            }
        }
        if self.current.peer_closed {
            return (Ok((REQ_PEER_CLOSED, None, false)), Some(done));
        }
        // hyper `wants_keep_alive()`: the request was keep-alive and neither a
        // graceful shutdown nor `keep_alive(false)` turned it off.
        let wants_keep_alive =
            self.current.request_keep_alive && self.keep_alive_enabled && !self.shutdown_requested;
        // The hand-off decision (hyper `on_upgrade`): a 101 the request asked for, or a
        // 2xx to CONNECT. A 101 the request did not ask for is not a tunnel (F28) — it
        // still ends the connection (`is_last`).
        let is_switch = (status == 101 && self.current.is_upgrade)
            || (self.current.is_connect && (200..300).contains(&status));
        // state → codec: encode (hyper `enforce_version` + `Server::encode`) and read
        // the encoder's verdicts back in the same step.
        let codec = self.codec.get();
        let head = match codec.serialize_response(
            py,
            status,
            headers,
            wants_keep_alive,
            self.current.http10,
            content_length,
            chunked,
        ) {
            Ok(head) => head,
            Err(e) => return (Err(e), Some(done)),
        };
        let keep_alive = !codec.response_is_last() && !codec.response_close_delimited();
        self.current.is_switch = is_switch;
        self.current.keep_alive = keep_alive;
        self.current.head_negotiated = true;
        if want {
            self.current.watch_wanted = true;
        }
        let unused = self.arm_decision(py, done);
        let armed = unused.is_none();
        (Ok((REQ_OK, Some(head), armed)), unused)
    }

    /// Any pipelined bytes the body decoder read past this request's body go back
    /// into the codec's persistent buffer (hyper keeps them in its single `read_buf`).
    fn feed_decoder_leftover(&mut self, py: Python<'_>) {
        if let Some(dec) = &self.current.decoder
            && dec.get().buffered() > 0
        {
            let leftover = dec.get().take_buffered(py);
            self.codec.get().feed(leftover.as_bytes(py));
        }
    }

    /// Positioned at the next head: leftover fed back, the codec's per-message
    /// state reset (its read buffer persists), the request forgotten.
    fn ready_for_next(&mut self, py: Python<'_>) -> Option<Py<H1BodyDecoder>> {
        self.feed_decoder_leftover(py);
        self.codec.get().reset();
        let old = self.current.decoder.take();
        self.current.response_done = true;
        self.current.body_done = true;
        old
    }

    /// Poison + close: hyper `Writing::Closed` / `close_read()` with the error stored
    /// on the connection -> the transport is dropped. Returns the transport to close
    /// (`None` if already gone or handed off).
    fn close_now(&mut self) -> Option<Py<PyAny>> {
        self.closed = true;
        self.reusable = false;
        if self.upgraded {
            return None;
        }
        self.transport.take()
    }
}

/// The connection + current-request state of one HTTP/1 server connection,
/// `frozen` + `subclass`: `ServerConnection` subclasses it and adds only async
/// machinery.
#[pyclass(module = "httpunk._httpunk", name = "H1ServerState", frozen, subclass)]
pub struct H1ServerState {
    inner: Mutex<Inner>,
}

impl H1ServerState {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }
}

#[pymethods]
impl H1ServerState {
    #[new]
    #[pyo3(signature = (codec, transport, *, keep_alive=true, half_close=false))]
    fn new(codec: Py<H1Codec>, transport: Py<PyAny>, keep_alive: bool, half_close: bool) -> Self {
        H1ServerState {
            inner: Mutex::new(Inner {
                codec,
                transport: Some(transport),
                closed: false,
                reusable: true,
                upgraded: false,
                half_close,
                keep_alive_enabled: keep_alive,
                shutdown_requested: false,
                idle_read_parked: false,
                watcher: None,
                current: Current::none(),
                next_seq: 1,
            }),
        }
    }

    // ===== GC visibility (design §4) =====

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Ok(me) = self.inner.try_lock() {
            visit.call(&me.codec)?;
            if let Some(t) = &me.transport {
                visit.call(t)?;
            }
            if let Some(w) = &me.watcher {
                if let Some(h) = &w.handle {
                    visit.call(h)?;
                }
                visit.call(&w.done)?;
                if let Some(e) = &w.error {
                    visit.call(e)?;
                }
            }
            if let Some(d) = &me.current.decoder {
                visit.call(d)?;
            }
        }
        Ok(())
    }

    fn __clear__(&self) {
        let taken = {
            let mut me = self.lock();
            (
                me.transport.take(),
                me.watcher.take(),
                me.current.decoder.take(),
            )
        };
        drop(taken);
    }

    // ===== connection-level queries =====

    /// The codec (one per connection, hyper's `Conn`): the driver keeps it for the
    /// single-owner byte work (head parse / encode, body framing).
    #[getter]
    fn codec(&self, py: Python<'_>) -> Py<H1Codec> {
        self.lock().codec.clone_ref(py)
    }

    /// The transport for a read or write, `None` once closed or handed off (the
    /// caller raises `ConnectionClosedError`).
    fn transport_ref(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().transport.as_ref().map(|t| t.clone_ref(py))
    }

    #[getter]
    fn closed(&self) -> bool {
        self.lock().closed
    }

    #[getter]
    fn reusable(&self) -> bool {
        self.lock().reusable
    }

    #[getter]
    fn upgraded(&self) -> bool {
        self.lock().upgraded
    }

    #[getter]
    fn half_close(&self) -> bool {
        self.lock().half_close
    }

    #[getter]
    fn keep_alive_enabled(&self) -> bool {
        self.lock().keep_alive_enabled
    }

    #[getter]
    fn shutdown_requested(&self) -> bool {
        self.lock().shutdown_requested
    }

    /// A watcher slot exists (a read is parked, or completed and not yet consumed).
    #[getter]
    fn has_watcher(&self) -> bool {
        self.lock().watcher.is_some()
    }

    /// A mid-message read is parked on the transport right now.
    #[getter]
    fn watcher_parked(&self) -> bool {
        self.lock().watcher.as_ref().is_some_and(|w| !w.completed)
    }

    #[getter]
    fn current_seq(&self) -> u64 {
        self.lock().current.seq
    }

    fn current_decoder(&self, py: Python<'_>) -> Option<Py<H1BodyDecoder>> {
        self.lock()
            .current
            .decoder
            .as_ref()
            .map(|d| d.clone_ref(py))
    }

    // ===== the accept loop (hyper dispatch.rs poll_loop / poll_read_head) =====

    /// `next_request` entry: where are we? `(code, transport)` — `NEXT_NONE` (serves
    /// no more), `NEXT_RAISE` (the current request is unanswered — hyper reads the
    /// next head only once the response is fully written), `NEXT_CLOSE` (an unread
    /// body with a reader parked on the transport: not drainable — closed here, the
    /// transport to close is returned), `NEXT_DRAIN` (an unread body: the drain now
    /// owns the transport read — run the one-poll drain, then `drain_done`),
    /// `NEXT_READY`.
    fn begin_read(&self, py: Python<'_>) -> (u8, Option<Py<PyAny>>) {
        let mut me = self.lock();
        if me.closed || !me.reusable {
            return (NEXT_NONE, None);
        }
        if me.current.seq != 0 {
            if !me.current.response_done {
                return (NEXT_RAISE, None);
            }
            if !me.current.body_done {
                if me.current.reading_body {
                    let t = me.close_now();
                    return (NEXT_CLOSE, t);
                }
                me.current.reading_body = true; // the drain's read: no body reader may start
                return (NEXT_DRAIN, None);
            }
        }
        let old = me.ready_for_next(py);
        let verdict = me.read_verdict(py);
        drop(me);
        drop(old);
        verdict
    }

    /// The one-poll drain's outcome (hyper `poll_drain_or_close_read`): `complete` =
    /// the body reached its end -> positioned at the next head, the read verdict (as
    /// `begin_read`); else `close_read()` -> `NEXT_CLOSE` with the transport to close.
    fn drain_done(&self, py: Python<'_>, complete: bool) -> (u8, Option<Py<PyAny>>) {
        let mut me = self.lock();
        me.current.reading_body = false;
        if !complete {
            let t = me.close_now();
            return (NEXT_CLOSE, t);
        }
        me.current.body_done = true;
        let old = me.ready_for_next(py);
        let verdict = me.read_verdict(py);
        drop(me);
        drop(old);
        verdict
    }

    /// The idle read (`READ_WATCHER` / `READ_TRANSPORT`) returned: a read that
    /// completes is no longer interruptible. Right after the read, before the parse —
    /// a head that keeps arriving over further reads must not be woken by a shutdown.
    fn unpark_idle_read(&self) {
        self.lock().idle_read_parked = false;
    }

    /// Feed `data` to the head parser (hyper `Conn::read_head`: parse, remember the
    /// method for the response, split the body bytes off) and, once a head is
    /// complete, make it the current request in the same step: its body decoder is
    /// built and fed the bytes read alongside the head. Returns `(head, seq,
    /// decoder)`, or `None` while the head is incomplete (read more). A parse
    /// failure raises `H1ParseError` (the codec remembers the automatic status).
    fn accept_head(
        &self,
        py: Python<'_>,
        data: &[u8],
    ) -> PyResult<Option<(Py<PyAny>, u64, Py<H1BodyDecoder>)>> {
        let mut me = self.lock();
        // state → codec.
        let Some(head) = me.codec.get().receive_request_head(py, data)? else {
            return Ok(None);
        };
        let h = head
            .bind(py)
            .cast::<RequestHead>()
            .expect("receive_request_head yields an H1RequestHead")
            .get();
        let decoder = Py::new(
            py,
            H1BodyDecoder::new(&h.body_kind, h.content_length.unwrap_or(0)),
        )?;
        // state → decoder: the body bytes that came with the head.
        let body = me.codec.get().take_body_raw();
        if !body.is_empty() {
            decoder.get().feed(&body);
        }
        let complete = decoder.get().is_complete();
        let (seq, old) = me.begin_request(decoder.clone_ref(py), h, complete);
        drop(me);
        drop(old);
        Ok(Some((head, seq, decoder)))
    }

    /// A head parse failed / the head-read deadline hit / the transport broke at
    /// the request boundary: closed; returns the transport for the automatic error
    /// response (best effort) and the close.
    fn fail_read(&self) -> Option<Py<PyAny>> {
        self.lock().close_now()
    }

    /// A clean EOF between requests (or a graceful shutdown released the idle read):
    /// no more requests; the transport stays for `close()` to close.
    fn stop_serving(&self) {
        let mut me = self.lock();
        me.closed = true;
        me.reusable = false;
    }

    /// `close()`: closed + non-reusable; returns the transport to close (`None` if
    /// handed off) and the watcher's done event to wait on before joining it.
    fn mark_closed(&self, py: Python<'_>) -> (Option<Py<PyAny>>, Option<Py<PyAny>>) {
        let mut me = self.lock();
        let transport = me.close_now();
        let done = me.watcher.as_ref().map(|w| w.done.clone_ref(py));
        (transport, done)
    }

    /// The body read broke: no further request may be served (hyper `Reading::Closed`).
    fn mark_unusable(&self) {
        self.lock().reusable = false;
    }

    /// `graceful_shutdown()` (hyper `disable_keep_alive`): stop reusing the
    /// connection. Returns the transport to `interrupt_read` on when a read is parked
    /// idly between requests and the backend can wake it (asyncio); the caller also
    /// sets the shutdown event for the select-based race (tonio).
    fn request_shutdown(&self, py: Python<'_>, native_read_interrupt: bool) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        me.reusable = false;
        me.shutdown_requested = true;
        if native_read_interrupt && me.idle_read_parked {
            return me.transport.as_ref().map(|t| t.clone_ref(py));
        }
        None
    }

    // ===== the current request =====

    /// `100 Continue`: claimed by the body reader only while `Writing::Init` (no
    /// response begun) and only for HTTP/1.1+ (hyper conn.rs L409-415, L311), once.
    /// Decided under the same lock as `try_respond`, so a head can never overtake it
    /// (the responder awaits the continue's write when told to).
    fn try_send_continue(&self, seq: u64) -> bool {
        let mut me = self.lock();
        if !me.is_current(seq) {
            return false;
        }
        let c = &mut me.current;
        if !c.expect_continue || c.continue_sent || c.responded || c.http10 {
            return false;
        }
        c.continue_sent = true;
        true
    }

    /// The body reader is about to pull from the transport: it becomes the one reader
    /// (`False`: stale, the body already ended, or the next request's drain owns the
    /// read — the body is no longer readable).
    fn begin_body_read(&self, seq: u64) -> bool {
        let mut me = self.lock();
        if !me.is_current(seq) || me.current.body_done || me.current.reading_body {
            return false;
        }
        me.current.reading_body = true;
        true
    }

    /// The body reader's pull returned (`complete`: the body reached its end — the
    /// mid-message window of a `peer_closed()` asked for earlier may now be armed:
    /// the caller re-runs the arm).
    fn end_body_read(&self, seq: u64, complete: bool) {
        let mut me = self.lock();
        if !me.is_current(seq) {
            return;
        }
        me.current.reading_body = false;
        if complete {
            me.current.body_done = true;
        }
    }

    /// `peer_closed()`: `Some(flag)` when the window is already over (or the request
    /// is stale — a stale request always completed without a FIN, see §3.2);
    /// `None` = still open: arm the watcher and wait.
    fn peer_closed_now(&self, seq: u64) -> Option<bool> {
        let me = self.lock();
        if !me.is_current(seq) {
            return Some(false);
        }
        if me.current.response_done {
            return Some(me.current.peer_closed);
        }
        None
    }

    /// The client closed its side mid-message (as the watcher recorded it).
    fn peer_closed_flag(&self, seq: u64) -> bool {
        let me = self.lock();
        me.is_current(seq) && me.current.peer_closed
    }

    /// The whole arm decision (see `Inner::arm_decision`) with the want/negotiated
    /// handshake: `want` records the ask (`peer_closed()`, a streamed or push
    /// response); `head_negotiated` = `is_switch` is decided. `done`: the Python
    /// event the watcher will set. Returns whether to spawn the watcher; the caller
    /// then spawns it and `store_watcher_handle`s the handle.
    #[pyo3(signature = (seq, done, *, want=true, head_negotiated=false))]
    fn arm_watcher(
        &self,
        py: Python<'_>,
        seq: u64,
        done: Py<PyAny>,
        want: bool,
        head_negotiated: bool,
    ) -> bool {
        let mut me = self.lock();
        if !me.is_current(seq) {
            drop(me);
            drop(done);
            return false;
        }
        if head_negotiated {
            me.current.head_negotiated = true;
        }
        if want {
            me.current.watch_wanted = true;
        }
        let unused = me.arm_decision(py, done);
        drop(me);
        let armed = unused.is_none();
        drop(unused);
        armed
    }

    /// Store the spawned watcher's handle. Refused — the handle is returned — when
    /// the connection was closed meanwhile (`close()` can no longer join it: the
    /// arm path joins it itself, which returns at once since the transport is gone).
    fn store_watcher_handle(&self, handle: Py<PyAny>) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        if me.closed {
            drop(me);
            return Some(handle);
        }
        match me.watcher.as_mut() {
            Some(w) if w.handle.is_none() => {
                w.handle = Some(handle);
                None
            }
            _ => {
                drop(me);
                Some(handle)
            }
        }
    }

    /// The watcher's read completed (hyper conn.rs L491-508): `data` = its bytes
    /// (`b""` = EOF), `None` with `error` = a transport error, both `None` = it found
    /// no transport. An EOF / error while the response is still in flight means the
    /// client is gone: non-reusable, the request `peer_closed` -> returns `True` so
    /// the caller resolves the request's `peer_closed()`.
    #[pyo3(signature = (data=None, error=None))]
    fn watcher_completed(
        &self,
        py: Python<'_>,
        data: Option<Py<PyBytes>>,
        error: Option<Py<PyAny>>,
    ) -> bool {
        let mut me = self.lock();
        let gone = data
            .as_ref()
            .is_none_or(|d| d.bind(py).len().unwrap_or(0) == 0);
        if let Some(w) = me.watcher.as_mut() {
            w.data = data;
            w.error = error;
            w.completed = true;
        } else {
            drop(me);
            drop((data, error));
            return false;
        }
        if gone && !me.current.response_done && !me.current.peer_closed {
            me.reusable = false;
            me.current.peer_closed = true;
            return true;
        }
        false
    }

    /// The watcher found no transport (a racing close): its slot completes with
    /// nothing to report.
    fn watcher_aborted(&self) {
        if let Some(w) = self.lock().watcher.as_mut() {
            w.completed = true;
        }
    }

    /// The watcher's done event, if a slot exists (the hand-off read awaits it).
    fn watcher_done(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().watcher.as_ref().map(|w| w.done.clone_ref(py))
    }

    /// Take the watcher's handle, exactly once, to join it.
    fn take_watcher_handle(&self) -> Option<Py<PyAny>> {
        self.lock().watcher.as_mut().and_then(|w| w.handle.take())
    }

    /// Consume the completed watcher's result `(data, error)` and free the slot (a
    /// still-unjoined handle stays for `close()`).
    fn take_watcher_result(&self) -> (Option<Py<PyBytes>>, Option<Py<PyAny>>) {
        let mut me = self.lock();
        let Some(w) = me.watcher.as_mut() else {
            return (None, None);
        };
        let out = (w.data.take(), w.error.take());
        if w.handle.is_none() {
            me.watcher = None;
        }
        out
    }

    /// The response is complete or failed: the mid-message window is over (the
    /// caller then sets the request's `peer_closed_evt`). `True` if it flipped.
    fn close_window(&self, seq: u64) -> bool {
        let mut me = self.lock();
        if !me.is_current(seq) || me.current.response_done {
            return false;
        }
        me.current.response_done = true;
        true
    }

    /// `respond()` / `send_response()`: the whole head step of hyper's
    /// `Conn::encode_head` (conn.rs) as ONE transition — claim the response (once),
    /// read the verdicts the claim must see, encode the head, record the head-time
    /// decisions and run the arm decision. Returns `(code, head, armed)`:
    ///
    /// - `REQ_STALE` / `REQ_ALREADY`: not the current request / already claimed;
    /// - `REQ_PEER_CLOSED`: the watcher saw the client's EOF mid-request
    ///   (`mid_message_detect_eof` -> `IncompleteMessage`): nothing is written;
    /// - `REQ_CONTINUE`: claimed, but a `100 Continue` claimed by the body reader is
    ///   being written — the caller awaits it (the request's continue event) and calls
    ///   again; the claim stands (`head_pending`) and the peer-closed verdict is
    ///   re-read then;
    /// - `REQ_OK`: `head` is the encoded head (hyper `enforce_version` +
    ///   `Server::encode` with `wants_keep_alive()` = request keep-alive, keep-alive
    ///   enabled, no shutdown), the reuse decision (`!is_last && !close_delimited`) and
    ///   the hand-off decision (a 101 the request asked for, or a 2xx to CONNECT) are
    ///   recorded, and `armed` says whether the caller must spawn the watcher for
    ///   `done` (`want`: a streamed / push body asks for the read).
    ///
    /// An encoder `User` error (a 1xx status, content-length + transfer-encoding)
    /// raises after the claim, as hyper's `encode_head` fails after `Writing::Init`.
    #[pyo3(signature = (seq, status, headers, done, *, content_length=None, chunked=false, want=false))]
    #[allow(clippy::too_many_arguments)] // hyper's Encode inputs + the arm ask
    fn respond_head(
        &self,
        py: Python<'_>,
        seq: u64,
        status: u16,
        headers: Option<&HeaderMap>,
        done: Py<PyAny>,
        content_length: Option<u64>,
        chunked: bool,
        want: bool,
    ) -> PyResult<(u8, Option<Py<PyBytes>>, bool)> {
        let mut me = self.lock();
        let (result, unused) = me.respond_head_locked(
            py,
            seq,
            status,
            headers,
            done,
            content_length,
            chunked,
            want,
        );
        drop(me);
        drop(unused); // the event, when the watcher was not armed (or nothing ran)
        result
    }

    /// The response is fully on the wire (hyper `try_keep_alive` after
    /// `Writing::KeepAlive`): apply the head-time decision. Returns
    /// `(switch, close, transport, leftover)`: a protocol switch hands the transport
    /// (plus the bytes already buffered past the head) to the caller's tunnel and
    /// detaches; otherwise `reusable &&= keep_alive` (a revoked connection never
    /// resurrects) and a non-reusable one is closed (`close`, take the transport).
    fn finish_response(
        &self,
        py: Python<'_>,
        seq: u64,
    ) -> (bool, bool, Option<Py<PyAny>>, Option<Py<PyBytes>>) {
        let mut me = self.lock();
        if !me.is_current(seq) {
            return (false, false, None, None);
        }
        me.current.response_done = true;
        if me.current.is_switch {
            me.feed_decoder_leftover(py);
            let leftover = me.codec.get().take_body(py);
            me.upgraded = true;
            me.closed = true;
            me.reusable = false;
            let transport = me.transport.take();
            return (true, false, transport, Some(leftover));
        }
        me.reusable = me.reusable && me.current.keep_alive;
        if me.reusable {
            return (false, false, None, None);
        }
        let transport = me.close_now();
        (false, true, transport, None)
    }

    /// A failed response poisons the connection (hyper `Writing::Closed`): the window
    /// closes, non-reusable, closed; returns the transport to close.
    fn fail_response(&self, seq: u64) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        if me.is_current(seq) {
            me.current.response_done = true;
        }
        me.close_now()
    }

    /// `detach()`: take over the raw connection without a response. Refused with
    /// `REQ_ALREADY` (responded), `REQ_STALE`, or `REQ_PARKED` (a mid-message read is
    /// parked: it can neither be handed to the caller — one reader per transport — nor
    /// cancelled without risking bytes). Otherwise the request is marked answered, the
    /// window closes, the bytes buffered past the head (plus any the watcher already
    /// read) are gathered as the tunnel's leftover, and the transport is handed off:
    /// `(code, transport, leftover)`.
    fn detach(&self, py: Python<'_>, seq: u64) -> (u8, Option<Py<PyAny>>, Option<Py<PyBytes>>) {
        let mut me = self.lock();
        if !me.is_current(seq) {
            return (REQ_STALE, None, None);
        }
        if me.current.responded {
            return (REQ_ALREADY, None, None);
        }
        if me.watcher.as_ref().is_some_and(|w| !w.completed) {
            return (REQ_PARKED, None, None);
        }
        me.current.responded = true;
        me.current.response_done = true;
        me.feed_decoder_leftover(py);
        if let Some(w) = me.watcher.as_mut()
            && let Some(data) = w.data.take()
        {
            me.codec.get().feed(data.as_bytes(py));
        }
        let leftover = me.codec.get().take_body(py);
        me.upgraded = true;
        me.closed = true;
        me.reusable = false;
        let transport = me.transport.take();
        (REQ_OK, transport, Some(leftover))
    }

    fn __repr__(&self) -> String {
        let me = self.lock();
        format!(
            "H1ServerState(seq={}, closed={}, reusable={}, responded={}, response_done={}, watcher={})",
            me.current.seq,
            me.closed,
            me.reusable,
            me.current.responded,
            me.current.response_done,
            me.watcher.is_some(),
        )
    }
}

// ===========================================================================
// The client connection (design §3.3) — hyper's client `Conn` + `Dispatcher`
// state: the single in-flight slot (`Conn::is_busy`), the idle watcher (the
// re-expression of hyper's always-polled `Connection` future), the background
// request-body writer, the error slot.
// ===========================================================================

/// `watcher_completed()` verdicts.
pub const WATCH_HANDOFF: u8 = 0; // the exchange's first read: stored for `_read_head`
pub const WATCH_IDLE_EOF: u8 = 1; // hyper's clean idle close: closed, the transport returned
pub const WATCH_IDLE_BYTES: u8 = 2; // bytes on an idle connection: poison (`new_unexpected_message`)
pub const WATCH_IDLE_ERROR: u8 = 3; // a transport error while idle: fail
pub const WATCH_IGNORED: u8 = 4; // a racing close already committed the flags

pub const CLIENT_CONSTANTS: [(&str, u8); 5] = [
    ("H1_WATCH_HANDOFF", WATCH_HANDOFF),
    ("H1_WATCH_IDLE_EOF", WATCH_IDLE_EOF),
    ("H1_WATCH_IDLE_BYTES", WATCH_IDLE_BYTES),
    ("H1_WATCH_IDLE_ERROR", WATCH_IDLE_ERROR),
    ("H1_WATCH_IGNORED", WATCH_IGNORED),
];

struct ClientInner {
    transport: Option<Py<PyAny>>,
    closed: bool,
    upgraded: bool,
    /// The stored failure (a traceback-free copy), first writer wins.
    error: Option<Py<PyAny>>,
    /// The single in-flight exchange holds the connection (hyper `Conn::is_busy`).
    busy: bool,
    /// An exchange has started: the watcher's completing read is the response's
    /// first bytes, handed to `_read_head` (else it enforces the idle rules).
    exchange_active: bool,
    /// The last response was HTTP/1.0: later requests downgrade (hyper `state.version`).
    peer_http10: bool,
    /// The in-flight request's background body writer: its detached scope.
    writer: Option<Py<PyAny>>,
    /// The body was sent in FULL (the writer finished): the connection may be reused.
    writer_finished: bool,
    watcher: Option<Watcher>,
}

impl ClientInner {
    fn close_now(&mut self) -> Option<Py<PyAny>> {
        self.closed = true;
        if self.upgraded {
            return None;
        }
        self.transport.take()
    }
}

/// The connection state of one HTTP/1 client connection, `frozen` + `subclass`:
/// `Connection` (httpunk/h1/client.py) subclasses it and adds only async machinery.
#[pyclass(module = "httpunk._httpunk", name = "H1ClientState", frozen, subclass)]
pub struct H1ClientState {
    inner: Mutex<ClientInner>,
}

impl H1ClientState {
    fn lock(&self) -> std::sync::MutexGuard<'_, ClientInner> {
        self.inner.lock().unwrap()
    }
}

#[pymethods]
impl H1ClientState {
    #[new]
    fn new(transport: Py<PyAny>) -> Self {
        H1ClientState {
            inner: Mutex::new(ClientInner {
                transport: Some(transport),
                closed: false,
                upgraded: false,
                error: None,
                busy: false,
                exchange_active: false,
                peer_http10: false,
                writer: None,
                writer_finished: false,
                watcher: None,
            }),
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Ok(me) = self.inner.try_lock() {
            if let Some(t) = &me.transport {
                visit.call(t)?;
            }
            if let Some(e) = &me.error {
                visit.call(e)?;
            }
            if let Some(w) = &me.writer {
                visit.call(w)?;
            }
            if let Some(w) = &me.watcher {
                if let Some(h) = &w.handle {
                    visit.call(h)?;
                }
                visit.call(&w.done)?;
                if let Some(e) = &w.error {
                    visit.call(e)?;
                }
            }
        }
        Ok(())
    }

    fn __clear__(&self) {
        let taken = {
            let mut me = self.lock();
            (
                me.transport.take(),
                me.error.take(),
                me.writer.take(),
                me.watcher.take(),
            )
        };
        drop(taken);
    }

    // ----- queries -----

    fn transport_ref(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().transport.as_ref().map(|t| t.clone_ref(py))
    }

    #[getter]
    fn closed(&self) -> bool {
        self.lock().closed
    }

    #[getter]
    fn busy(&self) -> bool {
        self.lock().busy
    }

    #[getter]
    fn upgraded(&self) -> bool {
        self.lock().upgraded
    }

    #[getter]
    fn peer_http10(&self) -> bool {
        self.lock().peer_http10
    }

    fn set_peer_http10(&self, value: bool) {
        self.lock().peer_http10 = value;
    }

    /// The stored error (a `clone_ref` of the stored copy); the caller raises
    /// `fresh_exc(err) from err`.
    #[getter]
    fn error(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().error.as_ref().map(|e| e.clone_ref(py))
    }

    /// The connection can serve no more requests (closed or failed).
    fn is_dead(&self) -> bool {
        let me = self.lock();
        me.closed || me.error.is_some()
    }

    #[getter]
    fn has_watcher(&self) -> bool {
        self.lock().watcher.is_some()
    }

    #[getter]
    fn writer_finished(&self) -> bool {
        self.lock().writer_finished
    }

    // ----- the single in-flight slot (hyper Conn::is_busy) -----

    /// Claim the connection for an exchange (`False`: busy — wait on the idle event
    /// with the waiter idiom and retry).
    fn try_begin_exchange(&self) -> bool {
        let mut me = self.lock();
        if me.busy {
            return false;
        }
        me.busy = true;
        me.exchange_active = false;
        me.writer_finished = false;
        true
    }

    /// Release the slot, exactly once (`True` if it flipped: wake the idle waiters).
    fn end_exchange(&self) -> bool {
        let mut me = self.lock();
        if !me.busy {
            return false;
        }
        me.busy = false;
        me.exchange_active = false;
        true
    }

    /// The request is about to be written: from here the watcher's completing read
    /// belongs to the exchange (hyper's one poll loop owns the read across the
    /// idle->busy transition).
    fn exchange_started(&self) {
        self.lock().exchange_active = true;
    }

    // ----- failure / close -----

    /// Poison + close (hyper `state.close()` with the error stored): `exc` is a
    /// traceback-free copy, stored only if no error is stored yet (`None`: close
    /// without recording, hyper's Io-less close). Returns the transport to close.
    #[pyo3(signature = (exc=None))]
    fn fail(&self, exc: Option<Py<PyAny>>) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        let unused = match exc {
            Some(e) if me.error.is_none() => {
                me.error = Some(e);
                None
            }
            other => other,
        };
        let transport = me.close_now();
        drop(me);
        drop(unused);
        transport
    }

    /// Close without an error (a non-reusable exchange completed, an idle EOF):
    /// returns the transport to close.
    fn close_now(&self) -> Option<Py<PyAny>> {
        self.lock().close_now()
    }

    /// `close()`: `(transport, watcher done event, writer scope)` — close the
    /// transport (what ends the parked watcher read), await the watcher, tear the
    /// writer down.
    fn mark_closed(
        &self,
        py: Python<'_>,
    ) -> (Option<Py<PyAny>>, Option<Py<PyAny>>, Option<Py<PyAny>>) {
        let mut me = self.lock();
        let transport = me.close_now();
        let done = me.watcher.as_ref().map(|w| w.done.clone_ref(py));
        let writer = me.writer.take();
        (transport, done, writer)
    }

    /// A 101 / 2xx-to-CONNECT: the connection stops being HTTP. Marks the transport
    /// handed off (never closed by this driver again) and returns it, in one step.
    fn upgrade(&self) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        me.upgraded = true;
        me.closed = true;
        me.transport.take()
    }

    // ----- the background body writer -----

    /// Store the in-flight writer's scope. Refused — returned — when the connection
    /// closed meanwhile: the exchange tears it down itself.
    fn store_writer(&self, scope: Py<PyAny>) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        if me.closed {
            drop(me);
            return Some(scope);
        }
        me.writer = Some(scope);
        None
    }

    /// Take the writer's scope to tear it down (exactly one owner: the exchange's
    /// own teardown or `close()`).
    fn take_writer(&self) -> Option<Py<PyAny>> {
        self.lock().writer.take()
    }

    /// The writer sent the body in full.
    fn writer_done(&self) {
        self.lock().writer_finished = true;
    }

    // ----- the idle watcher -----

    /// Arm the idle watcher for one idle period (`False`: the connection can serve
    /// no more, or one is already armed).
    fn arm_watcher(&self, done: Py<PyAny>) -> bool {
        let mut me = self.lock();
        if me.closed || me.error.is_some() || me.transport.is_none() || me.watcher.is_some() {
            drop(me);
            drop(done);
            return false;
        }
        me.exchange_active = false;
        me.watcher = Some(Watcher {
            handle: None,
            done,
            data: None,
            error: None,
            completed: false,
        });
        true
    }

    /// Store the spawned watcher's handle; refused (returned) if the connection
    /// closed meanwhile — the arming task joins it itself.
    fn store_watcher_handle(&self, handle: Py<PyAny>) -> Option<Py<PyAny>> {
        let mut me = self.lock();
        if me.closed {
            drop(me);
            return Some(handle);
        }
        match me.watcher.as_mut() {
            Some(w) if w.handle.is_none() => {
                w.handle = Some(handle);
                None
            }
            _ => {
                drop(me);
                Some(handle)
            }
        }
    }

    fn watcher_aborted(&self) {
        if let Some(w) = self.lock().watcher.as_mut() {
            w.completed = true;
        }
    }

    /// The watcher's read completed: dispatched by state (hyper conn.rs
    /// L463-489 / L510-520). Returns `(verdict, transport)`: `WATCH_HANDOFF` — an
    /// exchange is active, the bytes/error are its first read; `WATCH_IDLE_EOF` —
    /// closed here, close the returned transport; `WATCH_IDLE_BYTES` — poison
    /// (`new_unexpected_message`); `WATCH_IDLE_ERROR` — fail with the error;
    /// `WATCH_IGNORED` — a racing close already committed.
    #[pyo3(signature = (data=None, error=None))]
    fn watcher_completed(
        &self,
        py: Python<'_>,
        data: Option<Py<PyBytes>>,
        error: Option<Py<PyAny>>,
    ) -> (u8, Option<Py<PyAny>>) {
        let mut me = self.lock();
        let Some(w) = me.watcher.as_mut() else {
            drop(me);
            drop((data, error));
            return (WATCH_IGNORED, None);
        };
        w.completed = true;
        if me.exchange_active {
            let w = me.watcher.as_mut().expect("slot");
            w.data = data;
            w.error = error;
            return (WATCH_HANDOFF, None);
        }
        let empty = data
            .as_ref()
            .is_none_or(|d| d.bind(py).len().unwrap_or(0) == 0);
        if error.is_some() {
            drop(error);
            drop(data);
            if me.closed {
                return (WATCH_IGNORED, None);
            }
            return (WATCH_IDLE_ERROR, None);
        }
        if empty {
            let transport = me.close_now();
            return (WATCH_IDLE_EOF, transport);
        }
        (WATCH_IDLE_BYTES, None)
    }

    fn watcher_done(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().watcher.as_ref().map(|w| w.done.clone_ref(py))
    }

    fn take_watcher_handle(&self) -> Option<Py<PyAny>> {
        self.lock().watcher.as_mut().and_then(|w| w.handle.take())
    }

    /// Consume the hand-off `(data, error)` and free the slot.
    fn take_watcher_result(&self) -> (Option<Py<PyBytes>>, Option<Py<PyAny>>) {
        let mut me = self.lock();
        let Some(w) = me.watcher.as_mut() else {
            return (None, None);
        };
        let out = (w.data.take(), w.error.take());
        if w.handle.is_none() {
            me.watcher = None;
        }
        out
    }

    fn __repr__(&self) -> String {
        let me = self.lock();
        format!(
            "H1ClientState(closed={}, failed={}, busy={}, exchange_active={}, watcher={})",
            me.closed,
            me.error.is_some(),
            me.busy,
            me.exchange_active,
            me.watcher.is_some(),
        )
    }
}

/// A one-shot latch: `try_acquire()` succeeds exactly once, from whichever task
/// (or GC finalizer) gets there first — the cross-thread CAS the body's single
/// release / single consumer and the tunnel's single close need. Never blocks.
#[pyclass(module = "httpunk._httpunk", name = "OnceLatch", frozen)]
pub struct OnceLatch {
    flag: std::sync::atomic::AtomicBool,
}

#[pymethods]
impl OnceLatch {
    #[new]
    fn new() -> Self {
        OnceLatch {
            flag: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn try_acquire(&self) -> bool {
        self.flag
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    #[getter]
    fn is_set(&self) -> bool {
        self.flag.load(std::sync::atomic::Ordering::Acquire)
    }
}
