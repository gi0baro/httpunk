"""Low-level HTTP/2 client — h2: client.rs (`SendRequest` + `handshake`) + the
client half of `proto::streams`.

Holds the client's full role stack, mirroring `server.py`: `ClientStreamManager`
(the initiating-side stream manager — allocates ids, bound by the peer's
MAX_CONCURRENT_STREAMS), `Connection` (the client protocol driver over the shared
`H2ConnectionBase`), and `H2Connection` (the public per-connection handle).

`H2Connection` is the Python analogue of hyper's `client::conn::http2`: it
collapses `http2::handshake` + the spawned `Connection` driver + `SendRequest`
into one async-context-managed object. The core method is `send_request(Request)
-> Response` (≈ `SendRequest::send_request`); `get`/`request` are thin wrappers.
This layer is low-level by design — no pool, connector, or high-level client
(those live downstream; see PLAN.md §3.3).

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15.
"""

import contextlib
import threading

from .._common import BaseClientConnection
from ..exceptions import ConnectionClosedError, H2ProtocolError, H2Reason
from ..types import Response
from .connection import PREFACE, H2ConnectionBase
from .settings import LocalSettings, Settings
from .share import H2ResponseBody
from .stream import Stream
from .streams import StreamManager


# hyper's HTTP/2 client profile (hyper `proto/h2/client.rs`): a 2 MB per-stream recv
# window and a 5 MB connection recv window (vs the 65535 protocol default), 16 KB max
# frame size, 16 KB max header-list size. We ship the hyper *stack's* tuned profile,
# not bare-h2's empty-SETTINGS defaults (AUDIT-2026-07-09 F24). The stream window /
# frame / header-list are advertised in our initial SETTINGS; the connection window is
# raised by an initial WINDOW_UPDATE(0) (SETTINGS can't carry it).
_STREAM_WINDOW = 2 * 1024 * 1024
_CONN_WINDOW = 5 * 1024 * 1024
_MAX_FRAME_SIZE = 16 * 1024
_MAX_HEADER_LIST_SIZE = 16 * 1024


class ClientStreamManager(StreamManager):
    """The initiating side: allocates stream ids, gates on the peer's
    MAX_CONCURRENT_STREAMS, and receives responses. h2 `client::Peer` + the
    send-stream count in `Counts`."""

    def __init__(self, conn):
        super().__init__(conn)
        self._next_id = 1
        # Serializes stream-id allocation + the initial HEADERS send: HTTP/2
        # requires new stream ids to be monotonically increasing on the wire,
        # and tonio runs coroutines across worker threads.
        self._new_stream_lock = conn.backend.lock()
        # MAX_CONCURRENT_STREAMS gating (h2 counts.rs): an authoritative count of
        # streams we've opened, checked against the peer's limit at each open.
        # `None` limit = unlimited (the peer's default). A count (not a
        # semaphore) so lowering the limit while streams are active never lets
        # the effective cap drift back up as those streams finish.
        self._num_open_streams = 0
        self._stream_limit = None
        self._slot_evt = conn.backend.event()
        # Guards the check-and-increment of `_num_open_streams` against `_stream_limit`
        # (worker-thread concurrency; a plain `+=`/compare would race). Held only for
        # the tiny critical section — never across an await.
        self._slot_lock = threading.Lock()

    # ===== role hooks =====

    def _ensure_not_idle(self, stream_id):
        # Client-initiated streams are odd; we never enable push (no even ids).
        # An odd id >= our next id was never opened by us.
        # h2: proto/streams/streams.rs `ensure_not_idle` (L1714).
        if stream_id % 2 == 0 or stream_id >= self._next_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"frame on idle stream {stream_id}")

    def _release_slot(self, st):
        if st.holds_slot:
            st.holds_slot = False
            with self._slot_lock:
                self._num_open_streams -= 1
            self._slot_evt.set()

    def _apply_peer_stream_limit(self):
        self._apply_stream_limit(self._peer.max_concurrent_streams)

    def _on_go_away(self, last_stream_id, exc):
        for st in list(self._streams.values()):
            if st.id > last_stream_id:
                self._abort_stream(st, exc)
        self._slot_evt.set()  # wake open/ready waiters (they re-check _goaway)

    def _on_fail(self):
        self._slot_evt.set()

    # ===== opening (h2: client.rs send_request -> streams.rs send_request) =====

    async def open_stream(self, method, url, headers, *, end_stream, is_head):
        """Gate on MAX_CONCURRENT_STREAMS, allocate a stream id, and emit HEADERS
        atomically (ids must be strictly increasing on the wire). `end_stream`
        puts END_STREAM on the HEADERS frame for a bodyless request (h2
        `send_request` with `end_of_stream`), avoiding a trailing empty DATA.

        h2: proto/streams/streams.rs `send_request` (L218); state transition =
        state.rs `send_open`.
        """
        if self._goaway is not None:
            raise self._goaway
        await self._acquire_stream_slot()  # blocks on the limit; increments the count
        if self._conn.error is not None or self._goaway is not None:
            self._release_count()
            raise self._conn.error or self._goaway
        async with self._new_stream_lock:
            stream_id = self._next_id
            self._next_id += 2
            st = Stream(
                stream_id,
                self._conn.backend,
                send_window=self._peer.initial_window_size,
                recv_window=self._recv_init,
                is_head=is_head,
            )
            st.holds_slot = True
            self._streams[stream_id] = st
            st.state.send_open(eos=end_stream)
            await self._conn.send_frame(
                self._conn.codec.serialize_request_headers(stream_id, method, url, headers, end_stream=end_stream)
            )
        # The connection may have failed / GOAWAY'd between our pre-lock check and
        # inserting the stream, so `fail_all`/`handle_go_away`'s fan-out could have
        # missed it. Re-check and abort our own stream (idempotent) so a caller
        # doesn't wait forever on a response head that will never arrive.
        if self._conn.error is not None or self._goaway is not None:
            self._abort_stream(st, self._conn.error or self._goaway)
            raise self._conn.error or self._goaway
        return st

    # ===== MAX_CONCURRENT_STREAMS (h2: proto/streams/counts.rs) =====

    def _can_open(self):
        return self._stream_limit is None or self._num_open_streams < self._stream_limit

    def _try_claim_slot(self):
        """Atomically claim a MAX_CONCURRENT_STREAMS slot, or arm the wakeup if at
        the limit. The check-and-increment must be under `_slot_lock`: the backend
        runs coroutines across worker threads, so an unguarded check+`+= 1` would
        let two opens both pass the gate and over-subscribe the peer's limit
        (and `+=` on the count would itself race). Returns True if a slot was
        claimed."""
        with self._slot_lock:
            if self._can_open():
                self._num_open_streams += 1
                return True
            self._slot_evt.clear()  # armed under the lock so a concurrent release can't be missed
            return False

    async def _acquire_stream_slot(self):
        """Block until a MAX_CONCURRENT_STREAMS slot is free, then claim it by
        incrementing the open-stream count (h2 counts.rs `inc_num_send_streams`,
        gated by `can_inc_num_send_streams`)."""
        while not self._try_claim_slot():
            # A GOAWAY / connection failure that arrives while we're parked at the limit
            # must fail this request PROMPTLY — h2's recv_go_away fails queued streams
            # (streams.rs L736) rather than leaving them to wait for a surviving stream
            # to free a slot, which may never happen (F20). `_on_go_away`/`_on_fail`
            # set `_slot_evt` to wake us for this re-check.
            if self._goaway is not None:
                raise self._goaway
            if self._conn.error is not None:
                raise self._conn.error
            await self._slot_evt.wait()

    def _release_count(self):
        # Undo an _acquire_stream_slot that didn't result in a live stream.
        with self._slot_lock:
            self._num_open_streams -= 1
        self._slot_evt.set()

    async def wait_until_ready(self):
        """Wait until a new stream can be opened, then return — the connection is
        alive (not failed / not GOAWAY'd) and a MAX_CONCURRENT_STREAMS slot is
        free. Backs `H2Connection.ready` (h2 `SendRequest::ready`).

        Non-reserving: we only observe that a slot is available and return, so a
        concurrent `open_stream` may still take it first (h2's `poll_ready` is
        likewise non-reserving — `send_request` re-applies the backpressure).
        """
        while True:
            # Check GOAWAY BEFORE the connection error: after a graceful
            # GOAWAY(NO_ERROR) the peer closes, so both `_goaway` (GoAwayError) and
            # `_conn.error` (ConnectionClosedError from EOF) end up set; the GOAWAY is
            # the meaningful, retry-relevant one, and h2's recv_eof never overwrites the
            # existing conn_error (streams.rs L879). Also keeps this consistent with
            # `open_stream`, which checks `_goaway` first (F20).
            if self._goaway is not None:
                raise self._goaway
            if self._conn.error is not None:
                raise self._conn.error
            # Non-reserving: just observe capacity (a racy read is fine — we don't
            # increment; a concurrent open re-applies the gate under the lock).
            if self._can_open():
                return
            with self._slot_lock:
                self._slot_evt.clear()
                if self._can_open():
                    return
            await self._slot_evt.wait()

    def _apply_stream_limit(self, new_max):
        """Set MAX_CONCURRENT_STREAMS. Authoritative: if the new limit is below
        the current open count, further opens block (in `_acquire_stream_slot`)
        until enough streams close — the count can't drift past the limit.

        h2: proto/streams/counts.rs — the limit is checked against the exact
        `num_send_streams` count at each open, not tracked as free permits.
        """
        with self._slot_lock:
            self._stream_limit = new_max
        self._slot_evt.set()  # wake waiters to re-check against the new limit


class Connection(H2ConnectionBase):
    """The client protocol driver. Created and driven by the public
    `client.H2Connection`."""

    def __init__(self, transport, *, authority=None, scheme="http", backend=None, initial_window_size=None):
        # `authority`/`scheme` build the :authority/:scheme pseudo-headers for a
        # bare-path request. h2 takes :scheme from the request URI (client.rs
        # L1627); httpunk's caller supplies it here (`util.connect` passes "https"
        # for a TLS-dialed connection), defaulting to "http" for cleartext.
        self.authority = authority
        self.scheme = scheme
        # Our advertised per-stream recv window (SETTINGS_INITIAL_WINDOW_SIZE),
        # defaulting to hyper's 2 MB unless the caller overrides it.
        self._initial_window_size = initial_window_size if initial_window_size is not None else _STREAM_WINDOW
        # We advertise SETTINGS_ENABLE_PUSH=0 + our window/frame/header-list profile
        # (see connect()); these take effect for receiving once the peer ACKs.
        super().__init__(
            transport,
            backend=backend,
            codec_role="client",
            settings=Settings(
                LocalSettings(
                    initial_window_size=self._initial_window_size,
                    max_frame_size=_MAX_FRAME_SIZE,
                    max_header_list_size=_MAX_HEADER_LIST_SIZE,
                )
            ),
        )
        # Signalled once the peer's initial SETTINGS have been applied, so requests
        # respect the peer's limits/window from the first one.
        self._ready_evt = self.backend.event()
        self.streams = ClientStreamManager(self)
        self.streams._conn_recv_target = _CONN_WINDOW  # raised via WINDOW_UPDATE(0) in _begin
        # Grant a larger-than-default per-stream recv window IMMEDIATELY, before the
        # peer ACKs our SETTINGS: the peer only sends more than the 65535 default once
        # it has processed our SETTINGS, so accepting up to the advertised window can
        # never over-accept — whereas waiting for the ACK (`apply_local_settings`)
        # leaves a window where the peer uses the new size before we grant it → a
        # spurious stream FLOW_CONTROL_ERROR. A *smaller* window still waits for the
        # ACK (RFC 7540 §6.9.2) so the peer can't overrun data sent under the default.
        if self._initial_window_size > self.streams._recv_init:
            self.streams._recv_init = self._initial_window_size

    async def connect(self):
        # h2: client.rs `handshake` (L1220) — over the caller-supplied transport,
        # flush the client preface + our initial SETTINGS, spawn the driver, then
        # wait for the peer's initial SETTINGS (its connection preface) before we're
        # ready for requests. (Dialing/TLS/ALPN are the caller's job.)
        settings = {
            "enable_push": False,
            "initial_window_size": self._initial_window_size,
            "max_frame_size": _MAX_FRAME_SIZE,
            "max_header_list_size": _MAX_HEADER_LIST_SIZE,
        }
        await self._begin(PREFACE, settings)
        await self._ready_evt.wait()
        if self.error is not None:
            raise self.error

    def _signal_ready(self):
        self._ready_evt.set()

    def send_body_background(self, stream, body):
        """Send the request body concurrently with (not before) the caller awaiting
        the response head. h2's `SendStream` (body) and `ResponseFuture` (head) are
        independent: an early response that resets the request body — the
        413/redirect-during-upload pattern, where the server emits its response then
        RST_STREAM(NO_ERROR) — must still deliver the received response, not have the
        body-send error mask it. The write runs in the connection's dedicated write
        scope (not the read-pump scope, so it doesn't disturb the pump's delicate
        close teardown) so it can outlive `send_request` (full duplex) and is torn
        down when the connection closes (h2 client.rs: send_request returns the
        ResponseFuture immediately)."""
        self._write_scope.spawn(self._write_body(stream, body))

    async def _write_body(self, stream, body):
        # The stream may error/reset mid-send (413/redirect + RST_STREAM). That is
        # recorded on `stream.error` and surfaced when the response body is read; a
        # background writer has nowhere to propagate to, so suppress it here.
        with contextlib.suppress(Exception):
            await self.streams.send_body(stream, body)


class H2Connection(BaseClientConnection):
    """An HTTP/2 client connection over a caller-supplied, already-connected
    `transport` (BYO transport, like hyper's `client::conn::http2::handshake(io)`;
    dialing / TLS / ALPN are the caller's or `httpunk.util`'s job). Use as an
    async context manager; the driver's read-pump runs for the lifetime of the
    `async with` block, and the transport is closed on exit.

    `authority` (e.g. ``"example.com:443"``) builds the :authority pseudo-header
    for requests given a bare path; requests with an absolute-URL target carry
    their own authority. `__aenter__`/`__aexit__`/`request`/`get` come from
    `BaseClientConnection` (identical to `H1Connection`).
    """

    def __init__(self, transport, *, authority=None, scheme="http", backend=None, initial_window_size=None):
        self._conn = Connection(
            transport, authority=authority, scheme=scheme, backend=backend, initial_window_size=initial_window_size
        )

    def ready(self):
        """Wait until the connection can accept a new request — it's alive (not
        failed, no GOAWAY received) and a MAX_CONCURRENT_STREAMS slot is free —
        then return. Raises if the connection has failed or the peer sent GOAWAY.
        Mirrors h2's `SendRequest::ready` (client.rs L401; underlying
        `poll_ready` at L367).

        Best-effort / non-reserving, like h2's `poll_ready`: a concurrent
        `send_request` may still take the slot first, so `send_request` re-applies
        the same backpressure. Calling `ready()` first is therefore optional — it
        just lets a caller pre-flight capacity/liveness without opening a stream.
        """
        return self._conn.streams.wait_until_ready()

    async def send_request(self, request):
        """Send `request` and return its `Response` once the head arrives.

        h2: client.rs `SendRequest::send_request` (L512). Open the stream + send
        HEADERS, stream the body, then await the response head.
        """
        # A bodyless request carries END_STREAM on HEADERS (h2 `send_request`
        # with `end_of_stream`), not a trailing empty DATA frame. A HEAD request
        # response never has a body regardless of content-length.
        end_stream = request.body is None
        is_head = request.method.upper() == "HEAD"
        stream = await self._conn.streams.open_stream(
            request.method,
            self._resolve(request.target),
            request.headers,
            end_stream=end_stream,
            is_head=is_head,
        )
        if not end_stream:
            self._conn.send_body_background(stream, request.body)

        await stream.headers_evt.wait()
        # h2's `ResponseFuture` resolves the moment the head arrives (client.rs
        # `poll_response` returns on HEADERS). Gate on whether the head arrived
        # (`stream.status`), NOT on `self._conn.error`: a fully-received response
        # must still be returned when the connection closes right after (server
        # answers + GOAWAY + close is a legitimate graceful shutdown) — a subsequent
        # stream/connection error surfaces when the body is read (`H2ResponseBody`
        # re-raises `stream.error` after EOF). REGRESSION GUARD: do not re-add a
        # post-head `if self._conn.error: raise` here — it discards a received
        # response and diverges from hyper. (Not unit-testable over a loopback: the
        # bug is a scheduling race where `send_request` re-checks only after the
        # connection has failed, which a quiet loopback never reproduces.)
        if stream.status is None:  # woken by a reset/failure, not a real response head
            raise stream.error or self._conn.error or ConnectionClosedError("connection closed before response")
        return Response(stream.status, stream.headers, H2ResponseBody(stream, self._conn.streams))

    def _resolve(self, target):
        # An absolute URL passes through; a bare path is resolved against the
        # connection's authority (the codec splits it into :scheme/:authority/:path).
        if "://" in target:
            return target
        if self._conn.authority is None:
            raise ValueError(
                "request target is a bare path but the connection has no authority; "
                "pass authority=... to H2Connection or use an absolute-URL target"
            )
        return f"{self._conn.scheme}://{self._conn.authority}{target}"
