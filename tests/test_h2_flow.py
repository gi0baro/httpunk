"""Phase 2 end-to-end over tonio loopback: streaming a large response body
(exercises recv flow control + WINDOW_UPDATE) and multiplexing two concurrent
streams. The inline server respects the client's flow-control windows — waiting
for WINDOW_UPDATEs before sending past them — so the recv path is exercised for
real."""

import pytest
from _client import open_h2
from tonio.colored import Event, scope, sleep
from tonio.colored.net import open_tcp_listeners
from tonio.colored.sync import Lock

from httpunk import H2Reason, StreamResetError
from httpunk._httpunk import (
    H2_DEFAULT_DATA_FRAME_BUDGET as _DATA_FRAME_BUDGET,
    H2_DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD as _DATA_FRAME_OVERHEAD_THRESHOLD,
    H2_MAX_RECV_EMPTY_DATA_FRAMES as _MAX_RECV_EMPTY_DATA_FRAMES,
    H2Codec,
    H2FrameData as Data,
    H2FrameHeaders as Headers,
    H2FrameSettings as Settings,
    H2FrameWindowUpdate as WindowUpdate,
    H2Streams,
)
from httpunk.exceptions import H2ProtocolError
from httpunk.h2.client import Connection
from httpunk.h2.connection import PREFACE
from httpunk.h2.share import H2ResponseBody
from httpunk.h2.stream import Stream
from httpunk.http import HeaderMap


_DEFAULT_WINDOW = 65_535


class _Server:
    """A small h2c server that honours the peer's flow-control windows."""

    def __init__(self, listener, *, settings=None):
        self.listener = listener
        self.settings = settings or {}
        self.window_updates = 0
        self.headers_seen = []  # stream ids in the order HEADERS arrived
        self.req_bodies = {}  # stream_id -> received request body
        self._wlock = Lock()  # window state
        self._slock = Lock()  # serialize socket writes
        self._window_evt = Event()
        self._conn_window = _DEFAULT_WINDOW  # raised by the client's initial WINDOW_UPDATE(0)
        # The per-stream send window we may use toward the client = the client's
        # advertised SETTINGS_INITIAL_WINDOW_SIZE (applied in `_process`), NOT the bare
        # protocol default — else we'd under-send when the client advertises a larger
        # window (e.g. hyper's 2 MB) and deadlock waiting for a WINDOW_UPDATE it has no
        # reason to send.
        self._client_initial_window = _DEFAULT_WINDOW
        self._stream_windows = {}

    async def serve(self, responder, reqscope):
        self._stream = await self.listener.accept()
        self._codec = H2Codec("server")
        await self._send(self._codec.serialize_settings(**self.settings))

        raw = b""
        while len(raw) < len(PREFACE):
            chunk = await self._stream.receive_some(65536)
            if not chunk:
                return
            raw += chunk
        assert raw[: len(PREFACE)] == PREFACE

        await self._process(self._codec.receive(raw[len(PREFACE) :]), responder, reqscope)
        while True:
            chunk = await self._stream.receive_some(65536)
            if not chunk:
                break
            await self._process(self._codec.receive(chunk), responder, reqscope)

    async def _process(self, frames, responder, reqscope):
        for f in frames:
            if isinstance(f, Settings) and not f.ack:
                if f.initial_window_size is not None:
                    self._client_initial_window = f.initial_window_size  # streams the client will accept
                await self._send(self._codec.serialize_settings_ack())
            elif isinstance(f, WindowUpdate):
                self.window_updates += 1
                async with self._wlock:
                    if f.stream_id == 0:
                        self._conn_window += f.increment
                    else:
                        self._stream_windows[f.stream_id] = (
                            self._stream_windows.get(f.stream_id, self._client_initial_window) + f.increment
                        )
                self._window_evt.set()
            elif isinstance(f, Headers):
                self.headers_seen.append(f.stream_id)
                self._stream_windows.setdefault(f.stream_id, self._client_initial_window)
                # A bodyless request carries END_STREAM on HEADERS (no trailing
                # empty DATA frame), so the request is already complete here.
                if f.end_stream:
                    reqscope.spawn(responder(self, f.stream_id))
            elif isinstance(f, Data):
                self.req_bodies[f.stream_id] = self.req_bodies.get(f.stream_id, b"") + f.data
                if f.end_stream:
                    # Spawn so the read loop keeps running (and can observe any
                    # further HEADERS) while the responder sends.
                    reqscope.spawn(responder(self, f.stream_id))

    async def _send(self, data):
        async with self._slock:
            await self._stream.send_all(data)

    async def send_response(self, sid, body, *, status=200, chunk=16384):
        await self._send(
            self._codec.serialize_response_headers(sid, status, HeaderMap([("content-type", b"text/plain")]))
        )
        offset = 0
        while offset < len(body):
            while self._available(sid) <= 0:  # blocked on the peer's window
                self._window_evt.clear()
                if self._available(sid) > 0:
                    break
                await self._window_evt.wait()
            async with self._wlock:
                n = min(
                    self._conn_window,
                    self._stream_windows.get(sid, self._client_initial_window),
                    chunk,
                    len(body) - offset,
                )
                self._conn_window -= n
                self._stream_windows[sid] -= n
            await self._send(self._codec.serialize_data(sid, body[offset : offset + n], end_stream=False))
            offset += n
        await self._send(self._codec.serialize_data(sid, b"", end_stream=True))

    def _available(self, sid):
        return min(self._conn_window, self._stream_windows.get(sid, self._client_initial_window))


@pytest.mark.tonio
async def test_streaming_large_body():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    server = _Server(listener)
    payload = bytes(3 * 1024 * 1024)  # > the client's 2 MB stream window -> requires WINDOW_UPDATEs

    async def responder(srv, sid):
        await srv.send_response(sid, payload)

    chunks = []
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/big")
            async for chunk in resp.aiter_bytes():
                chunks.append(chunk)
            status = resp.status
        s.cancel()

    assert status == 200
    assert b"".join(chunks) == payload
    # The body exceeds the initial window, so consuming it must reclaim capacity.
    assert server.window_updates > 0


@pytest.mark.tonio
async def test_multiplexing_two_streams():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    server = _Server(listener)

    async def responder(srv, sid):
        await srv.send_response(sid, f"stream-{sid}".encode())

    results = {}
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            done = [Event(), Event()]

            async def fetch(i, path):
                resp = await conn.request("GET", path)
                results[path] = (resp.status, await resp.read())
                done[i].set()

            async with scope() as reqs:
                reqs.spawn(fetch(0, "/a"))
                reqs.spawn(fetch(1, "/b"))
                await done[0].wait()
                await done[1].wait()
                reqs.cancel()
        s.cancel()

    # Two concurrent streams on one connection; the client assigns ids 1 and 3
    # (odd, increasing), but which path lands on which id is a scheduling race.
    assert set(results.values()) == {(200, b"stream-1"), (200, b"stream-3")}


@pytest.mark.tonio
async def test_max_concurrent_streams_gating():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    # Advertise a limit of 1: the client must not open a second stream until the
    # first has closed.
    server = _Server(listener, settings={"max_concurrent_streams": 1})
    gating_held = []

    async def responder(srv, sid):
        if sid == 1:
            # Hold the first response briefly. A client that ignored the limit
            # would open stream 3 during this window; a gated client cannot.
            await sleep(0.05)
            gating_held.append(set(srv.headers_seen) == {1})
        await srv.send_response(sid, f"stream-{sid}".encode())

    results = {}
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            done = [Event(), Event()]

            async def fetch(i, path):
                resp = await conn.request("GET", path)
                results[path] = await resp.read()
                done[i].set()

            async with scope() as reqs:
                reqs.spawn(fetch(0, "/a"))
                reqs.spawn(fetch(1, "/b"))
                await done[0].wait()
                await done[1].wait()
                reqs.cancel()
        s.cancel()

    assert set(results.values()) == {b"stream-1", b"stream-3"}
    # While stream 1 was still open, the client never opened stream 3.
    assert gating_held == [True]


@pytest.mark.tonio
async def test_request_body_echo():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    server = _Server(listener)

    async def responder(srv, sid):
        await srv.send_response(sid, srv.req_bodies.get(sid, b""))

    async def gen_body():  # an async iterable of chunks
        yield b"hello "
        yield b"streamed "
        yield b"body"

    bytes_result = {}
    iter_result = {}
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            r1 = await conn.request("POST", "/bytes", body=b"a fixed body")
            bytes_result["body"] = await r1.read()
            r2 = await conn.request("POST", "/stream", body=gen_body())
            iter_result["body"] = await r2.read()
        s.cancel()

    assert bytes_result["body"] == b"a fixed body"
    assert iter_result["body"] == b"hello streamed body"


@pytest.mark.tonio
async def test_reset_wakes_flow_blocked_sender():
    """A peer RST_STREAM must wake a request-body sender parked on flow control,
    surfacing StreamResetError instead of hanging — h2 wakes the send side on
    reset. Regression for the client `recv_reset` wakeup (a hang would trip the
    6s deadline). The peer advertises a tiny window, so the client sends its
    window-worth and parks, then the peer RSTs instead of sending WINDOW_UPDATE."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def peer():
        stream = await listener.accept()
        codec = H2Codec("server")
        await stream.send_all(codec.serialize_settings(initial_window_size=5))
        raw = b""
        while len(raw) < len(PREFACE):
            raw += await stream.receive_some(65536)
        data, rst_sent = raw[len(PREFACE) :], False
        while True:
            for f in codec.receive(data):
                if isinstance(f, Settings) and not f.ack:
                    await stream.send_all(codec.serialize_settings_ack())
                elif isinstance(f, Data) and not rst_sent:
                    # The client has sent its 5-byte window and is now parked
                    # waiting for a WINDOW_UPDATE we never send — RST instead.
                    await stream.send_all(codec.serialize_rst_stream(f.stream_id, int(H2Reason.CANCEL)))
                    rst_sent = True
            data = await stream.receive_some(65536)
            if not data:
                break

    async with scope() as s:
        s.spawn(peer())
        async with open_h2(host, port) as conn:
            with pytest.raises(StreamResetError):
                await conn.request("POST", "/x", body=bytes(50))  # 50 > 5-byte window
        s.cancel()


@pytest.mark.tonio
async def test_reset_does_not_double_release_connection_window():
    """When a stream is reset, its in-flight recv data is returned to the CONNECTION
    window (`release_closed_capacity`). If the app then consumes the still-buffered
    bytes, `release_capacity` must NOT credit the connection window a second time
    (F22). Socket-free: a small amount stays below the WINDOW_UPDATE threshold, so the
    effect is visible directly on the connection recv window."""
    conn, st, data = _recv_streaming_connection()
    before = conn.conn_recv_available
    conn.recv_data(data(b"x" * 100))  # the pump consumed 100 conn-window bytes...
    assert conn.conn_recv_available == before - 100
    assert conn.stream_recv_unreleased(1) == 100  # ...delivered to the body queue, not yet read

    conn._reset(st, int(H2Reason.CANCEL))  # a reset returns the 100 to the conn window
    assert conn.conn_recv_available == before
    conn._after(conn.release_capacity(1, 100))  # app reads the buffered bytes -> must be a no-op
    assert conn.conn_recv_available == before  # NOT credited twice


def _recv_streaming_connection():
    """Socket-free client connection with one stream whose recv half is streaming
    (a request whose response head arrived), plus a codec round-trip helper to mint
    DATA frames (H2FrameData has no Python constructor — serialize then parse). The
    budget is pinned to the h2 default floor (Auto would resolve to _CONN_WINDOW/2) so
    the tests' arithmetic is deterministic."""
    conn = Connection(None, data_frame_budget=_DATA_FRAME_BUDGET)  # constructed only; never connected
    st = Stream(conn.backend)
    assert conn.try_claim_slot()
    st.id = conn.open_stream("GET", "http://x/", HeaderMap(), True, False, st)  # send half closed
    server, client = H2Codec("server"), H2Codec("client")
    [head] = client.receive(server.serialize_response_headers(1, 200))
    v = conn.recv_headers(head)  # the response head: the recv half is now streaming
    assert v.handle is st

    def data(payload, end=False):
        [frame] = client.receive(server.serialize_data(1, payload, end_stream=end))
        return frame

    return conn, st, data


def _deliver(conn, frame):
    """What the read pump does with a DATA verdict: queue the payload (and the EOF)."""
    v = conn.recv_data(frame)
    if v.handle is not None:
        if v.payload is not None:
            v.handle.body_send.send((v.payload, v.budgeted))
        if v.eof:
            v.handle.body_send.send(None)
    conn._after(v.flags)


@pytest.mark.tonio
async def test_small_data_frame_flood_exhausts_budget():
    """h2 0.4.16 #935: flow control bounds payload bytes, not frame count — a
    peer fragmenting into tiny unconsumed frames must exhaust the DATA-framing
    budget and die with a connection ENHANCE_YOUR_CALM."""
    conn, st, data = _recv_streaming_connection()
    with pytest.raises(H2ProtocolError) as exc:
        # 1-byte frames cost 255 budget each; 25600 total -> dies by frame 101.
        for _ in range(101):
            _deliver(conn, data(b"x"))
    assert exc.value.args[0] == int(H2Reason.ENHANCE_YOUR_CALM)


@pytest.mark.tonio
async def test_consumed_small_frames_return_budget():
    """Promptly-consumed small messages on a long-lived connection never exhaust
    the budget (each consumed chunk returns its charge), and a large frame earns
    budget back (capped) — h2 `release_data_frame`/`record_data_frame`."""
    conn, st, data = _recv_streaming_connection()
    for _ in range(300):  # 3x the raw budget in tiny frames, consumed as they come
        _deliver(conn, data(b"x"))
        assert await st.body_recv.receive() == (b"x", True)  # DataEvent: (payload, is_budgeted)
        conn.release_data_frame(1, 1)
    for _ in range(50):  # now leave 50 tiny frames unconsumed
        _deliver(conn, data(b"x"))
    drained = conn.data_frame_budget_available
    _deliver(conn, data(b"y" * 1000))  # a large frame replenishes...
    assert conn.data_frame_budget_available == drained + (1000 - _DATA_FRAME_OVERHEAD_THRESHOLD)
    conn.release_data_frame(1, 2**20)  # ...and replenishing never exceeds the cap
    assert conn.data_frame_budget_available <= _DATA_FRAME_BUDGET


@pytest.mark.tonio
async def test_empty_nonfinal_data_dropped_and_capped():
    """An empty non-final DATA frame has no effect on the HTTP message: it is
    never delivered to the app, does NOT touch the byte budget, and counts
    against its own per-connection lifetime cap instead (h2 0.4.19 counts.rs
    `num_recv_empty_data_frames` — a flood dies with ENHANCE_YOUR_CALM)."""
    conn, st, data = _recv_streaming_connection()
    _deliver(conn, data(b""))  # dropped
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET  # byte budget untouched
    assert conn.data_frame_budget_empty_frames == 1
    _deliver(conn, data(b"real"))
    assert await st.body_recv.receive() == (b"real", True)  # the empty frame never surfaced
    # An empty FINAL frame is the message end and IS delivered, unbudgeted
    # (h2: only `!is_end_stream` frames are budgeted or discarded).
    _deliver(conn, data(b"", end=True))
    assert await st.body_recv.receive() == (b"", False)
    assert await st.body_recv.receive() is None  # EOF sentinel follows
    assert conn.data_frame_budget_empty_frames == 1  # the final empty frame doesn't count


@pytest.mark.tonio
async def test_empty_data_frame_flood_dies_at_cap():
    """The empty-frame lifetime cap: MAX_RECV_EMPTY_DATA_FRAMES empties pass,
    the next one is a connection ENHANCE_YOUR_CALM — and the byte budget is
    still untouched (h2 0.4.19 counts.rs L97-113)."""
    conn, st, data = _recv_streaming_connection()
    for _ in range(_MAX_RECV_EMPTY_DATA_FRAMES):
        _deliver(conn, data(b""))
    with pytest.raises(H2ProtocolError) as exc:
        _deliver(conn, data(b""))
    assert exc.value.args[0] == int(H2Reason.ENHANCE_YOUR_CALM)
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET


@pytest.mark.tonio
async def test_final_data_frame_never_budgeted():
    """A final (END_STREAM) DATA frame is exempt from the framing budget — a
    stream receives at most one, so it can't create unbounded overhead (h2
    0.4.19 streams.rs L640-649). It is delivered tagged unbudgeted so the
    reader doesn't release a charge that was never made."""
    conn, st, data = _recv_streaming_connection()
    _deliver(conn, data(b"x", end=True))  # tiny AND final -> no charge
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET
    assert await st.body_recv.receive() == (b"x", False)
    assert await st.body_recv.receive() is None


@pytest.mark.tonio
async def test_reset_releases_buffered_frames_budget():
    """Resetting a stream with buffered-but-unread small frames returns their
    charge to the connection budget (h2 0.4.19: `clear_recv_buffer` /
    `release_closed_capacity` release each budgeted frame) — without this, a
    long-lived connection leaks budget on every abandoned body. A release
    racing the teardown must not double-credit (the F22-style clamp)."""
    conn, st, data = _recv_streaming_connection()
    for _ in range(50):  # 50 unconsumed tiny frames
        _deliver(conn, data(b"x"))
    charged = 50 * (_DATA_FRAME_OVERHEAD_THRESHOLD - 1)
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET - charged
    assert conn.stream_data_budget_charged(1) == charged
    conn._reset(st, int(H2Reason.CANCEL))
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET  # fully restored
    conn.release_data_frame(1, 1)  # a straggling reader release after the reclaim...
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET  # ...credits nothing (no double release)


@pytest.mark.tonio
async def test_aclose_after_eos_reclaims_window_and_budget():
    """Abandoning a FULLY-RECEIVED body with unread buffered chunks returns both
    the connection recv window and the framing-budget charge: upstream's drop
    path guards only the Drop-reset on eos (`maybe_cancel`, streams.rs L1686) —
    `release_closed_capacity` + `clear_recv_buffer` run UNCONDITIONALLY once no
    handle can read the data (streams.rs L1670-1676, recv.rs L502-522). aclose
    is this driver's deterministic drop hook. A straggling reader release after
    the reclaim credits nothing (F22)."""
    conn, st, data = _recv_streaming_connection()
    for _ in range(50):  # 50 unread tiny frames...
        _deliver(conn, data(b"x"))
    _deliver(conn, data(b"end", end=True))  # ...then EOS arrives, all unread
    assert conn.is_recv_end_stream(1)
    charged = 50 * (_DATA_FRAME_OVERHEAD_THRESHOLD - 1)  # the final frame is unbudgeted
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET - charged
    assert conn.stream_recv_unreleased(1) == 53  # 50x b"x" + b"end", none released
    before = conn.conn_recv_available

    await H2ResponseBody(st, conn).aclose()
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET  # budget fully restored
    assert conn.conn_recv_available == before + 53  # connection window fully restored
    assert not conn.has_stream(1)  # settled: nothing left to release

    restored = conn.conn_recv_available
    conn._after(conn.release_capacity(1, 1))  # a straggling reader release...
    conn.release_data_frame(1, 1)
    assert conn.conn_recv_available == restored  # ...credits neither window...
    assert conn.data_frame_budget_available == _DATA_FRAME_BUDGET  # ...nor budget, twice


@pytest.mark.tonio
async def test_data_frame_budget_resolve():
    """The budget resolves like h2 0.4.19 `DataFrameBudget::resolve`: a
    configured value as-is; Auto = half the connection recv window, floored at
    DEFAULT_DATA_FRAME_BUDGET."""
    conn = Connection(None)  # client Auto: _CONN_WINDOW (5 MB) / 2
    assert conn.data_frame_budget_available == 5 * 1024 * 1024 // 2
    conn = Connection(None, data_frame_budget=123)  # configured wins, unscaled
    assert conn.data_frame_budget_available == 123
    tiny = H2Streams(  # a tiny window floors at the default
        "client",
        initial_window_size=65_535,
        connection_window=1000,
        max_frame_size=16_384,
        max_header_list_size=16_384,
        max_send_buf_size=1,
    )
    assert tiny.data_frame_budget_available == _DATA_FRAME_BUDGET
