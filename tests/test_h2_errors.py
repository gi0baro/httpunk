"""Phase 3 error taxonomy: the client surfaces GOAWAY / RST_STREAM / EOF as
typed exceptions (all subclasses of H2Error), driven by a server that emits
those frames."""

import pytest
from _client import open_h2
from tonio.colored import Event, scope, sleep
from tonio.colored.net import open_tcp_listeners

from httpunk import (
    GoAwayError,
    H2Connection,
    H2Error,
    H2FlowControlError,
    H2ProtocolError,
    H2Reason,
    StreamResetError,
)
from httpunk._backend.tonio import TonioBackend
from httpunk._httpunk import (
    H2Codec,
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FrameRstStream as RstStream,
    H2FrameSettings as Settings,
)
from httpunk.h2.connection import PREFACE
from httpunk.http import HeaderMap


async def _accept_handshake(listener):
    """Accept, exchange SETTINGS, strip the client preface, and return
    `(stream, codec)` positioned to read/write frames. Also returns the first
    client request's stream id once seen."""
    stream = await listener.accept()
    codec = H2Codec("server")
    await stream.send_all(codec.serialize_settings())
    raw = b""
    while len(raw) < len(PREFACE):
        chunk = await stream.receive_some(65536)
        if not chunk:
            return stream, codec, None
        raw += chunk
    frames = list(codec.receive(raw[len(PREFACE) :]))
    return stream, codec, frames


async def _read_until_request(stream, codec, frames):
    """Handle SETTINGS ack + find the request's stream id, returning once the
    request is fully sent — END_STREAM on HEADERS for a bodyless request, or on
    the final DATA frame for one with a body."""
    sid = None
    while True:
        for f in frames:
            if isinstance(f, Settings) and not f.ack:
                await stream.send_all(codec.serialize_settings_ack())
            elif isinstance(f, Headers):
                sid = f.stream_id
                if f.end_stream:  # bodyless request: END_STREAM rides HEADERS
                    return sid
            elif isinstance(f, Data) and f.end_stream:
                return sid
        chunk = await stream.receive_some(65536)
        if not chunk:
            return sid
        frames = list(codec.receive(chunk))


async def _capture_request(stream, codec, frames):
    """Collect a request's frames until END_STREAM (ACKing SETTINGS along the way).
    Returns `(sid, headers_end_stream, [Data frames])` for asserting on wire framing."""
    sid = headers_es = None
    data = []
    while True:
        for f in frames:
            if isinstance(f, Settings) and not f.ack:
                await stream.send_all(codec.serialize_settings_ack())
            elif isinstance(f, Headers):
                sid, headers_es = f.stream_id, f.end_stream
                if f.end_stream:
                    return sid, headers_es, data
            elif isinstance(f, Data):
                data.append(f)
                if f.end_stream:
                    return sid, headers_es, data
        chunk = await stream.receive_some(65536)
        if not chunk:
            return sid, headers_es, data
        frames = list(codec.receive(chunk))


@pytest.mark.tonio
async def test_rst_stream_raises_stream_reset():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        await stream.send_all(codec.serialize_rst_stream(sid, H2Reason.REFUSED_STREAM))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(StreamResetError) as exc:
                await conn.request("GET", "/")
        s.cancel()

    assert exc.value.error_code == H2Reason.REFUSED_STREAM
    assert isinstance(exc.value, H2Error)


@pytest.mark.tonio
async def test_goaway_raises_goaway_error():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        await _read_until_request(stream, codec, frames)
        # last_stream_id=0 -> no streams processed; the client's stream is refused.
        await stream.send_all(codec.serialize_go_away(0, H2Reason.ENHANCE_YOUR_CALM, b"slow down"))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(GoAwayError) as exc:
                await conn.request("GET", "/")
        s.cancel()

    assert exc.value.error_code == H2Reason.ENHANCE_YOUR_CALM
    assert exc.value.debug_data == b"slow down"
    assert isinstance(exc.value, H2Error)


@pytest.mark.tonio
async def test_goaway_completes_in_flight_and_refuses_new():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        # Graceful GOAWAY: last_stream_id = sid, so this stream is still honoured.
        await stream.send_all(codec.serialize_go_away(sid, H2Reason.NO_ERROR))
        await stream.send_all(codec.serialize_response_headers(sid, 200))
        await stream.send_all(codec.serialize_data(sid, b"ok", end_stream=True))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/a")  # stream 1 <= last_stream_id: completes normally
            body = await resp.read()
            with pytest.raises(GoAwayError):
                await conn.request("GET", "/b")  # a new stream is refused
        s.cancel()

    assert resp.status == 200
    assert body == b"ok"


@pytest.mark.tonio
async def test_connection_closed_raises():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        await _read_until_request(stream, codec, frames)
        stream.close()  # abrupt close instead of a response

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2Error):  # ConnectionClosedError (or a reset surfaced as such)
                await conn.request("GET", "/")
        s.cancel()


@pytest.mark.tonio
async def test_cancel_sends_rst_stream():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen_rst = []
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            sid = await _read_until_request(stream, codec, frames)
            # Send headers + one body chunk, but leave the stream open (no END_STREAM).
            await stream.send_all(codec.serialize_response_headers(sid, 200))
            await stream.send_all(codec.serialize_data(sid, b"partial-body", end_stream=False))
            while True:
                chunk = await stream.receive_some(65536)
                if not chunk:  # client sent RST then closed
                    break
                for f in codec.receive(chunk):
                    if isinstance(f, RstStream):
                        seen_rst.append((f.stream_id, f.error_code))
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/big")
            first = None
            async for chunk in resp.aiter_bytes():
                first = chunk
                break  # read one chunk, then abandon the rest
            await resp.aclose()
        # Closing the connection makes the server see the RST + EOF; wait for it.
        await done.wait()
        s.cancel()

    assert resp.status == 200
    assert first == b"partial-body"
    assert seen_rst and seen_rst[0][1] == H2Reason.CANCEL


@pytest.mark.tonio
async def test_protocol_error_sends_goaway():
    """When the client detects a connection-level protocol/flow violation, it
    notifies the peer with GOAWAY and fails the connection."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen_goaway = []
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            await _read_until_request(stream, codec, frames)
            # A connection WINDOW_UPDATE that overflows our send window (> 2^31-1).
            await stream.send_all(codec.serialize_window_update(0, (1 << 31) - 1))
            while True:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    break
                for f in codec.receive(chunk):
                    if isinstance(f, GoAway):
                        seen_goaway.append(f.error_code)
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2FlowControlError):
                await conn.request("GET", "/")
        await done.wait()
        s.cancel()

    assert seen_goaway and seen_goaway[0] == H2Reason.FLOW_CONTROL_ERROR


@pytest.mark.tonio
async def test_malformed_frame_sends_goaway():
    """A malformed frame is a connection error: the client sends GOAWAY."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen_goaway = []
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            await _read_until_request(stream, codec, frames)
            # SETTINGS with a 3-byte payload (not a multiple of 6) -> malformed.
            malformed = (3).to_bytes(3, "big") + bytes([0x04, 0x00]) + (0).to_bytes(4, "big") + b"\xaa\xbb\xcc"
            await stream.send_all(malformed)
            while True:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    break
                for f in codec.receive(chunk):
                    if isinstance(f, GoAway):
                        seen_goaway.append(f.error_code)
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2ProtocolError):
                await conn.request("GET", "/")
        await done.wait()
        s.cancel()

    assert seen_goaway  # client told the peer about the protocol error


@pytest.mark.tonio
async def test_frame_on_idle_stream_sends_goaway():
    """A frame on a stream the client never opened (idle) is a connection error:
    the client sends GOAWAY(PROTOCOL_ERROR). h2: recv_headers idle-stream check."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen_goaway = []
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            await _read_until_request(stream, codec, frames)  # client opens stream 1
            # Respond on stream 5, which the client never opened -> idle stream.
            await stream.send_all(codec.serialize_response_headers(5, 200))
            while True:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    break
                for f in codec.receive(chunk):
                    if isinstance(f, GoAway):
                        seen_goaway.append(f.error_code)
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2ProtocolError):
                await conn.request("GET", "/")
        await done.wait()
        s.cancel()

    assert seen_goaway and seen_goaway[0] == H2Reason.PROTOCOL_ERROR


@pytest.mark.tonio
async def test_frame_on_closed_stream_resets_it():
    """A frame on a stream the client opened and has since closed gets
    RST_STREAM(STREAM_CLOSED); the connection survives. h2: may_have_forgotten_stream."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen_rst = []
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            sid = await _read_until_request(stream, codec, frames)
            await stream.send_all(codec.serialize_response_headers(sid, 200))
            await stream.send_all(codec.serialize_data(sid, b"ok", end_stream=True))
            # Late DATA on the now fully-closed stream (not one we locally reset).
            await stream.send_all(codec.serialize_data(sid, b"late", end_stream=False))
            # One loop: collect the client's RST + serve its second request.
            sid2 = None
            while True:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    break
                for f in codec.receive(chunk):
                    if isinstance(f, RstStream):
                        seen_rst.append((f.stream_id, f.error_code))
                    elif isinstance(f, Headers):
                        sid2 = f.stream_id
                        if f.end_stream:  # bodyless GET: request complete on HEADERS
                            await stream.send_all(codec.serialize_response_headers(sid2, 200))
                            await stream.send_all(codec.serialize_data(sid2, b"two", end_stream=True))
                    elif isinstance(f, Data) and f.end_stream and f.stream_id == sid2:
                        await stream.send_all(codec.serialize_response_headers(sid2, 200))
                        await stream.send_all(codec.serialize_data(sid2, b"two", end_stream=True))
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp1 = await conn.request("GET", "/a")
            assert await resp1.read() == b"ok"
            resp2 = await conn.request("GET", "/b")  # connection survived the late frame
            assert await resp2.read() == b"two"
        await done.wait()
        s.cancel()

    assert (1, H2Reason.STREAM_CLOSED) in seen_rst


@pytest.mark.tonio
async def test_locally_reset_stream_swallows_late_frames():
    """After the client RSTs a stream, late frames the peer sent before seeing
    the RST are ignored (h2 reset_stream_duration): no second RST, and the
    connection keeps serving further requests."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen_rst = []
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            sid = await _read_until_request(stream, codec, frames)
            await stream.send_all(codec.serialize_response_headers(sid, 200))
            await stream.send_all(codec.serialize_data(sid, b"partial", end_stream=False))
            # One uniform loop (frames for the RST and the 2nd request can share a
            # TCP chunk, so we must not discard any): on the client's RST(CANCEL)
            # send a frame it "crossed" with — which it should swallow — and serve
            # the second request whenever its END_STREAM arrives.
            late_sent = False
            sid2 = None
            while True:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    break
                for f in codec.receive(chunk):
                    if isinstance(f, RstStream):
                        seen_rst.append((f.stream_id, f.error_code))
                        if f.stream_id == sid and not late_sent:
                            await stream.send_all(codec.serialize_data(sid, b"late", end_stream=False))
                            late_sent = True
                    elif isinstance(f, Headers):
                        sid2 = f.stream_id
                        if f.end_stream:  # bodyless GET: request complete on HEADERS
                            await stream.send_all(codec.serialize_response_headers(sid2, 200))
                            await stream.send_all(codec.serialize_data(sid2, b"two", end_stream=True))
                    elif isinstance(f, Data) and f.end_stream and f.stream_id == sid2:
                        await stream.send_all(codec.serialize_response_headers(sid2, 200))
                        await stream.send_all(codec.serialize_data(sid2, b"two", end_stream=True))
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp1 = await conn.request("GET", "/a")
            async for _chunk in resp1.aiter_bytes():
                break  # read one chunk, then cancel
            await resp1.aclose()  # RST(CANCEL) -> stream enters the reset store
            resp2 = await conn.request("GET", "/b")
            assert await resp2.read() == b"two"
        await done.wait()
        s.cancel()

    # Exactly one RST (the cancel); the late frame was swallowed, not answered.
    assert seen_rst == [(1, H2Reason.CANCEL)]


@pytest.mark.tonio
async def test_stream_flow_violation_resets_only_that_stream():
    """A peer overrunning one stream's flow window RSTs *that* stream; the
    connection survives and further requests succeed."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    done = Event()

    async def server():
        try:
            stream, codec, frames = await _accept_handshake(listener)
            # Warm-up request, answered in full. Completing it guarantees the
            # client has processed our SETTINGS ACK (which precedes this response
            # on the wire) and so applied its advertised 100-byte initial window —
            # otherwise the overrun below races the ACK (streams opened before it
            # still use the 65535 default, RFC 7540 §6.9.2 / h2 `init_window_sz`).
            sid0 = await _read_until_request(stream, codec, frames)
            await stream.send_all(codec.serialize_response_headers(sid0, 200, end_stream=True))
            # Now overrun the 100-byte window on the next stream.
            sid1 = await _read_until_request(stream, codec, [])
            await stream.send_all(codec.serialize_response_headers(sid1, 200))
            await stream.send_all(codec.serialize_data(sid1, b"x" * 200, end_stream=False))
            # A further request still succeeds on the same connection.
            sid2 = await _read_until_request(stream, codec, [])
            await stream.send_all(codec.serialize_response_headers(sid2, 200))
            await stream.send_all(codec.serialize_data(sid2, b"ok", end_stream=True))
            while await stream.receive_some(65536):
                pass
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port, initial_window_size=100) as conn:
            assert (await conn.request("GET", "/warmup")).status == 200  # applies our SETTINGS window
            # The RST may surface at get() or at read() depending on whether the
            # pump has processed the overrun DATA by the time the caller resumes;
            # both are correct, so accept either.
            with pytest.raises(H2Error):  # stream reset (flow control)
                resp1 = await conn.request("GET", "/a")
                await resp1.read()
            resp2 = await conn.request("GET", "/b")  # connection survived
            assert await resp2.read() == b"ok"
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_head_response_content_length_not_enforced():
    """A HEAD response carries the content-length of the would-be body but no body
    (END_STREAM on HEADERS). It must NOT be rejected as a content-length violation
    — HEAD is exempt (h2 `ContentLength::Head`)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        await stream.send_all(
            codec.serialize_response_headers(sid, 200, HeaderMap([("content-length", "100")]), end_stream=True)
        )
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("HEAD", "/")
            assert resp.status == 200
            assert resp.headers["content-length"] == b"100"
            assert await resp.read() == b""  # no body, and no content-length error
        s.cancel()


@pytest.mark.tonio
async def test_content_length_over_19_digits_rejected():
    """A content-length with >19 digits risks overflowing u64, so h2's `parse_u64`
    rejects it outright (headers.rs L329) before parsing. The client resets that stream
    with PROTOCOL_ERROR rather than accepting an unparseable length (F37)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        await stream.send_all(codec.serialize_response_headers(sid, 200, HeaderMap([("content-length", "1" * 20)])))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2Error):  # RST_STREAM(PROTOCOL_ERROR): content-length too long
                resp = await conn.request("GET", "/a")
                await resp.read()
        s.cancel()


@pytest.mark.tonio
async def test_stream_id_overflow_refuses_new_stream():
    """Once client stream ids are exhausted (past 2^31-1), a new request is refused —
    h2 `StreamId::next_id` returns `StreamIdOverflow` — as a user error, while the
    connection itself stays alive (F43)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        # Complete the preface, then keep the connection open — the client refuses
        # before sending, so nothing to answer, but a premature close would surface as
        # a connection error and mask the overflow refusal we're testing.
        stream, _codec, _frames = await _accept_handshake(listener)
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            conn._conn.streams._next_id = (2**31 - 1) + 2  # exhaust the client id space
            with pytest.raises(H2Error, match="exhausted"):
                await conn.request("GET", "/a")
        s.cancel()


@pytest.mark.tonio
async def test_1xx_with_end_stream_is_reset_not_hung():
    """A 1xx interim response carrying END_STREAM is malformed — a 1xx is not the final
    response, so it can't end the stream (RFC 9113 §8.1). The client resets it
    (PROTOCOL_ERROR) instead of hanging forever on a final head that never comes (F38)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        await stream.send_all(codec.serialize_response_headers(sid, 100, end_stream=True))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2Error):  # RST_STREAM(PROTOCOL_ERROR): 1xx can't END_STREAM
                await conn.request("GET", "/")
        s.cancel()


@pytest.mark.tonio
async def test_empty_body_rides_end_stream_on_headers():
    """A statically-empty body (`b""`) is bodyless: END_STREAM rides the HEADERS frame
    with NO trailing empty DATA frame — hyper's empty body reports is_end_stream() (F39)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen = {}

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid, seen["headers_es"], seen["data"] = await _capture_request(stream, codec, frames)
        await stream.send_all(codec.serialize_response_headers(sid, 200, end_stream=True))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            assert (await conn.request("POST", "/", body=b"")).status == 200
        s.cancel()

    assert seen["headers_es"] is True  # END_STREAM on HEADERS
    assert seen["data"] == []  # and no DATA frame at all


@pytest.mark.tonio
async def test_zero_length_interior_data_frame_sent_not_elided():
    """An interior zero-length body chunk goes on the wire as an empty non-END_STREAM
    DATA frame, not elided — h2 queues zero-length DATA frames (prioritize.rs) (F40)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    seen = {}

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid, _es, seen["data"] = await _capture_request(stream, codec, frames)
        await stream.send_all(codec.serialize_response_headers(sid, 200, end_stream=True))
        while await stream.receive_some(65536):
            pass

    def body():
        yield b"a"
        yield b""  # interior zero-length chunk — must still be framed
        yield b"b"

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            assert (await conn.request("POST", "/", body=body())).status == 200
        s.cancel()

    payloads = [bytes(f.data) for f in seen["data"]]
    assert b"" in payloads  # the interior empty chunk was sent, not swallowed
    assert b"".join(payloads) == b"ab"


@pytest.mark.tonio
async def test_goaway_high_last_stream_id_accepted():
    """A first GOAWAY may carry any last_stream_id (h2 `Send::max_stream_id` starts
    at `StreamId::MAX`) — the standard graceful-shutdown pattern. Streams <= it keep
    running; the connection is NOT torn down with a PROTOCOL_ERROR."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        # Graceful GOAWAY with a high last_stream_id, then serve the in-flight stream.
        await stream.send_all(codec.serialize_go_away(2**31 - 1, H2Reason.NO_ERROR))
        await stream.send_all(codec.serialize_response_headers(sid, 200, HeaderMap([("content-length", "2")])))
        await stream.send_all(codec.serialize_data(sid, b"ok", end_stream=True))
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")  # stream 1 <= last_stream_id -> kept running
            assert resp.status == 200
            assert await resp.read() == b"ok"
        s.cancel()


@pytest.mark.tonio
async def test_close_wakes_inflight_waiter():
    """Closing a connection with an in-flight request must WAKE that request with an
    error, never hang it. The pump normally fails all streams on EOF, but close()'s
    cancel races that; fail_all in close() guarantees the straggler wakes regardless
    (F44). (A hang would trip the per-test tonio deadline and fail loudly.)"""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    req_received = Event()

    async def server():
        # Read the request, SIGNAL that it arrived, then NEVER respond and keep the
        # connection open — so the only thing that can wake the client's parked get()
        # is the client's own close(). The signal makes the test deterministic: once it
        # fires, do_get has finished sending HEADERS and is parked on the response.
        stream, codec, frames = await _accept_handshake(listener)
        await _read_until_request(stream, codec, frames)
        req_received.set()
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(server())
        conn = H2Connection(await TonioBackend().connect_tcp(host, port), authority=f"{host}:{port}")
        await conn.__aenter__()

        outcome = {}

        async def do_get():
            try:
                resp = await conn.request("GET", "/")
                outcome["resp"] = resp
            except Exception as exc:  # the test asserts on the exception type below
                outcome["err"] = exc

        async with scope() as inner:
            inner.spawn(do_get())
            await req_received.wait()  # do_get has sent HEADERS and is parked on the head
            await conn.__aexit__(None, None, None)  # close while do_get is parked
        # inner joined -> do_get finished; it must have been woken with an error
        assert isinstance(outcome.get("err"), H2Error)
        s.cancel()


@pytest.mark.tonio
async def test_aclose_after_connection_failure_is_clean():
    """When the connection fails while a response body is still open, the stream is
    transitioned to Closed (recv_eof fans out via fail_all), so aclose()-ing the
    response is a clean no-op instead of trying to RST_STREAM on the dead transport
    and erroring (F60)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    got_resp = Event()

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        await stream.send_all(codec.serialize_response_headers(sid, 200))  # head, no END_STREAM
        await got_resp.wait()  # let the client receive the head first
        stream.close()  # then abruptly fail the connection with the body still open

    async with scope() as s:
        s.spawn(server())
        conn = H2Connection(await TonioBackend().connect_tcp(host, port), authority=f"{host}:{port}")
        await conn.__aenter__()
        resp = await conn.request("GET", "/")  # head arrives; body left unread
        assert resp.status == 200
        got_resp.set()
        while conn._conn.error is None:  # wait until the pump processed the EOF (fail_all ran)
            await sleep(0)
        # The stream is now Closed via recv_eof; aclose must NOT RST the dead transport.
        await resp.aclose()  # would raise without F60
        await conn.__aexit__(None, None, None)
        s.cancel()


@pytest.mark.tonio
async def test_client_replies_goaway_when_idle_after_peer_goaway():
    """After the peer GOAWAYs and the in-flight stream finishes, the client sends its
    OWN acknowledging GOAWAY(NO_ERROR) rather than lingering until the peer closes the
    socket — h2 go_away_now-on-idle (F23)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    got_client_goaway = Event()

    async def server():
        stream, codec, frames = await _accept_handshake(listener)
        sid = await _read_until_request(stream, codec, frames)
        # Graceful GOAWAY (last_stream_id = sid, so this stream is honoured), then the
        # full response so the stream completes and the client goes idle.
        await stream.send_all(codec.serialize_go_away(sid, H2Reason.NO_ERROR))
        await stream.send_all(codec.serialize_response_headers(sid, 200))
        await stream.send_all(codec.serialize_data(sid, b"ok", end_stream=True))
        while True:  # read until the client's acknowledging GOAWAY reaches us
            chunk = await stream.receive_some(65536)
            if not chunk:
                break
            if any(isinstance(f, GoAway) for f in codec.receive(chunk)):
                got_client_goaway.set()
                break

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")
            assert await resp.read() == b"ok"
            await got_client_goaway.wait()  # the client replied with its own GOAWAY (else hangs)
        s.cancel()
