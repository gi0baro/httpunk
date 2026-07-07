"""Phase 3 error taxonomy: the client surfaces GOAWAY / RST_STREAM / EOF as
typed exceptions (all subclasses of H2Error), driven by a server that emits
those frames."""

import pytest
from _client import open_h2
from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners

from httpunk import (
    GoAwayError,
    H2Error,
    H2FlowControlError,
    H2ProtocolError,
    H2Reason,
    StreamResetError,
)
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
                await conn.get("/")
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
                await conn.get("/")
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
            resp = await conn.get("/a")  # stream 1 <= last_stream_id: completes normally
            body = await resp.read()
            with pytest.raises(GoAwayError):
                await conn.get("/b")  # a new stream is refused
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
                await conn.get("/")
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
            resp = await conn.get("/big")
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
                await conn.get("/")
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
                await conn.get("/")
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
                await conn.get("/")
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
            resp1 = await conn.get("/a")
            assert await resp1.read() == b"ok"
            resp2 = await conn.get("/b")  # connection survived the late frame
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
            resp1 = await conn.get("/a")
            async for _chunk in resp1.aiter_bytes():
                break  # read one chunk, then cancel
            await resp1.aclose()  # RST(CANCEL) -> stream enters the reset store
            resp2 = await conn.get("/b")
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
            assert (await conn.get("/warmup")).status == 200  # applies our SETTINGS window
            # The RST may surface at get() or at read() depending on whether the
            # pump has processed the overrun DATA by the time the caller resumes;
            # both are correct, so accept either.
            with pytest.raises(H2Error):  # stream reset (flow control)
                resp1 = await conn.get("/a")
                await resp1.read()
            resp2 = await conn.get("/b")  # connection survived
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
            resp = await conn.get("/")  # stream 1 <= last_stream_id -> kept running
            assert resp.status == 200
            assert await resp.read() == b"ok"
        s.cancel()
