"""Phase 3 error taxonomy: the client surfaces GOAWAY / RST_STREAM / EOF as
typed exceptions (all subclasses of HTTPunkError — the protocol violations under
H2Error, connection-closed under ConnectionClosedError), driven by a server that
emits those frames."""

import pytest
from _net import H2Peer, h2_client_against, listen, read_to_eof
from tonio.colored import Event, scope, sleep

from httpunk import (
    ConnectionClosedError,
    GoAwayError,
    H2Connection,
    H2Error,
    H2FlowControlError,
    H2ProtocolError,
    H2Reason,
    HTTPunkError,
    StreamResetError,
)
from httpunk._backend.tonio import TonioBackend
from httpunk._httpunk import (
    H2Codec,
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as Settings,
)
from httpunk.http import HeaderMap


@pytest.mark.tonio
async def test_rst_stream_raises_stream_reset():
    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_rst_stream(sid, H2Reason.REFUSED_STREAM)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        with pytest.raises(StreamResetError) as exc:
            await conn.request("GET", "/")

    assert exc.value.error_code == H2Reason.REFUSED_STREAM
    assert isinstance(exc.value, H2Error)


@pytest.mark.tonio
async def test_goaway_raises_goaway_error():
    async def server(peer):
        await peer.request()
        # last_stream_id=0 -> no streams processed; the client's stream is refused.
        await peer.send_go_away(0, H2Reason.ENHANCE_YOUR_CALM, b"slow down")
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        with pytest.raises(GoAwayError) as exc:
            await conn.request("GET", "/")

    assert exc.value.error_code == H2Reason.ENHANCE_YOUR_CALM
    assert exc.value.debug_data == b"slow down"
    assert isinstance(exc.value, H2Error)


@pytest.mark.tonio
async def test_goaway_completes_in_flight_and_refuses_new():
    async def server(peer):
        sid = (await peer.request()).stream_id
        # Graceful GOAWAY: last_stream_id = sid, so this stream is still honoured.
        await peer.send_go_away(sid, H2Reason.NO_ERROR)
        await peer.send_response_headers(sid, 200)
        await peer.send_data(sid, b"ok", end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        resp = await conn.request("GET", "/a")  # stream 1 <= last_stream_id: completes normally
        body = await resp.read()
        with pytest.raises(GoAwayError):
            await conn.request("GET", "/b")  # a new stream is refused

    assert resp.status == 200
    assert body == b"ok"


@pytest.mark.tonio
async def test_connection_closed_raises():
    async def server(peer):
        await peer.request()
        peer.close()  # abrupt close instead of a response

    async with h2_client_against(server) as conn:
        with pytest.raises(HTTPunkError):  # ConnectionClosedError (or a reset surfaced as such)
            await conn.request("GET", "/")


@pytest.mark.tonio
async def test_cancel_sends_rst_stream():
    seen_rst = []

    async def server(peer):
        sid = (await peer.request()).stream_id
        # Send headers + one body chunk, but leave the stream open (no END_STREAM).
        await peer.send_response_headers(sid, 200)
        await peer.send_data(sid, b"partial-body", end_stream=False)
        while (f := await peer.recv(RstStream)) is not None:  # client sent RST then closed
            seen_rst.append((f.stream_id, f.error_code))

    async with h2_client_against(server) as conn:
        resp = await conn.request("GET", "/big")
        first = None
        async for chunk in resp.aiter_bytes():
            first = chunk
            break  # read one chunk, then abandon the rest
        await resp.aclose()

    assert resp.status == 200
    assert first == b"partial-body"
    assert seen_rst and seen_rst[0][1] == H2Reason.CANCEL


@pytest.mark.tonio
async def test_protocol_error_sends_goaway():
    """When the client detects a connection-level protocol/flow violation, it
    notifies the peer with GOAWAY and fails the connection."""
    seen_goaway = []

    async def server(peer):
        await peer.request()
        # A connection WINDOW_UPDATE that overflows our send window (> 2^31-1).
        await peer.send_window_update(0, (1 << 31) - 1)
        while (f := await peer.recv(GoAway)) is not None:
            seen_goaway.append(f.error_code)

    async with h2_client_against(server) as conn:
        with pytest.raises(H2FlowControlError):
            await conn.request("GET", "/")

    assert seen_goaway and seen_goaway[0] == H2Reason.FLOW_CONTROL_ERROR


@pytest.mark.tonio
async def test_malformed_frame_sends_goaway():
    """A malformed frame is a connection error: the client sends GOAWAY."""
    seen_goaway = []

    async def server(peer):
        await peer.request()
        # SETTINGS with a 3-byte payload (not a multiple of 6) -> malformed.
        malformed = (3).to_bytes(3, "big") + bytes([0x04, 0x00]) + (0).to_bytes(4, "big") + b"\xaa\xbb\xcc"
        await peer.send(malformed)
        while (f := await peer.recv(GoAway)) is not None:
            seen_goaway.append(f.error_code)

    async with h2_client_against(server) as conn:
        with pytest.raises(H2ProtocolError):
            await conn.request("GET", "/")

    assert seen_goaway  # client told the peer about the protocol error


@pytest.mark.tonio
async def test_frame_on_idle_stream_sends_goaway():
    """A frame on a stream the client never opened (idle) is a connection error:
    the client sends GOAWAY(PROTOCOL_ERROR). h2: recv_headers idle-stream check."""
    seen_goaway = []

    async def server(peer):
        await peer.request()  # client opens stream 1
        # Respond on stream 5, which the client never opened -> idle stream.
        await peer.send_response_headers(5, 200)
        while (f := await peer.recv(GoAway)) is not None:
            seen_goaway.append(f.error_code)

    async with h2_client_against(server) as conn:
        with pytest.raises(H2ProtocolError):
            await conn.request("GET", "/")

    assert seen_goaway and seen_goaway[0] == H2Reason.PROTOCOL_ERROR


@pytest.mark.tonio
async def test_frame_on_closed_stream_resets_it():
    """A frame on a stream the client opened and has since closed gets
    RST_STREAM(STREAM_CLOSED); the connection survives. h2: may_have_forgotten_stream."""
    seen_rst = []

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 200)
        await peer.send_data(sid, b"ok", end_stream=True)
        # Late DATA on the now fully-closed stream (not one we locally reset).
        await peer.send_data(sid, b"late", end_stream=False)
        # One loop: collect the client's RST + serve its second request.
        sid2 = None
        while (f := await peer.recv()) is not None:
            if isinstance(f, RstStream):
                seen_rst.append((f.stream_id, f.error_code))
            elif isinstance(f, Headers):
                sid2 = f.stream_id
                if f.end_stream:  # bodyless GET: request complete on HEADERS
                    await peer.send_response_headers(sid2, 200)
                    await peer.send_data(sid2, b"two", end_stream=True)
            elif isinstance(f, Data) and f.end_stream and f.stream_id == sid2:
                await peer.send_response_headers(sid2, 200)
                await peer.send_data(sid2, b"two", end_stream=True)

    async with h2_client_against(server) as conn:
        resp1 = await conn.request("GET", "/a")
        assert await resp1.read() == b"ok"
        resp2 = await conn.request("GET", "/b")  # connection survived the late frame
        assert await resp2.read() == b"two"

    assert (1, H2Reason.STREAM_CLOSED) in seen_rst


@pytest.mark.tonio
async def test_locally_reset_stream_swallows_late_frames():
    """After the client RSTs a stream, late frames the peer sent before seeing
    the RST are ignored (h2 reset_stream_duration): no second RST, and the
    connection keeps serving further requests."""
    seen_rst = []

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 200)
        await peer.send_data(sid, b"partial", end_stream=False)
        # One uniform loop: on the client's RST(CANCEL) send a frame it "crossed"
        # with — which it should swallow — and serve the second request whenever
        # its END_STREAM arrives.
        late_sent = False
        sid2 = None
        while (f := await peer.recv()) is not None:
            if isinstance(f, RstStream):
                seen_rst.append((f.stream_id, f.error_code))
                if f.stream_id == sid and not late_sent:
                    await peer.send_data(sid, b"late", end_stream=False)
                    late_sent = True
            elif isinstance(f, Headers):
                sid2 = f.stream_id
                if f.end_stream:  # bodyless GET: request complete on HEADERS
                    await peer.send_response_headers(sid2, 200)
                    await peer.send_data(sid2, b"two", end_stream=True)
            elif isinstance(f, Data) and f.end_stream and f.stream_id == sid2:
                await peer.send_response_headers(sid2, 200)
                await peer.send_data(sid2, b"two", end_stream=True)

    async with h2_client_against(server) as conn:
        resp1 = await conn.request("GET", "/a")
        async for _chunk in resp1.aiter_bytes():
            break  # read one chunk, then cancel
        await resp1.aclose()  # RST(CANCEL) -> stream enters the reset store
        resp2 = await conn.request("GET", "/b")
        assert await resp2.read() == b"two"

    # Exactly one RST (the cancel); the late frame was swallowed, not answered.
    assert seen_rst == [(1, H2Reason.CANCEL)]


@pytest.mark.tonio
async def test_stream_flow_violation_resets_only_that_stream():
    """A peer overrunning one stream's flow window RSTs *that* stream; the
    connection survives and further requests succeed."""

    async def server(peer):
        # Warm-up request, answered in full. Completing it guarantees the
        # client has processed our SETTINGS ACK (which precedes this response
        # on the wire) and so applied its advertised 100-byte initial window —
        # otherwise the overrun below races the ACK (streams opened before it
        # still use the 65535 default, RFC 7540 §6.9.2 / h2 `init_window_sz`).
        sid0 = (await peer.request()).stream_id
        await peer.send_response_headers(sid0, 200, end_stream=True)
        # Now overrun the 100-byte window on the next stream.
        sid1 = (await peer.request()).stream_id
        await peer.send_response_headers(sid1, 200)
        await peer.send_data(sid1, b"x" * 200, end_stream=False)
        # A further request still succeeds on the same connection.
        sid2 = (await peer.request()).stream_id
        await peer.send_response_headers(sid2, 200)
        await peer.send_data(sid2, b"ok", end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server, initial_window_size=100) as conn:
        assert (await conn.request("GET", "/warmup")).status == 200  # applies our SETTINGS window
        # The RST may surface at get() or at read() depending on whether the
        # pump has processed the overrun DATA by the time the caller resumes;
        # both are correct, so accept either.
        with pytest.raises(H2Error):  # stream reset (flow control)
            resp1 = await conn.request("GET", "/a")
            await resp1.read()
        resp2 = await conn.request("GET", "/b")  # connection survived
        assert await resp2.read() == b"ok"


@pytest.mark.tonio
async def test_head_response_content_length_not_enforced():
    """A HEAD response carries the content-length of the would-be body but no body
    (END_STREAM on HEADERS). It must NOT be rejected as a content-length violation
    — HEAD is exempt (h2 `ContentLength::Head`)."""

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 200, HeaderMap([("content-length", "100")]), end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        resp = await conn.request("HEAD", "/")
        assert resp.status == 200
        assert resp.headers["content-length"] == b"100"
        assert await resp.read() == b""  # no body, and no content-length error


@pytest.mark.tonio
async def test_content_length_over_19_digits_rejected():
    """A content-length with >19 digits risks overflowing u64, so h2's `parse_u64`
    rejects it outright (headers.rs L329) before parsing. The client resets that stream
    with PROTOCOL_ERROR rather than accepting an unparseable length."""

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 200, HeaderMap([("content-length", "1" * 20)]))
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        with pytest.raises(H2Error):  # RST_STREAM(PROTOCOL_ERROR): content-length too long
            resp = await conn.request("GET", "/a")
            await resp.read()


@pytest.mark.tonio
async def test_stream_id_overflow_refuses_new_stream():
    """Once client stream ids are exhausted (past 2^31-1), a new request is refused —
    h2 `StreamId::next_id` returns `StreamIdOverflow` — as a user error, while the
    connection itself stays alive."""

    async def server(peer):
        # Keep the connection open — the client refuses before sending, so nothing to
        # answer, but a premature close would surface as a connection error and mask
        # the overflow refusal we're testing.
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        conn._conn.set_next_stream_id((2**31 - 1) + 2)  # exhaust the client id space
        with pytest.raises(H2Error, match="exhausted"):
            await conn.request("GET", "/a")


@pytest.mark.tonio
async def test_1xx_with_end_stream_is_reset_not_hung():
    """A 1xx interim response carrying END_STREAM is malformed — a 1xx is not the final
    response, so it can't end the stream (RFC 9113 §8.1). The client resets it
    (PROTOCOL_ERROR) instead of hanging forever on a final head that never comes."""

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 100, end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        with pytest.raises(H2Error):  # RST_STREAM(PROTOCOL_ERROR): 1xx can't END_STREAM
            await conn.request("GET", "/")


@pytest.mark.tonio
async def test_empty_body_rides_end_stream_on_headers():
    """A statically-empty body (`b""`) is bodyless: END_STREAM rides the HEADERS frame
    with NO trailing empty DATA frame — hyper's empty body reports is_end_stream()."""
    seen = {}

    async def server(peer):
        sid, headers, seen["data"] = await peer.request()
        seen["headers_es"] = headers.end_stream
        await peer.send_response_headers(sid, 200, end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        assert (await conn.request("POST", "/", body=b"")).status == 200

    assert seen["headers_es"] is True  # END_STREAM on HEADERS
    assert seen["data"] == []  # and no DATA frame at all


@pytest.mark.tonio
async def test_zero_length_interior_data_frame_sent_not_elided():
    """An interior zero-length body chunk goes on the wire as an empty non-END_STREAM
    DATA frame, not elided — h2 queues zero-length DATA frames (prioritize.rs)."""
    seen = {}

    async def server(peer):
        sid, _headers, seen["data"] = await peer.request()
        await peer.send_response_headers(sid, 200, end_stream=True)
        await read_to_eof(peer.stream)

    def body():
        yield b"a"
        yield b""  # interior zero-length chunk — must still be framed
        yield b"b"

    async with h2_client_against(server) as conn:
        assert (await conn.request("POST", "/", body=body())).status == 200

    payloads = [bytes(f.data) for f in seen["data"]]
    assert b"" in payloads  # the interior empty chunk was sent, not swallowed
    assert b"".join(payloads) == b"ab"


@pytest.mark.tonio
async def test_goaway_high_last_stream_id_accepted():
    """A first GOAWAY may carry any last_stream_id (h2 `Send::max_stream_id` starts
    at `StreamId::MAX`) — the standard graceful-shutdown pattern. Streams <= it keep
    running; the connection is NOT torn down with a PROTOCOL_ERROR."""

    async def server(peer):
        sid = (await peer.request()).stream_id
        # Graceful GOAWAY with a high last_stream_id, then serve the in-flight stream.
        await peer.send_go_away(2**31 - 1, H2Reason.NO_ERROR)
        await peer.send_response_headers(sid, 200, HeaderMap([("content-length", "2")]))
        await peer.send_data(sid, b"ok", end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        resp = await conn.request("GET", "/")  # stream 1 <= last_stream_id -> kept running
        assert resp.status == 200
        assert await resp.read() == b"ok"


@pytest.mark.tonio
async def test_close_wakes_inflight_waiter():
    """Closing a connection with an in-flight request must WAKE that request with an
    error, never hang it. The pump normally fails all streams on EOF, but close()'s
    cancel races that; fail_all in close() guarantees the straggler wakes regardless.
    (A hang would trip the per-test tonio deadline and fail loudly.)"""
    listener, host, port = await listen()
    req_received = Event()

    async def server():
        # Read the request, SIGNAL that it arrived, then NEVER respond and keep the
        # connection open — so the only thing that can wake the client's parked get()
        # is the client's own close(). The signal makes the test deterministic: once it
        # fires, do_get has finished sending HEADERS and is parked on the response.
        peer = await H2Peer.accept(listener)
        await peer.request()
        req_received.set()
        await read_to_eof(peer.stream)

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
        assert isinstance(outcome.get("err"), HTTPunkError)
        s.cancel()


@pytest.mark.tonio
async def test_aclose_after_connection_failure_is_clean():
    """When the connection fails while a response body is still open, the stream is
    transitioned to Closed (recv_eof fans out via fail_all), so aclose()-ing the
    response is a clean no-op instead of trying to RST_STREAM on the dead transport
    and erroring."""
    got_resp = Event()

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 200)  # head, no END_STREAM
        await got_resp.wait()  # let the client receive the head first
        peer.close()  # then abruptly fail the connection with the body still open

    async with h2_client_against(server) as conn:
        resp = await conn.request("GET", "/")  # head arrives; body left unread
        assert resp.status == 200
        got_resp.set()
        while not conn._conn.is_failed():  # wait until the pump processed the EOF (fail_all ran)
            await sleep(0)
        # The stream is now Closed via recv_eof; aclose must NOT RST the dead transport.
        await resp.aclose()  # must not reset the dead transport


@pytest.mark.tonio
async def test_client_replies_goaway_when_idle_after_peer_goaway():
    """After the peer GOAWAYs and the in-flight stream finishes, the client sends its
    OWN acknowledging GOAWAY(NO_ERROR) rather than lingering until the peer closes the
    socket — h2 go_away_now-on-idle."""
    got_client_goaway = Event()

    async def server(peer):
        sid = (await peer.request()).stream_id
        # Graceful GOAWAY (last_stream_id = sid, so this stream is honoured), then the
        # full response so the stream completes and the client goes idle.
        await peer.send_go_away(sid, H2Reason.NO_ERROR)
        await peer.send_response_headers(sid, 200)
        await peer.send_data(sid, b"ok", end_stream=True)
        if await peer.recv(GoAway) is not None:  # the client's acknowledging GOAWAY
            got_client_goaway.set()

    async with h2_client_against(server) as conn:
        resp = await conn.request("GET", "/")
        assert await resp.read() == b"ok"
        await got_client_goaway.wait()  # the client replied with its own GOAWAY (else hangs)


@pytest.mark.tonio
async def test_end_stream_with_content_length_resets_not_hangs():
    """A response whose final HEADERS carry END_STREAM AND content-length>0 is malformed
    (the declared length can't match the zero body bytes). The client resets it
    (PROTOCOL_ERROR) and send_request raises PROMPTLY, instead of hanging forever on the
    response head because reset_stream skipped waking the waiter on the closed stream."""

    async def server(peer):
        sid = (await peer.request()).stream_id
        await peer.send_response_headers(sid, 200, HeaderMap([("content-length", "5")]), end_stream=True)
        await read_to_eof(peer.stream)

    async with h2_client_against(server) as conn:
        with pytest.raises(H2Error):  # RST_STREAM(PROTOCOL_ERROR), woken promptly
            await conn.request("GET", "/")


@pytest.mark.tonio
async def test_close_flushes_queued_frames_even_after_connection_error():
    """`close()` drains the control-frame queue BEFORE the FIN regardless of
    `error` being set — h2's `State::Closing` runs `codec.shutdown` (flush, then
    shutdown) on every non-IO close (connection.rs L341-348). The GOAWAY-exchanged
    clean close relies on this: its reply GOAWAY is queued with `error` already
    set, and whether the write pump wakes before `close()` tears it down is a
    scheduler race. The write-pump wake is stubbed out so the queue is guaranteed
    to still hold the frame when `close()` runs."""
    seen = []

    async def server(peer):
        # This server NEVER writes after the handshake — deliberately no
        # SETTINGS-acks: the client goes ready on our SETTINGS alone and its
        # test body closes immediately, so an ack sent from here would race the
        # (legitimate) close and can land on an already-closed socket. The
        # client's kernel answers that with RST, which both kills this loop
        # short of "eof" and may discard the already-flushed PING from our
        # receive queue. Nothing in the client's path here needs
        # `apply_local_settings`.
        while True:
            try:
                chunk = await peer.stream.receive_some(65536)
            except ConnectionResetError:
                # Belt-and-braces: should a reset race reappear, fail the
                # assertion visibly (with `seen`).
                chunk = b""
            if not chunk:
                seen.append("eof")
                break
            for f in peer.codec.receive(chunk):
                if not (isinstance(f, Settings) and not f.ack):
                    seen.append(f)

    async with h2_client_against(server) as conn:
        inner = conn._conn
        # The pump swaps the pending buffer under the send lock (the ordering
        # invariant): held here, the pump wakes but cannot take the frame, so it
        # stays queued across the failure.
        async with inner._send_lock:
            # A peer PING makes the state queue its PONG (same payload) — a queued frame.
            [ping] = H2Codec("client").receive(H2Codec("server").serialize_ping(b"flushme!"))
            inner._after(inner.recv_ping(ping))
            inner._fail(ConnectionClosedError("simulated failure"))
        # Lock released before `close()` runs: its stop signal reaches the pump,
        # whose drain-then-exit is what flushes the queued frame.

    # `__aexit__` -> `close()` must have written the queued PING before closing.
    assert any(isinstance(f, Ping) and f.data == b"flushme!" for f in seen), seen


@pytest.mark.tonio
@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize(
    ("values", "valid"),
    [
        (("2", "2", "2"), True),
        (("2", "02", "002"), True),
        (("2", "3", "2"), False),
        (("2", "2", "3"), False),
        (("2", "2", "invalid"), False),
        (("2", "2", "18446744073709551616"), False),
    ],
)
async def test_validate_multiple_content_length_headers(method, values, valid):
    # h2 tests/h2-tests/tests/client_request.rs:1467 validate_multiple_content_length_headers.
    resets = []

    async def server(peer):
        sid = (await peer.request()).stream_id
        assert sid == 1
        await peer.send_response_headers(sid, 200, HeaderMap([("content-length", v) for v in values]))
        if valid:
            await peer.send_data(sid, b"" if method == "HEAD" else b"ok", end_stream=True)
        while (f := await peer.recv(RstStream)) is not None:
            resets.append((f.stream_id, f.error_code))

    async with h2_client_against(server) as conn:
        if valid:
            resp = await conn.request(method, "/")
            assert [bytes(chunk) async for chunk in resp.aiter_bytes()] == [b"" if method == "HEAD" else b"ok"]
        else:
            with pytest.raises(StreamResetError) as exc:
                await conn.request(method, "/")
            assert type(exc.value) is StreamResetError
            assert exc.value.error_code == H2Reason.PROTOCOL_ERROR
            assert exc.value.stream_id == 1

    assert resets == ([] if valid else [(1, H2Reason.PROTOCOL_ERROR)])


@pytest.mark.tonio
async def test_recv_too_big_headers():
    # h2 tests/h2-tests/tests/client_request.rs:893 recv_too_big_headers.
    requests, resets, errors = [], [], []

    async def server(peer):
        while len(requests) < 2:
            f = await peer.recv(Headers)
            assert f is not None
            requests.append(f.stream_id)
            assert f.method == "GET" and f.end_stream
            assert f.scheme == "https" and f.authority == "http2.akamai.com" and f.path == "/"
        assert peer.settings.max_header_list_size == 40
        assert requests == [1, 3]
        c = peer.codec
        await peer.send(c.serialize_response_headers(1, 200, end_stream=True), c.serialize_response_headers(3, 200))
        while (f := await peer.recv(RstStream)) is not None:
            resets.append((f.stream_id, f.error_code))

    async def request(conn):
        with pytest.raises(StreamResetError) as exc:
            await conn.request("GET", "https://http2.akamai.com/")
        assert type(exc.value) is StreamResetError
        assert exc.value.error_code == H2Reason.PROTOCOL_ERROR
        errors.append(exc.value.stream_id)

    async with h2_client_against(server, max_header_list_size=40) as conn, scope() as s:
        s.spawn(request(conn))
        s.spawn(request(conn))
    assert sorted(errors) == [1, 3]
    assert resets == [(3, H2Reason.PROTOCOL_ERROR)]


@pytest.mark.tonio
async def test_response_missing_status():
    # h2 tests/h2-tests/tests/client_request.rs:824 response_missing_status.
    requests = []
    resets = []

    async def server(peer):
        while (f := await peer.recv()) is not None:
            if isinstance(f, RstStream):
                resets.append((f.stream_id, f.error_code))
            elif isinstance(f, Headers):
                requests.append(f.stream_id)
                assert f.method == "GET"
                assert f.end_stream
                if f.stream_id == 1:
                    # Empty HPACK block: HEADERS with END_HEADERS | END_STREAM.
                    await peer.send(b"\x00\x00\x00\x01\x05\x00\x00\x00\x01")
                elif f.stream_id == 3:
                    await peer.send_response_headers(3, 103)
                    # Literal without indexing, new name: server: test, no :status.
                    block = b"\x00\x06server\x04test"
                    await peer.send(len(block).to_bytes(3, "big") + b"\x01\x04\x00\x00\x00\x03" + block)
                elif f.stream_id == 5:
                    assert resets == [(3, H2Reason.PROTOCOL_ERROR)]
                    await peer.send_response_headers(5, 204, end_stream=True)
                else:
                    pytest.fail(f"unexpected request stream {f.stream_id}")

    async with h2_client_against(server) as conn:
        for sid in (1, 3):
            with pytest.raises(StreamResetError) as exc:
                await conn.request("GET", "/")
            assert type(exc.value) is StreamResetError
            assert exc.value.stream_id == sid
            assert exc.value.error_code == H2Reason.PROTOCOL_ERROR
        assert (await conn.request("GET", "/")).status == 204

    assert requests == [1, 3, 5]
    assert resets == [(3, H2Reason.PROTOCOL_ERROR)]
