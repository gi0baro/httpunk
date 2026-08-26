"""HTTP/2 server (`H2Server`) over a tonio loopback, driven by httpunk's own
`H2Connection` client — true end-to-end coverage of both sides of the stack:
request/response, request + response bodies, headers/pseudo-headers, streaming,
and multiplexing.
"""

import contextlib

import pytest
from _client import open_h2
from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners

from httpunk import H2Reason
from httpunk._backend.tonio import TonioBackend
from httpunk._httpunk import (
    H2Codec,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FrameRstStream as RstStream,
    H2FrameSettings as Settings,
    H2FrameWindowUpdate as WindowUpdate,
)
from httpunk.h2 import H2Server
from httpunk.h2.connection import PREFACE
from httpunk.http import HeaderMap


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


async def _echo_server(listener, ready=None):
    """Accept one connection and echo each request: reply 200 with body
    `b"<METHOD> <path> -> " + request_body`."""
    transport = await listener.accept()
    async with H2Server(transport) as server:
        if ready is not None:
            ready.set()
        async for req in server:
            body = await req.read()
            reply = f"{req.method} {req.path} -> ".encode() + body
            await req.respond(200, headers={"content-type": "text/plain"}, body=reply)


@pytest.mark.tonio
async def test_server_get():
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/hello")
            assert resp.status == 200
            assert resp.headers["content-type"] == b"text/plain"
            assert await resp.read() == b"GET /hello -> "
        s.cancel()


@pytest.mark.tonio
async def test_server_post_echo_body():
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/submit", body=b"payload!")
            assert resp.status == 200
            assert await resp.read() == b"POST /submit -> payload!"
        s.cancel()


@pytest.mark.tonio
async def test_client_gets_early_response_despite_body_reset():
    """A server that responds before reading the request body resets the stream while
    the client is still uploading (F3: RST_STREAM(NO_ERROR)). The client must still
    return the received response — the body-send error must not mask it (F6). The body
    exceeds the flow-control window, so the send is guaranteed in-flight (blocked on a
    WINDOW_UPDATE) when the RST lands."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    await req.respond(200, body=b"early")  # respond WITHOUT reading the body

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/x", body=b"x" * (2 * 1024 * 1024))  # > the server's 1 MB window
            assert resp.status == 200
            assert await resp.read() == b"early"
        s.cancel()


@pytest.mark.tonio
async def test_server_streaming_response_body():
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                await req.read()

                async def chunks():
                    yield b"a" * 50_000
                    yield b"b" * 50_000  # total 100KB > one window -> needs WINDOW_UPDATEs

                await req.respond(200, body=chunks())

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/big")
            body = await resp.read()
        s.cancel()
    assert body == b"a" * 50_000 + b"b" * 50_000


@pytest.mark.tonio
async def test_server_multiplexed_requests():
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server, scope() as handlers:

            async def handle(req):
                await req.respond(200, body=f"{req.path}".encode())

            async for req in server:
                handlers.spawn(handle(req))  # serve concurrently (h2 multiplexing)

    results = {}
    async with scope() as s:
        s.spawn(serve())
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

    assert results == {"/a": (200, b"/a"), "/b": (200, b"/b")}


@pytest.mark.tonio
async def test_server_request_headers():
    listener, host, port = await _listener()
    seen = {}

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                seen["authority"] = req.authority
                seen["scheme"] = req.scheme
                seen["x-custom"] = req.headers.get("x-custom")
                await req.respond(204)

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/", headers={"x-custom": "abc"})
            assert resp.status == 204
            assert await resp.read() == b""
        s.cancel()

    assert seen["scheme"] == "http"
    assert seen["authority"] == f"{host}:{port}"
    assert seen["x-custom"] == b"abc"


# ----- adversarial: a raw client (arbitrary frames) must provoke the right
#       connection-level errors from the server -----


async def _raw_handshake(host, port):
    """Connect a raw client: send the preface + SETTINGS, return (transport, codec).

    Callers must run their test body in `try/finally: transport.close(); s.cancel()`:
    an assertion that fires while the transport is still open leaves the spawned
    server parked in a read only our EOF can end (tonio's optimistic cancellation
    cannot land on its pre-existing waiter), and the scope join then waits it out
    forever — the conftest deadline turns the real failure into an opaque timeout.
    Both calls are idempotent, so a body that already closed/cancelled is fine."""
    transport = await TonioBackend().connect_tcp(host, port)
    codec = H2Codec("client")
    await transport.send_all(PREFACE + codec.serialize_settings(enable_push=False))
    return transport, codec


async def _read_goaway(transport, codec):
    """Read frames (acking the server's SETTINGS) until a GOAWAY arrives, or None on EOF."""
    while True:
        data = await transport.receive_some(65536)
        if not data:
            return None
        for f in codec.receive(data):
            if isinstance(f, Settings) and not f.ack:
                await transport.send_all(codec.serialize_settings_ack())
            elif isinstance(f, GoAway):
                return f


async def _serve_forever(listener):
    transport = await listener.accept()
    with contextlib.suppress(Exception):
        async with H2Server(transport) as server:
            async for req in server:
                await req.respond(200)


async def _read_frame(transport, codec, kind):
    """Read frames (acking the server's SETTINGS) until one of type `kind` arrives,
    or None on EOF."""
    while True:
        data = await transport.receive_some(65536)
        if not data:
            return None
        for f in codec.receive(data):
            if isinstance(f, Settings) and not f.ack:
                await transport.send_all(codec.serialize_settings_ack())
            elif isinstance(f, kind):
                return f


@pytest.mark.tonio
async def test_server_rsts_unread_request_body_with_no_error():
    """A server that responds without consuming the request body drops the request:
    while the client is still sending (recv half open), h2 sends RST_STREAM(NO_ERROR)
    so it stops (the nginx-compat `maybe_cancel` rule). Regression guard for F3 — the
    unread upload used to pin the connection window with no RST ever sent."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    await req.respond(200)  # respond WITHOUT reading the body

    async with scope() as s:
        s.spawn(serve())
        transport, codec = await _raw_handshake(host, port)
        try:
            # Open stream 1 with a body but never send END_STREAM (client still uploading).
            await transport.send_all(codec.serialize_request_headers(1, "POST", "http://x/a", HeaderMap()))
            await transport.send_all(codec.serialize_data(1, b"partial upload", end_stream=False))
            rst = await _read_frame(transport, codec, RstStream)
            assert rst is not None and rst.stream_id == 1
            assert rst.error_code == H2Reason.NO_ERROR  # NO_ERROR (nginx-compat), not CANCEL
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_advertises_hyper_settings_profile():
    """The server ships hyper's tuned profile (F24), not bare-h2 defaults: its initial
    SETTINGS carry MAX_CONCURRENT_STREAMS=200, a 1 MB stream window, 16 KB max frame,
    16 KB max header list, and NO ENABLE_PUSH; plus an initial WINDOW_UPDATE(0) that
    raises the connection recv window from 65535 to 1 MB."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_serve_forever(listener))
        transport, codec = await _raw_handshake(host, port)
        try:
            frames = []
            while not any(isinstance(f, WindowUpdate) for f in frames):  # SETTINGS then WINDOW_UPDATE(0)
                data = await transport.receive_some(65536)
                assert data, "connection closed before the server's preface completed"
                frames += codec.receive(data)
            settings = next(f for f in frames if isinstance(f, Settings) and not f.ack)
            assert settings.max_concurrent_streams == 200
            assert settings.initial_window_size == 1024 * 1024
            assert settings.max_frame_size == 16 * 1024
            assert settings.enable_push is None  # the server does not advertise ENABLE_PUSH
            wu = next(f for f in frames if isinstance(f, WindowUpdate))
            assert wu.stream_id == 0
            assert wu.increment == 1024 * 1024 - 65535  # raise the 65535 default conn window to 1 MB
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_forgotten_stream_frames_swallowed_after_first_rst():
    """A frame on a forgotten (closed) stream draws ONE RST_STREAM(STREAM_CLOSED); the
    id then enters the reset store, so further frames on it are silently swallowed
    rather than each drawing another RST (F17 — the reset-amplification defence)."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_serve_forever(listener))  # 200 (bodyless) per request
        transport, codec = await _raw_handshake(host, port)
        try:
            # Complete stream 1 so the server closes + forgets it.
            await transport.send_all(
                codec.serialize_request_headers(1, "GET", "http://x/a", HeaderMap(), end_stream=True)
            )
            assert (await _read_frame(transport, codec, Headers)).stream_id == 1  # the 200 response
            # Two late DATA frames on the now-forgotten stream 1, then a fresh request (stream
            # 3) as a read barrier so we know both late frames were processed.
            await transport.send_all(codec.serialize_data(1, b"late1", end_stream=False))
            await transport.send_all(codec.serialize_data(1, b"late2", end_stream=False))
            await transport.send_all(
                codec.serialize_request_headers(3, "GET", "http://x/b", HeaderMap(), end_stream=True)
            )
            rsts = 0
            done = False
            while not done:
                data = await transport.receive_some(65536)
                assert data, "connection closed unexpectedly"
                for f in codec.receive(data):
                    if isinstance(f, Settings) and not f.ack:
                        await transport.send_all(codec.serialize_settings_ack())
                    elif isinstance(f, RstStream) and f.stream_id == 1:
                        rsts += 1
                    elif isinstance(f, Headers) and f.stream_id == 3:  # barrier: stream 3 served
                        done = True
            assert rsts == 1  # the 2nd late frame was swallowed, not answered with another RST
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_goaway_on_remote_reset_flood():
    """Rapid Reset (CVE-2023-44487): a flood of HEADERS+RST_STREAM on streams the app
    never accepts. Reset pending-accept streams stop counting as concurrent, so
    MAX_CONCURRENT can't gate the flood; h2 caps them separately (20) and tears the
    connection down with GOAWAY(ENHANCE_YOUR_CALM). Regression guard for F4."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        with contextlib.suppress(Exception):
            async with H2Server(transport):
                await Event().wait()  # hold the connection open; never accept a request

    async with scope() as s:
        s.spawn(serve())
        transport, codec = await _raw_handshake(host, port)
        try:
            sid = 1
            with contextlib.suppress(Exception):  # server may GOAWAY + close mid-flood
                for _ in range(30):  # well past the cap of 20
                    await transport.send_all(codec.serialize_request_headers(sid, "GET", "http://x/p", HeaderMap()))
                    await transport.send_all(codec.serialize_rst_stream(sid, int(H2Reason.CANCEL)))
                    sid += 2
            ga = await _read_frame(transport, codec, GoAway)
            assert ga is not None
            assert ga.error_code == H2Reason.ENHANCE_YOUR_CALM
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_goaway_on_idle_stream_data():
    """DATA on a stream the client never opened via HEADERS (idle) is a connection
    PROTOCOL_ERROR — the server GOAWAYs (h2 recv `ensure_not_idle`)."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_serve_forever(listener))
        transport, codec = await _raw_handshake(host, port)
        try:
            await transport.send_all(codec.serialize_data(5, b"x", end_stream=False))  # idle stream 5
            ga = await _read_goaway(transport, codec)
            assert ga is not None
            assert ga.error_code == H2Reason.PROTOCOL_ERROR
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_swallows_late_headers_on_reset_stream():
    """A HEADERS frame on a stream the server just locally-reset is swallowed (h2
    reset-stream store), not a connection PROTOCOL_ERROR — so the connection
    survives and still serves later requests (Tier-1 drift #4: the server now
    consults the reset store before the decreased-id check)."""
    listener, host, port = await _listener()

    async def serve():
        transport = await listener.accept()
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server, scope() as handlers:
                async for req in server:

                    async def handle(r):
                        with contextlib.suppress(Exception):
                            await r.read()
                            await r.respond(200)

                    handlers.spawn(handle(req))

    async with scope() as s:
        s.spawn(serve())
        transport, codec = await _raw_handshake(host, port)
        try:
            # stream 1: declare content-length 5 but send 10 bytes -> the server RSTs stream 1.
            await transport.send_all(
                codec.serialize_request_headers(1, "POST", "http://x/a", HeaderMap([("content-length", "5")]))
            )
            await transport.send_all(codec.serialize_data(1, b"0123456789", end_stream=False))
            # A late HEADERS on the now-reset stream 1 (the client hadn't seen the RST):
            # must be swallowed, not treated as a decreased-id connection error.
            await transport.send_all(codec.serialize_request_headers(1, "POST", "http://x/a", end_stream=True))
            # A fresh request on stream 3 must still be served (connection alive).
            await transport.send_all(codec.serialize_request_headers(3, "GET", "http://x/b", end_stream=True))

            status, goaway = None, None
            while status is None and goaway is None:
                data = await transport.receive_some(65536)
                if not data:
                    break
                for f in codec.receive(data):
                    if isinstance(f, Settings) and not f.ack:
                        await transport.send_all(codec.serialize_settings_ack())
                    elif isinstance(f, Headers) and f.stream_id == 3:
                        status = f.status
                    elif isinstance(f, GoAway):
                        goaway = f
        finally:
            transport.close()
            s.cancel()

    assert goaway is None, "connection torn down instead of swallowing the late HEADERS"
    assert status == 200


@pytest.mark.tonio
async def test_server_goaway_on_rst_stream_zero():
    """RST_STREAM on stream 0 is a connection PROTOCOL_ERROR (h2 recv_reset)."""
    listener, host, port = await _listener()
    async with scope() as s:
        s.spawn(_serve_forever(listener))
        transport, codec = await _raw_handshake(host, port)
        try:
            await transport.send_all(codec.serialize_rst_stream(0, H2Reason.CANCEL))
            ga = await _read_goaway(transport, codec)
            assert ga is not None
            assert ga.error_code == H2Reason.PROTOCOL_ERROR
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_h2_request_trailers_round_trip():
    """Request trailers (F45): the client sends them as a trailing HEADERS frame
    (END_STREAM) after the DATA frames; the H2Server decodes them into `req.trailers`."""
    listener, host, port = await _listener()
    seen = {}

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/", body=b"data", trailers={"x-checksum": "abc"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["body"] == b"data"
    assert seen["trailers"] is not None
    assert seen["trailers"].get("x-checksum") == b"abc"


@pytest.mark.tonio
async def test_h2_connection_specific_trailers_rejected():
    """Trailers are a HEADERS block, so connection-specific fields are rejected
    (RFC 9113 §8.2.2; h2 0.4.16 #925) — BEFORE the stream is opened, so the
    connection stays fully usable and a subsequent valid trailer still sends."""
    listener, host, port = await _listener()
    seen = {}

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(ValueError):
                await conn.request("POST", "/", body=b"data", trailers={"connection": "close"})
            with pytest.raises(ValueError):  # `te` may only carry "trailers"
                await conn.request("POST", "/", body=b"data", trailers={"te": "gzip"})
            # The rejected calls never touched the wire — the SAME connection
            # still carries a valid-trailer request end to end.
            resp = await conn.request("POST", "/", body=b"data", trailers={"x-checksum": "abc"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["trailers"].get("x-checksum") == b"abc"


@pytest.mark.tonio
async def test_h2_connection_specific_request_headers_rejected():
    """Regular request HEADERS get the same RFC 9113 §8.2.2 rejection as
    trailers (h2 send.rs `send_headers` -> `check_headers`) — before any
    stream/slot state is touched, so the connection stays fully usable."""
    listener, host, port = await _listener()

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                await req.read()
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(ValueError):
                await conn.request("GET", "/", headers={"connection": "keep-alive"})
            with pytest.raises(ValueError):  # `te` may only carry "trailers"
                await conn.request("GET", "/", headers={"te": "gzip"})
            resp = await conn.request("GET", "/", headers={"te": "trailers"})  # the one legal `te`
            assert await resp.read() == b"ok"
        s.cancel()


@pytest.mark.tonio
async def test_h2_connection_specific_response_headers_rejected():
    """The server's response HEADERS get the same §8.2.2 rejection — before the
    state transition, so the handler can still send a valid response on the
    same stream after a rejected attempt."""
    listener, host, port = await _listener()

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                await req.read()
                with pytest.raises(ValueError):
                    await req.respond(200, headers={"connection": "close"}, body=b"nope")
                await req.respond(200, body=b"ok")  # stream untouched -> still respondable

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")
            assert resp.status == 200
            assert await resp.read() == b"ok"
        s.cancel()


@pytest.mark.tonio
async def test_h2_bodyless_request_with_trailers():
    """A request with trailers but NO body still ends on the trailing HEADERS frame
    (HEADERS + trailers, no DATA) rather than END_STREAM on the request HEADERS (F45)."""
    listener, host, port = await _listener()
    seen = {}

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/", trailers={"x-done": "1"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["body"] == b""
    assert seen["trailers"].get("x-done") == b"1"


# ===== peer GOAWAY → acknowledging GOAWAY + connection done (F23, server role) =====


async def _serve_and_report(listener, done):
    """Serve one connection; set `done` once the accept loop has exited and the
    server's `__aexit__` has run (i.e. the transport is closed)."""
    transport = await listener.accept()
    with contextlib.suppress(Exception):
        async with H2Server(transport) as server, scope() as handlers:
            async for req in server:

                async def handle(r):
                    await r.read()
                    await r.respond(200, body=b"ok")

                handlers.spawn(handle(req))
    done.set()


async def _settle_handshake(transport, codec, seen):
    """Read the server's SETTINGS and ack it BEFORE the test sends GOAWAY: once the
    server has replied GOAWAY it stops reading (h2: `should_close_now` → no more
    `poll_next`), so a SETTINGS ack still in flight would sit unread in its socket
    buffer and turn its close into a TCP RST instead of a FIN."""
    while not any(isinstance(f, Settings) and not f.ack for f in seen):
        data = await transport.receive_some(65536)
        assert data
        for f in codec.receive(data):
            if isinstance(f, Settings) and not f.ack:
                await transport.send_all(codec.serialize_settings_ack())
            seen.append(f)


async def _read_until_eof(transport, codec, *, seen=None):
    """Read frames (acking SETTINGS) until the peer's FIN. Returns the frames seen."""
    seen = [] if seen is None else seen
    while True:
        data = await transport.receive_some(65536)
        if not data:
            return seen
        for f in codec.receive(data):
            if isinstance(f, Settings) and not f.ack:
                await transport.send_all(codec.serialize_settings_ack())
            seen.append(f)


async def _assert_goaway_then_eof(transport, codec, done, *, seen=None):
    """The one contract under test: the server replies GOAWAY(NO_ERROR), then CLOSES
    (the client reads EOF — without the fix it hangs here), and its accept loop ends."""
    seen = await _read_until_eof(transport, codec, seen=seen)
    goaways = [f for f in seen if isinstance(f, GoAway)]
    assert len(goaways) == 1 and goaways[0].error_code == H2Reason.NO_ERROR
    await done.wait()
    transport.close()
    return seen


@pytest.mark.tonio
@pytest.mark.parametrize("last_id", [0, 1])
async def test_server_closes_after_goaway_reply_when_idle(last_id):
    """Peer GOAWAY on an idle connection (one completed request): the server sends its
    acknowledging GOAWAY(NO_ERROR) AND closes the connection / ends the accept loop —
    h2 `go_away_now` → `should_close_now` → the connection future resolves (F23).
    Previously only the GOAWAY went out and the socket leaked forever."""
    listener, host, port = await _listener()
    done = Event()
    async with scope() as s:
        s.spawn(_serve_and_report(listener, done))
        transport, codec = await _raw_handshake(host, port)
        try:
            await transport.send_all(codec.serialize_request_headers(1, "GET", "http://x/", end_stream=True))
            seen = []
            while not any(isinstance(f, Headers) for f in seen):  # full response for stream 1
                data = await transport.receive_some(65536)
                assert data
                for f in codec.receive(data):
                    if isinstance(f, Settings) and not f.ack:
                        await transport.send_all(codec.serialize_settings_ack())
                    seen.append(f)
            await transport.send_all(codec.serialize_go_away(last_id, H2Reason.NO_ERROR))
            await _assert_goaway_then_eof(transport, codec, done, seen=seen)
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_closes_after_goaway_reply_no_streams_ever():
    """Peer GOAWAY right after the handshake, no stream ever opened: same contract."""
    listener, host, port = await _listener()
    done = Event()
    async with scope() as s:
        s.spawn(_serve_and_report(listener, done))
        transport, codec = await _raw_handshake(host, port)
        try:
            seen = []
            await _settle_handshake(transport, codec, seen)
            await transport.send_all(codec.serialize_go_away(0, H2Reason.NO_ERROR))
            await _assert_goaway_then_eof(transport, codec, done, seen=seen)
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_closes_after_goaway_received_mid_request():
    """Peer GOAWAY(last=1) while stream 1 is still in flight, and the peer sends NOTHING
    more. Stream 1 finishing is a local event — h2's `drop_stream_ref` wakes the
    connection task so the idle check re-runs (streams.rs L1647); httpunk must do the
    same from the stream-close path, not only after inbound bytes."""
    listener, host, port = await _listener()
    done = Event()
    async with scope() as s:
        s.spawn(_serve_and_report(listener, done))
        transport, codec = await _raw_handshake(host, port)
        try:
            seen = []
            await _settle_handshake(transport, codec, seen)
            # Request body still pending (no END_STREAM) when the GOAWAY lands...
            await transport.send_all(codec.serialize_request_headers(1, "POST", "http://x/", end_stream=False))
            await transport.send_all(codec.serialize_go_away(1, H2Reason.NO_ERROR))
            # ...then finish it; after this the client is silent.
            await transport.send_all(codec.serialize_data(1, b"x", end_stream=True))
            seen = await _assert_goaway_then_eof(transport, codec, done, seen=seen)
            # Stream 1 was <= last_stream_id, so it was served before the close.
            assert any(isinstance(f, Headers) and f.stream_id == 1 and f.status == 200 for f in seen)
        finally:
            transport.close()
            s.cancel()


@pytest.mark.tonio
async def test_server_keeps_serving_after_phase1_goaway():
    """A phase-1 graceful GOAWAY (last_stream_id = 2^31-1) is NOT an idle-close
    trigger: the connection must stay open and keep serving (h2 `should_close_on_idle`
    excludes `StreamId::MAX`)."""
    listener, host, port = await _listener()
    done = Event()
    async with scope() as s:
        s.spawn(_serve_and_report(listener, done))
        transport, codec = await _raw_handshake(host, port)
        try:
            await transport.send_all(codec.serialize_go_away(2**31 - 1, H2Reason.NO_ERROR))
            await transport.send_all(codec.serialize_request_headers(1, "GET", "http://x/", end_stream=True))
            seen = []
            while not any(isinstance(f, Headers) for f in seen):
                data = await transport.receive_some(65536)
                assert data, "server closed after a phase-1 GOAWAY"
                for f in codec.receive(data):
                    if isinstance(f, Settings) and not f.ack:
                        await transport.send_all(codec.serialize_settings_ack())
                    seen.append(f)
            assert not any(isinstance(f, GoAway) for f in seen)
            assert not done.is_set()
            transport.close()
            await done.wait()  # the server sees our FIN and exits cleanly
        finally:
            transport.close()
            s.cancel()
