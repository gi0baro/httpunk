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
            resp = await conn.get("/hello")
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
            resp = await conn.get("/big")
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
                resp = await conn.get(path)
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
    """Connect a raw client: send the preface + SETTINGS, return (transport, codec)."""
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
        # Open stream 1 with a body but never send END_STREAM (client still uploading).
        await transport.send_all(codec.serialize_request_headers(1, "POST", "http://x/a", HeaderMap()))
        await transport.send_all(codec.serialize_data(1, b"partial upload", end_stream=False))
        rst = await _read_frame(transport, codec, RstStream)
        assert rst is not None and rst.stream_id == 1
        assert rst.error_code == H2Reason.NO_ERROR  # NO_ERROR (nginx-compat), not CANCEL
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
        sid = 1
        with contextlib.suppress(Exception):  # server may GOAWAY + close mid-flood
            for _ in range(30):  # well past the cap of 20
                await transport.send_all(codec.serialize_request_headers(sid, "GET", "http://x/p", HeaderMap()))
                await transport.send_all(codec.serialize_rst_stream(sid, int(H2Reason.CANCEL)))
                sid += 2
        ga = await _read_frame(transport, codec, GoAway)
        assert ga is not None
        assert ga.error_code == H2Reason.ENHANCE_YOUR_CALM
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
        await transport.send_all(codec.serialize_data(5, b"x", end_stream=False))  # idle stream 5
        ga = await _read_goaway(transport, codec)
        assert ga is not None
        assert ga.error_code == H2Reason.PROTOCOL_ERROR
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
        await transport.send_all(codec.serialize_rst_stream(0, H2Reason.CANCEL))
        ga = await _read_goaway(transport, codec)
        assert ga is not None
        assert ga.error_code == H2Reason.PROTOCOL_ERROR
        transport.close()
        s.cancel()
