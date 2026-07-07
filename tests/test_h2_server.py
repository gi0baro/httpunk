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
    H2FrameSettings as Settings,
)
from httpunk.h2 import H2Server
from httpunk.h2.connection import PREFACE


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
