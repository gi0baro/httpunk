"""Phase 1 end-to-end: an HTTP/2 (h2c, cleartext) GET over a real tonio TCP
loopback. Exercises serialize + framing + HPACK + the connection driver +
sockets — everything except TLS (Phase 4). The server side is built inline from
the server-role H2Codec (not part of the library yet)."""

import pytest
from _client import open_h2
from tonio.colored import scope
from tonio.colored.net import open_tcp_listeners

from httpunk._httpunk import (
    H2Codec,
    H2FrameData as Data,
    H2FrameHeaders as Headers,
    H2FrameSettings as Settings,
)
from httpunk.h2.connection import PREFACE
from httpunk.http import HeaderMap


async def _serve_one_h2c(listener, *, status=200, body=b"hello h2"):
    """Accept one connection, complete the h2 handshake, read a single request,
    and reply with `status` + `body`."""
    stream = await listener.accept()
    codec = H2Codec("server")

    # The server's connection preface is just its SETTINGS frame.
    await stream.send_all(codec.serialize_settings())

    # Read + strip the 24-byte client preface, then frame the rest.
    raw = b""
    while len(raw) < len(PREFACE):
        chunk = await stream.receive_some(65536)
        if not chunk:
            return
        raw += chunk
    assert raw[: len(PREFACE)] == PREFACE

    request_sid = None
    request_done = False

    async def handle(frames):
        nonlocal request_sid, request_done
        for frame in frames:
            if isinstance(frame, Settings) and not frame.ack:
                await stream.send_all(codec.serialize_settings_ack())
            elif isinstance(frame, Headers):
                request_sid = frame.stream_id
                # A bodyless request carries END_STREAM on HEADERS (no trailing
                # empty DATA frame), matching hyper/h2's `send_request`.
                if frame.end_stream:
                    request_done = True
            elif isinstance(frame, Data) and frame.end_stream:
                request_done = True

    await handle(codec.receive(raw[len(PREFACE) :]))
    while not request_done:
        chunk = await stream.receive_some(65536)
        if not chunk:
            break
        await handle(codec.receive(chunk))

    reply = codec.serialize_response_headers(request_sid, status, HeaderMap([("content-type", b"text/plain")]))
    reply += codec.serialize_data(request_sid, body, end_stream=True)
    await stream.send_all(reply)

    # Drain until the client goes away, so we don't close with the client's
    # in-flight SETTINGS-ack still unread (which would send an RST).
    while await stream.receive_some(65536):
        pass


@pytest.mark.tonio
async def test_h2c_get_loopback():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async with scope() as s:
        s.spawn(_serve_one_h2c(listener, status=200, body=b"hello h2"))

        async with open_h2(host, port) as conn:
            resp = await conn.get("/")
            body = await resp.read()

        s.cancel()

    assert resp.status == 200
    assert body == b"hello h2"
    assert resp.headers["content-type"] == b"text/plain"
