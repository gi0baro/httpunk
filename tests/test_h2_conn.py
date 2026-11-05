"""Phase 1 end-to-end: an HTTP/2 (h2c, cleartext) GET over a real tonio TCP
loopback. Exercises serialize + framing + HPACK + the connection driver +
sockets — everything except TLS (Phase 4). The server side is a raw server-role
`H2Peer`."""

import pytest
from _net import H2Peer, listen, open_h2, read_to_eof
from tonio.colored import scope

from httpunk import Version
from httpunk.http import HeaderMap


async def _serve_one_h2c(listener, *, status=200, body=b"hello h2"):
    """Accept one connection, complete the h2 handshake, read a single request,
    and reply with `status` + `body`."""
    peer = await H2Peer.accept(listener)
    sid = (await peer.request()).stream_id
    await peer.send(
        peer.codec.serialize_response_headers(sid, status, HeaderMap([("content-type", b"text/plain")])),
        peer.codec.serialize_data(sid, body, end_stream=True),
    )

    # Drain until the client goes away, so we don't close with the client's
    # in-flight SETTINGS-ack still unread (which would send an RST).
    await read_to_eof(peer.stream)


@pytest.mark.tonio
async def test_h2c_get_loopback():
    listener, host, port = await listen()

    async with scope() as s:
        s.spawn(_serve_one_h2c(listener, status=200, body=b"hello h2"))

        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")
            body = await resp.read()

        s.cancel()

    assert resp.status == 200
    assert resp.version is Version.HTTP_2  # h2 stamps HTTP_2 on every response (hyper `Response::version`)
    assert body == b"hello h2"
    assert resp.headers["content-type"] == b"text/plain"
