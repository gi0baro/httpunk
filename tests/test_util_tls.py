"""End-to-end TLS: `httpunk.util.connect` negotiates h2 vs h1 by ALPN over a real
`tonio` TLS loopback (a `trustme`-minted CA + cert), and the auto server serves the
decrypted stream. This is the only test that exercises `TonioBackend.connect_tls`
(ALPN offer + read-back) and the full encrypted round-trip end to end.
"""

import ssl

import pytest
import trustme
from tonio.colored import scope
from tonio.colored.net import open_tcp_listeners
from tonio.colored.net.tls import open_tls_over_tcp_listeners

from httpunk import H1Connection, H2Connection
from httpunk.util import auto, connect


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


def _server_ctx(ca, alpn):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(ctx)
    ctx.set_alpn_protocols(list(alpn))
    return ctx


def _client_ctx(ca):
    ctx = ssl.create_default_context()
    ca.configure_trust(ctx)
    return ctx


async def _echo(server):
    async with server:
        async for req in server:
            body = await req.read()
            await req.respond(200, body=b"tls:" + body)


@pytest.mark.tonio
async def test_https_alpn_negotiates_h2(ca):
    listener = (await open_tls_over_tcp_listeners(0, _server_ctx(ca, ("h2", "http/1.1")), host="127.0.0.1"))[0]
    host, port = listener.transport.socket.getsockname()[:2]

    async def server_side():
        await _echo(await auto.serve(await listener.accept()))

    async with scope() as s:
        s.spawn(server_side())
        conn = await connect(f"https://127.0.0.1:{port}/", ssl_context=_client_ctx(ca), alpn=("h2", "http/1.1"))
        assert isinstance(conn, H2Connection)  # ALPN chose h2 -> the "upgrade"
        async with conn:
            resp = await conn.request("POST", "/", body=b"hi")
            assert await resp.read() == b"tls:hi"


@pytest.mark.tonio
async def test_https_falls_back_to_h1_when_alpn_is_http11(ca):
    # The client offers only http/1.1, so the server selects it -> h1 (the fallback).
    listener = (await open_tls_over_tcp_listeners(0, _server_ctx(ca, ("h2", "http/1.1")), host="127.0.0.1"))[0]
    host, port = listener.transport.socket.getsockname()[:2]

    async def server_side():
        await _echo(await auto.serve(await listener.accept()))

    async with scope() as s:
        s.spawn(server_side())
        conn = await connect(f"https://127.0.0.1:{port}/", ssl_context=_client_ctx(ca), alpn=("http/1.1",))
        assert isinstance(conn, H1Connection)
        async with conn:
            resp = await conn.request("POST", "/", headers={"host": f"127.0.0.1:{port}"}, body=b"hey")
            assert await resp.read() == b"tls:hey"


@pytest.mark.tonio
async def test_http_cleartext_is_h1(ca):
    # No TLS, no ALPN -> h1 over plain TCP (h2c is out of scope, matching hyper-util).
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]

    async def server_side():
        await _echo(await auto.serve(await listener.accept()))

    async with scope() as s:
        s.spawn(server_side())
        conn = await connect(f"http://127.0.0.1:{port}/")
        assert isinstance(conn, H1Connection)
        async with conn:
            resp = await conn.request("POST", "/", headers={"host": f"127.0.0.1:{port}"}, body=b"plain")
            assert await resp.read() == b"tls:plain"
