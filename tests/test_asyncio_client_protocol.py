"""The reusable asyncio CLIENT protocols (`httpunk.asyncio.{H1,H2,Auto}ClientProtocol`)
— the mirror of the server protocols. A host dials with `loop.create_connection(factory)`
and, once connected, `await proto.ready()` returns the httpunk client connection to send
requests on. Here the "host" is the test loop and the server is httpunk's own reusable
server protocol on the asyncio backend.
"""

import asyncio
import ssl

import pytest
import trustme

from httpunk import H1Connection, H2Connection
from httpunk.asyncio import (
    AutoClientProtocol,
    H1ClientProtocol,
    H1ServerProtocol,
    H2ClientProtocol,
    H2ServerProtocol,
)


class _EchoH2(H2ServerProtocol):
    async def handle(self, request):
        body = await request.read()
        await request.respond(200, body=b"h2:" + request.path.encode() + b":" + body)


class _EchoH1(H1ServerProtocol):
    async def handle(self, request):
        body = await request.read()
        await request.respond(200, body=b"h1:" + body)


async def _server(protocol_cls, *, ssl_ctx=None):
    """`create_server(protocol_cls)` on loopback, capturing the per-connection server
    protocol instances so a test can join their serve task for a clean teardown.
    Returns `(server, host, port, protocols)`."""
    loop = asyncio.get_running_loop()
    protocols = []

    def factory():
        p = protocol_cls()
        protocols.append(p)
        return p

    server = await loop.create_server(factory, "127.0.0.1", 0, ssl=ssl_ctx)
    host, port = server.sockets[0].getsockname()[:2]
    return server, host, port, protocols


@pytest.mark.asyncio
async def test_h2_client_protocol_roundtrip():
    server, host, port, sprotos = await _server(_EchoH2)
    loop = asyncio.get_running_loop()
    async with server:
        _transport, proto = await loop.create_connection(
            lambda: H2ClientProtocol(authority=f"{host}:{port}"), host, port
        )
        conn = await proto.ready()
        assert isinstance(conn, H2Connection)  # ready() hands back the facade
        resp = await conn.request("POST", "/x", body=b"hi")
        assert await resp.read() == b"h2:/x:hi"
        await proto.aclose()
        await sprotos[0]._serve_task  # client closed -> server serve loop ends


@pytest.mark.asyncio
async def test_h2_client_protocol_multiplexed_concurrently():
    server, host, port, sprotos = await _server(_EchoH2)
    loop = asyncio.get_running_loop()
    async with server:
        _transport, proto = await loop.create_connection(
            lambda: H2ClientProtocol(authority=f"{host}:{port}"), host, port
        )
        conn = await proto.ready()
        r1, r2 = await asyncio.gather(conn.request("GET", "/a"), conn.request("GET", "/b"))
        b1, b2 = await asyncio.gather(r1.read(), r2.read())
        assert {b1, b2} == {b"h2:/a:", b"h2:/b:"}
        await proto.aclose()
        await sprotos[0]._serve_task


@pytest.mark.asyncio
async def test_h1_client_protocol_roundtrip():
    server, host, port, sprotos = await _server(_EchoH1)
    loop = asyncio.get_running_loop()
    async with server:
        _transport, proto = await loop.create_connection(
            lambda: H1ClientProtocol(authority=f"{host}:{port}"), host, port
        )
        conn = await proto.ready()
        assert isinstance(conn, H1Connection)
        # Low-level h1: the caller supplies Host (not auto-added).
        resp = await conn.request("POST", "/y", headers={"host": host}, body=b"hey")
        assert await resp.read() == b"h1:hey"
        await proto.aclose()
        await sprotos[0]._serve_task


@pytest.mark.asyncio
async def test_auto_client_protocol_falls_back_to_h1_on_plain_tcp():
    # No TLS -> no ALPN -> AutoClientProtocol picks HTTP/1 (matching util.connect).
    server, host, port, sprotos = await _server(_EchoH1)
    loop = asyncio.get_running_loop()
    async with server:
        _transport, proto = await loop.create_connection(
            lambda: AutoClientProtocol(authority=f"{host}:{port}"), host, port
        )
        conn = await proto.ready()
        assert isinstance(conn, H1Connection)
        resp = await conn.request("GET", "/z", headers={"host": host})
        assert await resp.read() == b"h1:"
        await proto.aclose()
        await sprotos[0]._serve_task


@pytest.mark.asyncio
async def test_client_protocol_aclose_before_connect_is_noop():
    # aclose() before connection_made (never dialed) must be a harmless no-op.
    proto = H2ClientProtocol(authority="example.com:443")
    await proto.aclose()


# ----- TLS + ALPN (AutoClientProtocol negotiates h2) -----


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


def _server_ctx(ca):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(ctx)
    ctx.set_alpn_protocols(["h2", "http/1.1"])
    return ctx


def _client_ctx(ca):
    ctx = ssl.create_default_context()
    ca.configure_trust(ctx)
    ctx.set_alpn_protocols(["h2", "http/1.1"])
    return ctx


@pytest.mark.asyncio
async def test_auto_client_protocol_negotiates_h2_over_tls(ca):
    server, host, port, sprotos = await _server(_EchoH2, ssl_ctx=_server_ctx(ca))
    loop = asyncio.get_running_loop()
    async with server:
        _transport, proto = await loop.create_connection(
            lambda: AutoClientProtocol(authority=f"{host}:{port}", scheme="https"),
            host,
            port,
            ssl=_client_ctx(ca),
            server_hostname="127.0.0.1",
        )
        conn = await proto.ready()
        assert isinstance(conn, H2Connection)  # ALPN chose h2 over TLS
        resp = await conn.request("GET", "/x")
        assert await resp.read() == b"h2:/x:"
        await proto.aclose()
        await sprotos[0]._serve_task
