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
from httpunk._backend.tonio import TonioBackend
from httpunk.util import auto, connect


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


def _server_ctx(ca, alpn):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1").configure_cert(ctx)
    ctx.set_alpn_protocols(list(alpn))
    return ctx


def _client_ctx(ca, alpn):
    # A caller-supplied context is never mutated by `connect()`: it carries its own ALPN offer.
    ctx = ssl.create_default_context()
    ca.configure_trust(ctx)
    ctx.set_alpn_protocols(list(alpn))
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
        # Echo the request's :scheme so the client can assert it. F1 regression
        # guard: a TLS-dialed h2 connection must carry :scheme=https, not the old
        # hardcoded "http".
        server = await auto.serve(await listener.accept())
        async with server:
            async for req in server:
                body = await req.read()
                await req.respond(200, body=f"{req.scheme}:".encode() + body)

    async with scope() as s:
        s.spawn(server_side())
        conn = await connect(f"https://127.0.0.1:{port}/", ssl_context=_client_ctx(ca, ("h2", "http/1.1")))
        assert isinstance(conn, H2Connection)  # ALPN chose h2 -> the "upgrade"
        async with conn:
            resp = await conn.request("POST", "/", body=b"hi")
            assert await resp.read() == b"https:hi"  # :scheme=https, not "http"


@pytest.mark.tonio
async def test_https_falls_back_to_h1_when_alpn_is_http11(ca):
    # The client offers only http/1.1, so the server selects it -> h1 (the fallback).
    listener = (await open_tls_over_tcp_listeners(0, _server_ctx(ca, ("h2", "http/1.1")), host="127.0.0.1"))[0]
    host, port = listener.transport.socket.getsockname()[:2]
    server_errors = []

    async def server_side():
        # Recorded, not spawned-and-forgotten: the client's context exit closes its
        # idle TLS connection the orderly way (close_notify, hyper's `poll_shutdown`),
        # and the server must treat that as the connection ending (clean `async for`
        # exit) rather than die. A swallowed crash here once let this test pass for
        # the wrong reason.
        try:
            await _echo(await auto.serve(await listener.accept()))
        except Exception as exc:
            server_errors.append(exc)

    async with scope() as s:
        s.spawn(server_side())
        conn = await connect(f"https://127.0.0.1:{port}/", ssl_context=_client_ctx(ca, ("http/1.1",)))
        assert isinstance(conn, H1Connection)
        async with conn:
            resp = await conn.request("POST", "/", headers={"host": f"127.0.0.1:{port}"}, body=b"hey")
            assert await resp.read() == b"tls:hey"
    assert not server_errors  # the client's close ended the server loop cleanly


_H1_OK = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"


@pytest.mark.tonio
async def test_h1_client_orderly_close_sends_close_notify(ca):
    """hyper's two ends, on the wire. An idle h1 connection closed by the caller is the
    `Connection` future completing -> `poll_shutdown` -> tokio-rustls writes
    close_notify: tonio's server-side TLS read then ends with a CLEAN EOF (`b""`).
    The abortive close (`close_transport`, hyper dropping the IO) sends no alert:
    that read raises the backend's broken-transport error instead."""
    listener = (await open_tls_over_tcp_listeners(0, _server_ctx(ca, ("http/1.1",)), host="127.0.0.1"))[0]
    host, port = listener.transport.socket.getsockname()[:2]
    ends = []

    async def raw_server(n):
        for _ in range(n):
            stream = await listener.accept()
            await stream.receive_some()  # the request head
            await stream.send_all(_H1_OK)
            try:
                ends.append(await stream.receive_some())  # b"" = EOF with close_notify
            except Exception as exc:
                ends.append(exc)
            finally:
                stream.transport.close()

    async with scope() as s:
        s.spawn(raw_server(2))
        backend = TonioBackend()
        # 1. the orderly end: `H1Connection.__aexit__` on an idle connection
        stream, _ = await backend.connect_tls(host, port, ssl_context=_client_ctx(ca, ("http/1.1",)))
        async with H1Connection(stream) as conn:
            resp = await conn.request("GET", "/", headers={"host": host})
            assert await resp.read() == b"ok"
        # 2. the abortive end: the backend's sync close (hyper dropping the IO)
        stream, _ = await backend.connect_tls(host, port, ssl_context=_client_ctx(ca, ("http/1.1",)))
        await stream.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        await stream.receive_some()
        backend.close_transport(stream)
    assert ends[0] == b""  # close_notify: a clean TLS EOF
    assert isinstance(ends[1], backend.broken_transport_errors)  # no alert: a torn-down transport


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
