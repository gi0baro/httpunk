"""End-to-end TLS: `httpunk.util.connect` negotiates h2 vs h1 by ALPN over a real
`tonio` TLS loopback (a `trustme`-minted CA + cert), and the auto server serves the
decrypted stream. This is the only test that exercises `TonioBackend.connect_tls`
(ALPN offer + read-back) and the full encrypted round-trip end to end.
"""

import ssl

import pytest
import trustme
from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners
from tonio.colored.net.tls import TLSStream, open_tls_over_tcp_listeners
from tonio.exceptions import ResourceBroken

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


@pytest.mark.tonio
async def test_bounded_reader_over_tls(ca):
    """The bounded read over a `TLSStream`: the raw read beneath the SSL dance carries
    the deadline — a silent peer -> None; the stream keeps working afterwards."""
    listener = (await open_tls_over_tcp_listeners(0, _server_ctx(ca, ("http/1.1",)), host="127.0.0.1"))[0]
    host, port = listener.transport.socket.getsockname()[:2]
    backend = TonioBackend()
    accepted, got = [], Event()

    async def accept():
        accepted.append(await listener.accept())
        got.set()

    async with scope() as s:
        s.spawn(accept())
        client, _ = await backend.connect_tls(host, port, ssl_context=_client_ctx(ca, ("http/1.1",)))
        await got.wait()
        server = accepted[0]
        try:
            read = backend.bounded_reader(server)
            assert await read(100, backend.monotonic() + 0.05) is None
            await client.send_all(b"late")
            assert await server.receive_some(100) == b"late"
            await client.send_all(b"early")
            assert await read(100, backend.monotonic() + 5.0) == b"early"
        finally:
            backend.close_transport(client)
            backend.close_transport(server)
            s.cancel()


# ----- TLS over an existing stream: `wrap_tls` on a CONNECT tunnel -----

_CONNECT_OK = b"HTTP/1.1 200 Connection Established\r\n\r\n"


async def _read_head(stream):
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = await stream.receive_some(65536)
        if not chunk:
            return None
        buf += chunk
    return buf


async def _tunnel_origin(listener, server_ctx, errors):
    """A CONNECT proxy and the `https` origin behind it, in one: answer the CONNECT with
    200, then speak TLS on the SAME TCP connection (that is what a tunnel carries) and
    serve HTTP over it. Errors are recorded, never swallowed (see `_echo`'s caller)."""
    try:
        stream = await listener.accept()
        head = await _read_head(stream)
        assert head is not None and head.startswith(b"CONNECT 127.0.0.1:443 HTTP/1.1\r\n")
        await stream.send_all(_CONNECT_OK)
        tls = TLSStream(stream, server_ctx, server_side=True)
        await tls.handshake()
        server = await auto.serve(tls)
        async with server:
            async for req in server:
                body = await req.read()
                await req.respond(200, body=b"tls:" + body)
    except Exception as exc:
        errors.append(exc)


async def _connect_tunnel(backend, host, port):
    """Dial the proxy, CONNECT, and take the tunnel's IO back out: `(io, read_buf)`."""
    transport = await backend.connect_tcp(host, port)
    async with H1Connection(transport, authority=f"{host}:{port}") as proxy:
        resp = await proxy.request("CONNECT", "127.0.0.1:443", headers={"host": "127.0.0.1:443"})
        assert resp.status == 200 and resp.is_upgrade
        return resp.upgraded.downcast()  # the tunnel outlives the proxy connection object


@pytest.mark.tonio
async def test_wrap_tls_tunnel_alpn_h2(ca):
    """The origin's TLS runs inside the tunnel; ALPN picks h2 exactly as `connect_tls` would."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    errors = []
    backend = TonioBackend()

    async with scope() as s:
        s.spawn(_tunnel_origin(listener, _server_ctx(ca, ("h2", "http/1.1")), errors))
        io, read_buf = await _connect_tunnel(backend, host, port)
        assert read_buf == b""  # a TLS server never speaks before the ClientHello
        stream, selected = await backend.wrap_tls(
            io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca, ("h2", "http/1.1")), prefix=read_buf
        )
        assert selected == "h2"
        async with H2Connection(stream, authority="127.0.0.1:443", scheme="https") as conn:
            resp = await conn.request("POST", "/", body=b"hi")
            assert await resp.read() == b"tls:hi"
    assert not errors


@pytest.mark.tonio
async def test_wrap_tls_tunnel_h1_keep_alive(ca):
    """h1 over the tunnel behaves like a direct `connect_tls` connection: keep-alive reuse
    (the send-time peek before the second request runs on the wrapped stream — the
    `receive_nowait` TLS arm, the very path punkreq's own wrapper broke on) and the
    orderly close ending the server loop cleanly."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    errors = []
    backend = TonioBackend()

    async with scope() as s:
        s.spawn(_tunnel_origin(listener, _server_ctx(ca, ("http/1.1",)), errors))
        io, read_buf = await _connect_tunnel(backend, host, port)
        stream, selected = await backend.wrap_tls(
            io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca, ("http/1.1",)), prefix=read_buf
        )
        assert selected == "http/1.1"
        async with H1Connection(stream, authority="127.0.0.1:443") as conn:
            for body in (b"one", b"two"):
                resp = await conn.request("POST", "/", headers={"host": "127.0.0.1:443"}, body=body)
                assert await resp.read() == b"tls:" + body
    assert not errors  # close_notify from the client ended the server's `async for` cleanly


@pytest.mark.tonio
async def test_wrap_tls_tunnel_close_ends_reach_the_socket(ca):
    """hyper's two ends through the tunnel: the orderly close writes close_notify (the
    origin reads a clean EOF); the abortive `close_transport` closes the socket beneath
    the TLS layer with no alert (the origin's read breaks)."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    ends = []
    backend = TonioBackend()

    async def raw_origin(n):
        for _ in range(n):
            stream = await listener.accept()
            assert await _read_head(stream) is not None  # the CONNECT
            await stream.send_all(_CONNECT_OK)
            tls = TLSStream(stream, _server_ctx(ca, ("http/1.1",)), server_side=True)
            await tls.handshake()
            await tls.receive_some()  # the request head
            await tls.send_all(_H1_OK)
            try:
                ends.append(await tls.receive_some())  # b"" = EOF with close_notify
            except Exception as exc:
                ends.append(exc)
            finally:
                stream.close()

    async with scope() as s:
        s.spawn(raw_origin(2))
        ctx = _client_ctx(ca, ("http/1.1",))
        # 1. the orderly end: `H1Connection.__aexit__` on an idle connection
        io, read_buf = await _connect_tunnel(backend, host, port)
        stream, _ = await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=ctx, prefix=read_buf)
        async with H1Connection(stream) as conn:
            resp = await conn.request("GET", "/", headers={"host": "127.0.0.1:443"})
            assert await resp.read() == b"ok"
        # 2. the abortive end: the backend's sync close (hyper dropping the IO)
        io, read_buf = await _connect_tunnel(backend, host, port)
        stream, _ = await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=ctx, prefix=read_buf)
        await stream.send_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        await stream.receive_some()
        backend.close_transport(stream)
    assert ends[0] == b""
    assert isinstance(ends[1], backend.broken_transport_errors)


@pytest.mark.tonio
async def test_wrap_tls_prefix_feeds_the_handshake(ca):
    """`prefix` is the tunnel's `read_buf`, fed to the TLS layer before the handshake
    (hyper's `Rewind`). The peer sends nothing after the 200, so a handshake that did
    NOT consume the prefix would park waiting for a ServerHello; one that did fails on
    it at once (not a TLS record) — before the ClientHello ever leaves the egress BIO.
    The failure closes the socket: all the peer reads is EOF."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    ends = []
    backend = TonioBackend()

    async def silent_proxy():
        stream = await listener.accept()
        assert await _read_head(stream) is not None
        await stream.send_all(_CONNECT_OK)
        ends.append(await stream.receive_some())  # the ClientHello would come here; only the close does
        stream.close()

    async with scope() as s:
        s.spawn(silent_proxy())
        io, read_buf = await _connect_tunnel(backend, host, port)
        assert read_buf == b""
        with pytest.raises(ResourceBroken) as info:
            await backend.wrap_tls(
                io, server_hostname="127.0.0.1", ssl_context=_client_ctx(ca, ("h2",)), prefix=b"HTTP/1.1 200 OK\r\n"
            )
        assert isinstance(info.value.__cause__, ssl.SSLError)
    assert ends == [b""]  # the socket closed: nothing to clean up


@pytest.mark.tonio
async def test_wrap_tls_handshake_failure_closes_the_socket(ca):
    """A refused certificate: `connect_tls`'s error (`ResourceBroken` from the ssl
    error), and the socket beneath is closed before it propagates — the caller has no
    half-wrapped stream to tear down. The origin sees the alert, then EOF."""
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    ends = []
    backend = TonioBackend()

    async def origin():
        stream = await listener.accept()
        assert await _read_head(stream) is not None
        await stream.send_all(_CONNECT_OK)
        tls = TLSStream(stream, _server_ctx(ca, ("http/1.1",)), server_side=True)
        try:
            await tls.handshake()
        except Exception as exc:
            ends.append(exc)  # the client's alert
        ends.append(await stream.receive_some())  # then the raw socket reads EOF
        stream.close()

    async with scope() as s:
        s.spawn(origin())
        io, read_buf = await _connect_tunnel(backend, host, port)
        untrusting = ssl.create_default_context()  # no `ca.configure_trust`: verification fails
        untrusting.set_alpn_protocols(["http/1.1"])
        with pytest.raises(ResourceBroken) as info:
            await backend.wrap_tls(io, server_hostname="127.0.0.1", ssl_context=untrusting, prefix=read_buf)
        assert isinstance(info.value.__cause__, ssl.SSLCertVerificationError)
    assert isinstance(ends[0], ResourceBroken) and ends[1] == b""


@pytest.mark.tonio
async def test_wrap_tls_refuses_anything_but_a_socket_stream():
    """tonio's `TLSStream` is designed over a socket stream only: TLS over TLS (a tunnel
    through an `https://` proxy) is refused at setup, not left to fail on a close."""
    with pytest.raises(TypeError, match="SocketStream"):
        await TonioBackend().wrap_tls(object(), server_hostname="127.0.0.1")
