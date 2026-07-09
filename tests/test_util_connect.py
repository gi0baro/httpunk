"""`httpunk.util.connect` — scheme handling + ALPN negotiation (h2 upgrade / h1
fallback), driven by a stub backend so the routing logic is tested without a real
network. End-to-end TLS negotiation is covered in `test_util_tls.py`.
"""

import pytest

from httpunk import H1Connection, H2Connection
from httpunk._backend.tonio import TonioBackend
from httpunk.util import connect


class _FakeStream:
    """A stand-in connected transport. `connect()` returns the connection
    un-entered, so no IO ever touches this."""


class _StubBackend(TonioBackend):
    """A `TonioBackend` whose dial methods are stubbed (record the call, hand back
    a fake stream + a chosen ALPN); the sync primitives (scope/lock/event) stay
    real so the returned connection can be constructed."""

    def __init__(self, *, selected_alpn=None):
        self._selected_alpn = selected_alpn
        self.tls_calls = []
        self.tcp_calls = []

    async def connect_tls(self, host, port, *, alpn=None, ssl_context=None):
        self.tls_calls.append((host, port, alpn, ssl_context))
        return _FakeStream(), self._selected_alpn

    async def connect_tcp(self, host, port):
        self.tcp_calls.append((host, port))
        return _FakeStream()


@pytest.mark.tonio
async def test_https_h2_alpn_returns_h2_connection():
    backend = _StubBackend(selected_alpn="h2")
    conn = await connect("https://example.com/", backend=backend)
    assert isinstance(conn, H2Connection)
    assert conn._conn.authority == "example.com:443"
    assert backend.tls_calls == [("example.com", 443, ("h2", "http/1.1"), None)]


@pytest.mark.tonio
async def test_https_http11_alpn_returns_h1_connection():
    conn = await connect("https://example.com/", backend=_StubBackend(selected_alpn="http/1.1"))
    assert isinstance(conn, H1Connection)
    assert conn._conn.authority == "example.com:443"


@pytest.mark.tonio
async def test_https_no_alpn_falls_back_to_h1():
    # No ALPN selected by the peer -> h1 fallback (RFC 7301).
    conn = await connect("https://example.com/", backend=_StubBackend(selected_alpn=None))
    assert isinstance(conn, H1Connection)


@pytest.mark.tonio
async def test_http_is_h1_over_plain_tcp():
    backend = _StubBackend()
    conn = await connect("http://example.com:8080/", backend=backend)
    assert isinstance(conn, H1Connection)
    assert conn._conn.authority == "example.com:8080"
    assert backend.tcp_calls == [("example.com", 8080)]
    assert backend.tls_calls == []  # no ALPN / TLS on cleartext


@pytest.mark.tonio
async def test_explicit_port_and_custom_alpn_are_forwarded():
    backend = _StubBackend(selected_alpn="h2")
    conn = await connect("https://h.example:9443/", backend=backend, alpn=("h2",))
    assert isinstance(conn, H2Connection)
    assert conn._conn.authority == "h.example:9443"
    assert backend.tls_calls == [("h.example", 9443, ("h2",), None)]


@pytest.mark.tonio
async def test_rejects_unknown_scheme():
    with pytest.raises(ValueError, match="unsupported scheme"):
        await connect("ftp://example.com/", backend=_StubBackend())


@pytest.mark.tonio
async def test_rejects_url_without_host():
    with pytest.raises(ValueError, match="no host"):
        await connect("https:///path", backend=_StubBackend())
