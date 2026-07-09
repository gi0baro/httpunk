"""Composable pools (`httpunk.util.pool`): `Singleton` coalesces concurrent gets to
one shared connection (h2), `Cache` checks connections out and back for reuse (h1),
and `Map` routes destinations to lazily-built per-key inner pools. The pool owns each
connection's enter/close lifecycle; the connector hands back an un-entered connection.
"""

import pytest
from tonio.colored import scope
from tonio.colored.net import open_tcp_listeners

from httpunk import H1Connection, H2Connection
from httpunk._backend.tonio import TonioBackend
from httpunk.h1.server import H1Server
from httpunk.h2.server import H2Server
from httpunk.util.pool import Cache, Map, Singleton


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


@pytest.mark.tonio
async def test_singleton_coalesces_concurrent_gets_into_one_connection():
    listener, host, port = await _listener()
    accepts = {"n": 0}

    async def connector(_dst):
        transport = await TonioBackend().connect_tcp(host, port)
        return H2Connection(transport, authority=f"{host}:{port}")

    pool = Singleton(connector)

    async def serve(transport):
        async with H2Server(transport) as server:
            async for req in server:
                await req.respond(200, body=b"ok")

    async with scope() as s:

        async def accept_loop():
            while True:
                transport = await listener.accept()
                accepts["n"] += 1
                s.spawn(serve(transport))

        s.spawn(accept_loop())

        # Two concurrent gets: one drives the connect, the other coalesces onto it.
        got = {}

        async def get(i):
            got[i] = await pool.get()

        async with scope() as gs:
            gs.spawn(get(1))
            gs.spawn(get(2))

        assert got[1] is got[2]  # same shared connection
        resp = await got[1].request("GET", "/")
        assert await resp.read() == b"ok"
        assert accepts["n"] == 1  # only one connection was ever made
        assert not pool.is_empty()

        await pool.aclose()
        assert pool.is_empty()
        s.cancel()


@pytest.mark.tonio
async def test_singleton_retain_drops_connection_when_predicate_false():
    listener, host, port = await _listener()

    async def connector(_dst):
        transport = await TonioBackend().connect_tcp(host, port)
        return H2Connection(transport, authority=f"{host}:{port}")

    pool = Singleton(connector)

    async def serve(transport):
        async with H2Server(transport) as server:
            async for req in server:
                await req.respond(200, body=b"ok")

    async with scope() as s:

        async def accept_loop():
            while True:
                s.spawn(serve(await listener.accept()))

        s.spawn(accept_loop())
        await pool.get()
        assert not pool.is_empty()
        await pool.retain(lambda _conn: True)  # keep
        assert not pool.is_empty()
        await pool.retain(lambda _conn: False)  # evict
        assert pool.is_empty()
        s.cancel()


class _StubConn:
    """A minimal pooled-connection stand-in: an entered/closed lifecycle + a
    settable `closed` liveness flag (what `Singleton.get` checks)."""

    def __init__(self, tag):
        self.tag = tag
        self.closed = False
        self.exited = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.exited = True


@pytest.mark.tonio
async def test_singleton_reconnects_when_shared_connection_dies():
    """A dead shared connection is auto-evicted and replaced on the next get()
    (hyper-util `Singled::poll_ready` resets a closed service to Empty) — F35;
    previously `get()` handed back the corpse forever."""
    conns = []

    async def connector(_dst):
        conns.append(_StubConn(len(conns)))
        return conns[-1]

    pool = Singleton(connector)
    a = await pool.get()
    assert a is conns[0]
    assert await pool.get() is a  # still alive -> the same shared connection

    a.closed = True  # the shared connection dies
    b = await pool.get()
    assert b is conns[1] and b is not a  # auto-evicted and reconnected
    assert a.exited  # the dead connection was closed
    await pool.aclose()


@pytest.mark.tonio
async def test_cache_reuses_idle_connection():
    listener, host, port = await _listener()
    accepts = {"n": 0}

    async def connector(_dst):
        transport = await TonioBackend().connect_tcp(host, port)
        return H1Connection(transport, authority=f"{host}:{port}")

    cache = Cache(connector)

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                await req.respond(200, body=b"ok")

    async with scope() as s:

        async def accept_loop():
            while True:
                transport = await listener.accept()
                accepts["n"] += 1
                s.spawn(serve(transport))

        s.spawn(accept_loop())

        async with cache.checkout() as c1:
            resp = await c1.request("GET", "/", headers={"host": host})
            assert await resp.read() == b"ok"
        async with cache.checkout() as c2:
            resp = await c2.request("GET", "/", headers={"host": host})
            assert await resp.read() == b"ok"

        assert c1 is c2  # the second checkout reused the idle connection
        assert accepts["n"] == 1  # so only one TCP connection was ever opened
        assert not cache.is_empty()

        await cache.aclose()
        assert cache.is_empty()
        s.cancel()


@pytest.mark.tonio
async def test_cache_closes_connection_on_exception_instead_of_reusing():
    listener, host, port = await _listener()

    async def connector(_dst):
        transport = await TonioBackend().connect_tcp(host, port)
        return H1Connection(transport, authority=f"{host}:{port}")

    cache = Cache(connector)

    async def serve(transport):
        async with H1Server(transport) as server:
            async for req in server:
                await req.read()
                await req.respond(200, body=b"ok")

    async with scope() as s:

        async def accept_loop():
            while True:
                s.spawn(serve(await listener.accept()))

        s.spawn(accept_loop())

        with pytest.raises(RuntimeError):
            async with cache.checkout():
                raise RuntimeError("boom")  # error during use -> connection closed, not reused
        assert cache.is_empty()
        s.cancel()


@pytest.mark.tonio
async def test_map_builds_one_pool_per_key_and_closes_all():
    built = []

    def make_pool(url):
        built.append(url)
        return Singleton(lambda _dst: None)  # connector never called in this test

    m = Map(make_pool)
    assert m.is_empty()

    p1 = m.pool_for("http://a.example/x")
    p1_again = m.pool_for("http://a.example/y")  # same (scheme, host, port) -> same pool
    p2 = m.pool_for("http://b.example/")  # different key -> different pool
    p3 = m.pool_for("http://a.example:8080/")  # port is part of the key -> different pool

    assert p1 is p1_again
    assert p1 is not p2
    assert p1 is not p3
    assert built == ["http://a.example/x", "http://b.example/", "http://a.example:8080/"]
    assert not m.is_empty()

    await m.aclose()  # closes every inner pool
    assert m.is_empty()


@pytest.mark.tonio
async def test_map_normalizes_default_port_from_scheme():
    """A URL with no explicit port routes to the SAME pool as its scheme-default port —
    http://x and http://x:80 are one destination, not two (F52)."""
    built = []

    def make_pool(url):
        built.append(url)
        return Singleton(lambda _dst: None)

    m = Map(make_pool)
    p_bare = m.pool_for("http://a.example/x")  # implicit port 80
    p_80 = m.pool_for("http://a.example:80/y")  # explicit 80 -> same key
    p_https = m.pool_for("https://a.example/")  # implicit 443
    p_443 = m.pool_for("https://a.example:443/")  # explicit 443 -> same key
    assert p_bare is p_80
    assert p_https is p_443
    assert p_bare is not p_https  # different scheme + default port
    assert built == ["http://a.example/x", "https://a.example/"]  # only two pools built
    await m.aclose()


class _StubPool:
    """Records retain/aclose so a Map test can assert forwarding."""

    def __init__(self):
        self.retained = _StubPool  # sentinel: retain not called
        self.closed = False

    async def retain(self, predicate):
        self.retained = predicate

    async def aclose(self):
        self.closed = True


@pytest.mark.tonio
async def test_map_retain_forwards_and_clear_resets():
    """Map.retain forwards to every inner pool; clear() closes them and drops the routing
    table so the Map rebuilds lazily on the next pool_for (F52)."""
    pools = []

    def make_pool(_url):
        pools.append(_StubPool())
        return pools[-1]

    m = Map(make_pool)
    m.pool_for("http://a/")
    m.pool_for("http://b/")

    def pred(_conn):
        return True

    await m.retain(pred)
    assert all(p.retained is pred for p in pools)  # forwarded to each inner pool

    await m.clear()
    assert all(p.closed for p in pools)  # clear() closed them
    assert m.is_empty()  # and reset the routing table
    m.pool_for("http://c/")  # still usable — rebuilt lazily
    assert not m.is_empty()
