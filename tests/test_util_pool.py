"""Composable pools (`httpunk.util.pool`): `Singleton` coalesces concurrent gets to
one shared connection (h2), `Cache` checks connections out and back for reuse (h1),
and `Map` routes destinations to lazily-built per-key inner pools. The pool owns each
connection's enter/close lifecycle; the connector hands back an un-entered connection.
"""

import errno

import pytest
from _runtime import cancelled_type
from tonio.colored import scope
from tonio.colored.net import open_tcp_listeners

from httpunk import H1Connection, H2Connection
from httpunk._backend import Backend
from httpunk._backend.tonio import TonioBackend
from httpunk.h1.server import H1Server
from httpunk.h2.server import H2Server
from httpunk.util.pool import Cache, Canceled, Map, Singleton


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


async def _accept_loop(listener, s, serve, accepts=None):
    """Accept until the listener is closed — the test body's `finally` closes it BEFORE
    `s.cancel()`, and a parked accept ends on the closed fd (EBADF). Never leave the
    loop to the cancel alone: a cancel landing on a loop that was woken by a connection
    and has not re-parked yet is delivered only at its next suspension, and after the
    runtime starts tearing down (every still-open fd is shut down, `accept` answers
    EAGAIN forever) that loop has none — it spins, and the runtime waits on it."""
    while True:
        try:
            transport = await listener.accept()
        except OSError as exc:
            if exc.errno == errno.EBADF:
                return  # the listener was closed: the orderly end
            raise
        if accepts is not None:
            accepts["n"] += 1
        s.spawn(serve(transport))


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
        s.spawn(_accept_loop(listener, s, serve))
        try:
            await pool.get()
            assert not pool.is_empty()
            await pool.retain(lambda _conn: True)  # keep
            assert not pool.is_empty()
            await pool.retain(lambda _conn: False)  # evict
            assert pool.is_empty()
        finally:
            listener.close()
            s.cancel()


class _StubConn:
    """A minimal pooled-connection stand-in: an entered/closed lifecycle + a
    settable `closed` liveness flag (what `Singleton.get` checks)."""

    def __init__(self, tag):
        self.tag = tag
        self.closed = False
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *exc):
        self.closed = self.exited = True


class _PendingGet:
    def __init__(self, pool, backend):
        self.pool = pool
        self.scope = backend.scope()
        self.done = backend.event()
        self.conn = self.error = None
        self.finished = False
        self.joined = False

    async def _run(self):
        try:
            self.conn = await self.pool.get()
        except BaseException as exc:
            self.error = exc
        finally:
            self.finished = True
            self.done.set()

    async def __aenter__(self):
        await self.scope.__aenter__()
        self.scope.spawn(self._run())
        return self

    async def cancel(self):
        if not self.joined:
            self.scope.cancel()
            await self.scope.__aexit__(None, None, None)
            # Tonio's scope exit can precede the child's cancellation unwind.
            await self.done.wait()
            self.joined = True

    async def __aexit__(self, *exc):
        await self.cancel()


class _ObservedReady:
    """Observe entry to the round's wait without scheduler sleeps."""

    def __init__(self, pool, backend):
        self.ready = pool._rnd.ready
        self.joined = [backend.event(), backend.event()]
        self.waits = 0
        self.resume = backend.event()
        self.resume.set()
        pool._rnd.ready = self

    async def wait(self):
        index = self.waits
        self.waits += 1
        self.joined[index].set()
        await self.ready.wait()
        await self.resume.wait()

    def set(self):
        self.ready.set()


class _Maker:
    def __init__(self, runtime):
        self.backend = Backend(runtime).create()
        self.started = [self.backend.event(), self.backend.event()]
        self.release = [self.backend.event(), self.backend.event()]
        self.conns = [_StubConn(0), _StubConn(1)]
        self.calls = 0
        self.error = None
        self.pool = Singleton(self, backend=self.backend)

    async def __call__(self, _dst):
        index = self.calls
        self.calls += 1
        self.started[index].set()
        await self.release[index].wait()
        if self.error is not None:
            raise self.error
        return self.conns[index]

    def get(self):
        return _PendingGet(self.pool, self.backend)

    def observe(self):
        return _ObservedReady(self.pool, self.backend)


async def _first_call_drives_subsequent_wait(runtime):
    # hyper-util src/client/pool/singleton.rs:517, first_call_drives_subsequent_wait.
    maker = _Maker(runtime)
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        async with maker.get() as waiter:
            await ready.joined[0].wait()
            assert not driver.finished and not waiter.finished
            assert maker.calls == 1
            maker.release[0].set()
            await driver.done.wait()
            await waiter.done.wait()
            assert driver.error is waiter.error is None
            assert driver.conn is waiter.conn is maker.conns[0]
    await maker.pool.aclose()


async def _made_state_returns_immediately(runtime):
    # hyper-util src/client/pool/singleton.rs:541, made_state_returns_immediately.
    maker = _Maker(runtime)
    maker.release[0].set()
    conn = await maker.pool.get()
    assert await maker.pool.get() is conn
    assert maker.calls == 1
    await maker.pool.aclose()


async def _cached_service_poll_ready_error_clears_singleton(runtime):
    # hyper-util src/client/pool/singleton.rs:560,
    # cached_service_poll_ready_error_clears_singleton. Our readiness signal is
    # conn.closed at get(), not a Service.poll_ready error; replacement is usable.
    maker = _Maker(runtime)
    maker.release[0].set()
    conn = await maker.pool.get()
    conn.closed = True
    maker.release[1].set()
    replacement = await maker.pool.get()
    assert replacement is maker.conns[1] and replacement is not conn
    assert conn.exited
    assert not replacement.closed
    assert await maker.pool.get() is replacement
    assert maker.calls == 2
    await maker.pool.aclose()


async def _cancel_waiter_does_not_affect_others(runtime, *, third=False):
    # hyper-util src/client/pool/singleton.rs:618, cancel_waiter_does_not_affect_others;
    # :754, cancel_non_driver_waiter_does_not_block_others (third=True).
    maker = _Maker(runtime)
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        async with maker.get() as cancelled:
            await ready.joined[0].wait()
            if third:
                async with maker.get() as other:
                    await ready.joined[1].wait()
                    await cancelled.cancel()
                    assert isinstance(cancelled.error, cancelled_type(runtime))
                    maker.release[0].set()
                    await driver.done.wait()
                    await other.done.wait()
                    assert driver.error is other.error is None
                    assert driver.conn is other.conn is maker.conns[0]
            else:
                await cancelled.cancel()
                assert isinstance(cancelled.error, cancelled_type(runtime))
                maker.release[0].set()
                await driver.done.wait()
                assert driver.error is None
                assert driver.conn is maker.conns[0]
        assert maker.calls == 1
    await maker.pool.aclose()


async def _maker_error_is_shared_with_waiters(runtime):
    # hyper-util src/client/pool/singleton.rs:636, maker_error_is_shared_with_waiters.
    # No SingletonError wrapper; Python participants receive equal fresh copies,
    # not upstream's pointer-equal Arc error sources.
    maker = _Maker(runtime)
    maker.error = OSError("maker failed")
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        async with maker.get() as waiter, maker.get() as other:
            await ready.joined[0].wait()
            await ready.joined[1].wait()
            maker.release[0].set()
            for call in (driver, waiter, other):
                await call.done.wait()
                assert type(call.error) is OSError
                assert str(call.error) == "maker failed"
                assert call.error.args == maker.error.args
            assert len({id(call.error) for call in (driver, waiter, other)}) == 3
            assert waiter.error.__cause__ is other.error.__cause__
            assert waiter.error.__cause__.__traceback__ is None
            assert maker.calls == 1
            assert maker.pool.is_empty()


async def _cancel_driver_hands_off_to_waiter(runtime, *, parked=False):
    # hyper-util src/client/pool/singleton.rs:668, cancel_driver_hands_off_to_waiter;
    # :692, cancel_driver_promotes_parked_waiter (parked=True).
    # Restarting on cancellation calls the connector twice, upstream only once.
    # A Python get() registers only when awaited: the unpolled waiter joins after
    # cancellation; the parked variant observes its wait before cancelling.
    maker = _Maker(runtime)
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        if not parked:
            await driver.cancel()
        async with maker.get() as waiter:
            if parked:
                await ready.joined[0].wait()
                assert not waiter.finished
                await driver.cancel()
            assert isinstance(driver.error, cancelled_type(runtime))
            await maker.started[1].wait()
            assert not waiter.finished  # promoted driver is polling the new connect
            maker.release[1].set()
            await waiter.done.wait()
            assert waiter.error is None
            assert waiter.conn is maker.conns[1]
            assert maker.calls == 2
    await maker.pool.aclose()


async def _cancel_all_waiters_clears_singleton(runtime):
    # hyper-util src/client/pool/singleton.rs:737, cancel_all_waiters_clears_singleton.
    maker = _Maker(runtime)
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        async with maker.get() as waiter:
            await ready.joined[0].wait()
            await driver.cancel()
            await waiter.cancel()
            assert isinstance(driver.error, cancelled_type(runtime))
            assert isinstance(waiter.error, cancelled_type(runtime))
            assert maker.pool.is_empty()


@pytest.mark.tonio
async def test_first_call_drives_subsequent_wait_tonio():
    await _first_call_drives_subsequent_wait("tonio")


@pytest.mark.asyncio
async def test_first_call_drives_subsequent_wait_asyncio():
    await _first_call_drives_subsequent_wait("asyncio")


@pytest.mark.tonio
async def test_made_state_returns_immediately_tonio():
    await _made_state_returns_immediately("tonio")


@pytest.mark.asyncio
async def test_made_state_returns_immediately_asyncio():
    await _made_state_returns_immediately("asyncio")


@pytest.mark.tonio
async def test_cached_service_poll_ready_error_clears_singleton_tonio():
    await _cached_service_poll_ready_error_clears_singleton("tonio")


@pytest.mark.asyncio
async def test_cached_service_poll_ready_error_clears_singleton_asyncio():
    await _cached_service_poll_ready_error_clears_singleton("asyncio")


@pytest.mark.tonio
async def test_cancel_waiter_does_not_affect_others_tonio():
    await _cancel_waiter_does_not_affect_others("tonio")


@pytest.mark.asyncio
async def test_cancel_waiter_does_not_affect_others_asyncio():
    await _cancel_waiter_does_not_affect_others("asyncio")


@pytest.mark.tonio
async def test_cancel_non_driver_waiter_does_not_block_others_tonio():
    await _cancel_waiter_does_not_affect_others("tonio", third=True)


@pytest.mark.asyncio
async def test_cancel_non_driver_waiter_does_not_block_others_asyncio():
    await _cancel_waiter_does_not_affect_others("asyncio", third=True)


@pytest.mark.tonio
async def test_maker_error_is_shared_with_waiters_tonio():
    await _maker_error_is_shared_with_waiters("tonio")


@pytest.mark.asyncio
async def test_maker_error_is_shared_with_waiters_asyncio():
    await _maker_error_is_shared_with_waiters("asyncio")


@pytest.mark.tonio
async def test_cancel_driver_hands_off_to_waiter_tonio():
    await _cancel_driver_hands_off_to_waiter("tonio")


@pytest.mark.asyncio
async def test_cancel_driver_hands_off_to_waiter_asyncio():
    await _cancel_driver_hands_off_to_waiter("asyncio")


@pytest.mark.tonio
async def test_cancel_driver_promotes_parked_waiter_tonio():
    await _cancel_driver_hands_off_to_waiter("tonio", parked=True)


@pytest.mark.asyncio
async def test_cancel_driver_promotes_parked_waiter_asyncio():
    await _cancel_driver_hands_off_to_waiter("asyncio", parked=True)


@pytest.mark.tonio
async def test_cancel_all_waiters_clears_singleton_tonio():
    await _cancel_all_waiters_clears_singleton("tonio")


@pytest.mark.asyncio
async def test_cancel_all_waiters_clears_singleton_asyncio():
    await _cancel_all_waiters_clears_singleton("asyncio")


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
        s.spawn(_accept_loop(listener, s, serve, accepts))
        try:
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
        finally:
            listener.close()
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
        s.spawn(_accept_loop(listener, s, serve))
        try:
            with pytest.raises(RuntimeError):
                async with cache.checkout():
                    raise RuntimeError("boom")  # error during use -> connection closed, not reused
            assert cache.is_empty()
        finally:
            listener.close()
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
    http://x and http://x:80 are one destination, not two."""
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
    table so the Map rebuilds lazily on the next pool_for."""
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


# ----- two-party transitions: one step under one lock -----


class _FakeConn:
    """An un-entered connection stand-in: records enter/close, closes on `__aexit__`."""

    def __init__(self):
        self.entered = False
        self.closed = False
        self.busy = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False


@pytest.mark.tonio
async def test_singleton_aclose_during_connect_ditches_the_round():
    """Dropped-round waiters fail without waiting for the connector or reading a
    later round's connection; a late driver closes what it made."""
    maker = _Maker("tonio")
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        async with maker.get() as waiter:
            await ready.joined[0].wait()
            await maker.pool.aclose()
            maker.release[1].set()
            later = await maker.pool.get()
            await waiter.done.wait()
            assert isinstance(waiter.error, Canceled)
            assert waiter.conn is None and not driver.finished
            maker.release[0].set()
            await driver.done.wait()
            assert isinstance(driver.error, Canceled)
            assert maker.conns[0].entered and maker.conns[0].closed
            assert maker.conns[0].exited
            assert await maker.pool.get() is later
    await maker.pool.aclose()
    assert maker.pool.is_empty()


@pytest.mark.tonio
async def test_singleton_waiter_keeps_its_successful_round():
    # Derived from singleton.rs:254-258: a delivered result outlives its batch,
    # even when aclose() and a later connect precede this waiter's resumption.
    maker = _Maker("tonio")
    async with maker.get() as driver:
        await maker.started[0].wait()
        ready = maker.observe()
        ready.resume.clear()
        async with maker.get() as waiter:
            await ready.joined[0].wait()
            maker.release[0].set()
            await driver.done.wait()
            await maker.pool.aclose()
            maker.release[1].set()
            later = await maker.pool.get()
            ready.resume.set()
            await waiter.done.wait()
            assert waiter.error is None
            assert waiter.conn is driver.conn is maker.conns[0]
            assert waiter.conn is not later
    await maker.pool.aclose()


@pytest.mark.tonio
async def test_singleton_retain_evaluates_predicate_outside_the_lock():
    """The predicate is foreign code: it may re-enter the singleton (here `is_empty`)
    without deadlocking, and the eviction is a compare-and-swap on the same connection."""

    async def connector(_dst):
        return _FakeConn()

    pool = Singleton(connector)
    conn = await pool.get()
    seen = []
    await pool.retain(lambda c: seen.append(pool.is_empty()) or False)  # re-enters the pool, then evicts
    assert seen == [False] and conn.closed and pool.is_empty()


@pytest.mark.tonio
async def test_cache_checkin_after_aclose_closes_instead_of_parking():
    """A lease that outlives `aclose()` must not resurrect the pool: its connection is
    closed on return, never parked in a closed cache."""

    async def connector(_dst):
        return _FakeConn()

    cache = Cache(connector)
    async with cache.checkout() as conn:
        await cache.aclose()  # while the lease is out
    assert conn.closed  # closed on return...
    assert cache.is_empty()  # ...not parked


@pytest.mark.tonio
async def test_map_pool_for_after_aclose_refuses_and_factory_runs_unlocked():
    """After `aclose()` (end of life) no pool is built; the factory (foreign code) runs
    outside the lock, so it may consult the map itself."""
    m = Map(lambda _url: (m.is_empty(), _StubPool())[1])  # the factory re-enters the map: no deadlock
    m.pool_for("http://a/")
    await m.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        m.pool_for("http://b/")
