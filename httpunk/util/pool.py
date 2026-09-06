"""Composable connection pools — `httpunk.util`'s analogue of hyper-util's
`client::pool::{singleton, cache, map}`.

Connection reuse is inherently runtime-bound (it manages live connections), so —
unlike the vendored proxy matcher — it lives in Python. hyper-util layers these as
tower `Service`s; we keep the same *concepts and names* as concrete helpers, with
**no** `Service`/`MakeService` abstraction:

- `Singleton` — coalesce concurrent connects to **one shared** connection (the HTTP/2
  case: one multiplexed connection for all callers).
- `Cache` — a set of idle connections, checked out and returned for reuse (the HTTP/1
  case: one request at a time per connection).
- `Map` — route by destination `(scheme, host, port)` to a per-key inner pool, built
  lazily.

**Lifecycle contract.** A `connector` is an async callable `connector(dst) ->
connection` returning an **un-entered** `H1/H2Connection` (typically
`lambda url: util.connect(url)`). The pool owns the connection's lifetime: it enters
it (`__aenter__` — the HTTP handshake) on create and closes it (`__aexit__`) on
eviction. Callers of the pool never enter/close a pooled connection — they just send
requests on it. Liveness is checked at *use* time (a request on a dead connection
raises, as with any pool); `retain()` is the eviction hook for stale connections.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from .. import _backend
from .._httpunk import uri_parts


class Canceled(Exception):  # noqa: N818 - `Canceled` is hyper-util's exact name (singleton.rs)
    """A `Singleton` waiter's coalesced connection attempt was ditched because the
    in-flight connect (driven by another caller) failed. Retry by calling again
    (mirrors hyper-util's singleton `Canceled`)."""


class Singleton:
    """Shares a single connection across all callers, coalescing concurrent
    `get()`s onto one connect — the HTTP/2 pattern (one multiplexed connection).
    Mirrors hyper-util `pool::singleton::Singleton` (State: empty → making → made).
    """

    def __init__(
        self, connector: Callable[..., Awaitable[Any]], *, backend: _backend.BackendLike | None = None
    ) -> None:
        self._connector = connector
        self._backend = _backend.resolve(backend)
        # Guards the state machine: every transition is ONE step under it, never held
        # across an await, and never around foreign code (the connector, a predicate).
        self._lock = threading.Lock()
        self._state = "empty"  # empty | making | made
        self._conn = None
        self._ready = None  # event signalling the current making round is done
        # The making round's id: `aclose()`/`retain()` during a connect bump it, so a
        # driver whose round was ditched closes the connection it made instead of
        # installing it (and its waiters get `Canceled`) — an in-flight connect can
        # never resurrect a closed singleton or leak a second connection.
        self._round = 0

    async def get(self, dst: Any = None) -> Any:
        """The shared connection, connecting once. Concurrent callers during the
        connect wait for it; if that connect fails, the driver raises the real error
        and the waiters get `Canceled`."""
        stale = None
        with self._lock:
            if self._state == "made":
                if not self._conn.closed:
                    return self._conn
                # The shared connection died (driver failed / GOAWAY). Ditch it and
                # reconnect — mirroring hyper-util `Singled::poll_ready` resetting a closed
                # service to Empty (F35); otherwise every `get()` would hand back the
                # corpse. We become the making-driver below and close the dead one first.
                stale, self._conn, self._state = self._conn, None, "empty"
            if self._state == "empty":
                self._state = "making"
                self._round += 1
                self._ready = self._backend.event()
                ready, driver, rnd = self._ready, True, self._round
            else:  # making — wait for the driver
                ready, driver, rnd = self._ready, False, self._round

        if stale is not None:
            with contextlib.suppress(Exception):
                await stale.__aexit__(None, None, None)  # close the dead connection

        if driver:
            try:
                conn = await self._connector(dst)
                await conn.__aenter__()  # HTTP handshake — the pool owns the lifetime
            except BaseException:
                with self._lock:
                    if self._round == rnd and self._state == "making":
                        self._state = "empty"  # ditch the round so the next get() retries
                ready.set()
                raise
            with self._lock:
                installed = self._round == rnd and self._state == "making"
                if installed:
                    self._conn, self._state = conn, "made"
            ready.set()
            if not installed:
                # `aclose()`/`retain()` ditched this round mid-connect: the singleton is
                # closed (or reset), so the fresh connection must not be installed.
                with contextlib.suppress(Exception):
                    await conn.__aexit__(None, None, None)
                raise Canceled("the singleton was closed while this call was connecting")
            return conn

        await ready.wait()
        with self._lock:
            if self._state == "made":
                return self._conn
        raise Canceled("the connection attempt this call was waiting on failed")

    async def retain(self, predicate: Callable[[Any], bool]) -> None:
        """Drop (and close) the shared connection if `predicate(conn)` is False —
        the eviction hook for a dead/stale connection. No-op while empty/making.
        The predicate (foreign code) runs OUTSIDE the lock; the eviction is then a
        compare-and-swap on the same connection."""
        with self._lock:
            conn = self._conn if self._state == "made" else None
        if conn is None or predicate(conn):
            return
        with self._lock:
            if self._conn is conn:
                self._conn, self._state = None, "empty"
                self._round += 1  # a concurrent connect round, if any, is ditched too
            else:
                conn = None
        if conn is not None:
            await conn.__aexit__(None, None, None)

    def is_empty(self) -> bool:
        """True iff no connection has been made (or is being made)."""
        with self._lock:
            return self._state == "empty"

    async def aclose(self) -> None:
        """Close the shared connection and reset to empty. A connect in flight is
        ditched: its driver closes what it made and raises `Canceled`."""
        with self._lock:
            conn, self._conn, self._state = self._conn, None, "empty"
            self._round += 1
        if conn is not None:
            await conn.__aexit__(None, None, None)


class Cache:
    """A set of idle connections reused via `checkout()` — the HTTP/1 pattern (one
    request at a time per connection). Mirrors hyper-util `pool::cache::Cache`: a
    checkout hands back an idle connection (or makes one) and, on release, returns
    it to the idle set. Release is a lease context manager (the Python stand-in for
    hyper-util's drop-returns-to-cache)."""

    def __init__(
        self, connector: Callable[..., Awaitable[Any]], *, backend: _backend.BackendLike | None = None
    ) -> None:
        self._connector = connector
        self._backend = _backend.resolve(backend)
        self._lock = threading.Lock()  # the idle set + the closed flag: one step each
        self._idle = []
        # `aclose()` happened: a connection checked in afterwards is closed, not parked
        # (the pool can never be resurrected by a lease that outlived it).
        self._closed = False

    def checkout(self, dst: Any = None) -> _Lease:
        """A lease over a connection: `async with cache.checkout(dst) as conn: ...`.
        On a clean exit the connection returns to the idle set for reuse; if the body
        raised, it is closed instead (a failed exchange may have left it unusable)."""
        return _Lease(self, dst)

    async def _acquire(self, dst):
        with self._lock:
            conn = self._idle.pop() if self._idle else None
        if conn is not None:
            return conn
        # Deliberate simplification vs hyper-util (F50, documented WON'T-FIX): on an
        # empty pool we connect straight away rather than registering a waiter that
        # RACES an in-flight connection's return-to-pool (hyper-util `pool::Checkout` +
        # the `waiters` queue in `put`). A faithful race needs a detached connect that
        # runs to completion and is pooled if it loses (cancelling it mid-handshake
        # would leak a half-open socket), plus Client-level connection-establishment
        # coordination this low-level `Cache` doesn't own. It also barely reduces the
        # connection count here: HTTP/1 is one-request-per-connection, so N concurrent
        # checkouts genuinely need N connections. Connecting is always safe and serves
        # the checkout promptly; the only cost is possibly more idle connections under
        # bursty contention, which `retain()`/`aclose()` reclaim.
        conn = await self._connector(dst)
        await conn.__aenter__()  # HTTP handshake — the pool owns the lifetime
        return conn

    def _checkin(self, conn):
        """Park `conn` in the idle set; `False` if the cache is closed (the lease
        closes it instead) — decided in the same step as the park."""
        with self._lock:
            if self._closed:
                return False
            self._idle.append(conn)
            return True

    async def retain(self, predicate: Callable[[Any], bool]) -> None:
        """Keep only the idle connections `predicate(conn)` returns True for; close
        the rest. The eviction hook for idle/stale connections. The predicate
        (foreign code) runs outside the lock over a snapshot; a connection checked
        out meanwhile is simply no longer there to drop."""
        with self._lock:
            snapshot = list(self._idle)
        rejected = [conn for conn in snapshot if not predicate(conn)]
        if not rejected:
            return
        with self._lock:
            drop = [conn for conn in self._idle if any(conn is r for r in rejected)]
            self._idle = [conn for conn in self._idle if not any(conn is r for r in rejected)]
        for conn in drop:
            await conn.__aexit__(None, None, None)

    def is_empty(self) -> bool:
        """True iff no idle connections are cached."""
        with self._lock:
            return not self._idle

    async def aclose(self) -> None:
        """Close every idle connection. Leases still out finish their exchange; their
        connections are closed on return instead of parked."""
        with self._lock:
            conns, self._idle = self._idle, []
            self._closed = True
        for conn in conns:
            await conn.__aexit__(None, None, None)


class _Lease:
    """The `Cache.checkout` context manager (see `Cache.checkout`)."""

    def __init__(self, cache, dst):
        self._cache = cache
        self._dst = dst
        self._conn = None

    async def __aenter__(self):
        self._conn = await self._cache._acquire(self._dst)
        return self._conn

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        conn = self._conn
        # Park only a connection that is alive AND whose last exchange completed.
        # `closed` mirrors hyper-util's `Cached` not returning a service whose
        # `poll_ready` failed (pool/cache.rs: `Cached::poll_ready` sets
        # `is_closed` on error; `impl Drop for Cached` then skips the put) —
        # kept live by the connection itself (the h1 idle watcher / h2 read
        # pump), exactly as upstream assumes. `busy` is a RUNTIME-FORCED extra
        # (documented divergence): Rust state transitions are sync between
        # polls, so "exchange never completed but the lease exited cleanly" is
        # unrepresentable there, while Python suspension points make it real —
        # a release interrupted mid-teardown leaves the slot held, and parking
        # that connection would deadlock the next checkout on `send_request`.
        # Drop it instead: never park open-and-lying. (Liveness stays a use-time
        # check, as in hyper-util: a peer close landing right after this park is
        # caught by the next checkout's request, not here.)
        if exc_type is None and not (conn.closed or getattr(conn, "busy", False)) and self._cache._checkin(conn):
            pass  # clean exit + completed exchange -> idle set
        else:
            # Deliberate simplification vs hyper-util (F51, documented WON'T-FIX): on ANY
            # exception during use we close the connection, whereas hyper-util returns it
            # to the pool when it's still open (`is_open()`). The sync `closed or busy`
            # check above could now discriminate here too, but closing on error stays
            # the safe, conservative choice; the only cost is not reusing a connection
            # whose exchange happened to complete before the caller raised for an
            # unrelated reason.
            await conn.__aexit__(None, None, None)
        return False


def _default_key(url):
    # Normalize the port from the scheme when absent, so `http://x` and `http://x:80`
    # route to the SAME per-destination pool rather than two (F52) — the same `Uri`
    # split `connect()` canonicalizes a connection's destination with.
    scheme, host, port, _authority = uri_parts(url)
    return (scheme, host, port)


class Map:
    """Routes a destination URL to a per-key inner pool, built lazily. Mirrors
    hyper-util `pool::map::Map`: a customizable key extractor + a factory that
    builds the inner pool for a new key. The inner pool is whatever the caller
    chooses per destination (a `Singleton`, a `Cache`, …); `Map` only owns the
    keyed lookup + lifecycle."""

    def __init__(self, make_pool: Callable[[str], Any], *, key: Callable[[str], Any] = _default_key) -> None:
        self._make_pool = make_pool  # (url) -> a pool (Singleton | Cache | ...)
        self._key = key  # (url) -> hashable key; default (scheme, host, port)
        self._lock = threading.Lock()  # the routing table + the closed flag: one step each
        self._pools = {}
        self._closed = False  # `aclose()` (end of life): no pool is built afterwards

    def pool_for(self, url: str) -> Any:
        """The inner pool for `url`'s key, creating it via the factory on first use.
        The factory and the key function (foreign code) run outside the lock; two
        callers racing for a new key keep the first one inserted."""
        k = self._key(url)
        with self._lock:
            pool = self._pools.get(k)
            closed = self._closed
        if pool is not None:
            return pool
        if closed:
            raise RuntimeError("Map is closed")
        pool = self._make_pool(url)
        with self._lock:
            if self._closed:
                raise RuntimeError("Map is closed")
            return self._pools.setdefault(k, pool)

    def is_empty(self) -> bool:
        """True iff no per-destination pools exist yet."""
        with self._lock:
            return not self._pools

    async def retain(self, predicate: Callable[[Any], bool]) -> None:
        """Prune stale connections across every per-destination pool — forwards
        `retain(predicate)` to each inner pool (the eviction hook, consistent with
        `Singleton`/`Cache`) (F52). Empty inner pools are left in place (rebuilt lazily
        anyway); use `clear()` to drop the routing table itself."""
        with self._lock:
            pools = list(self._pools.values())
        for pool in pools:
            await pool.retain(predicate)

    async def clear(self) -> None:
        """Close every per-destination pool and drop the routing table; the `Map`
        stays usable and rebuilds pools lazily on the next `pool_for` (F52)."""
        with self._lock:
            pools, self._pools = list(self._pools.values()), {}
        for pool in pools:
            await pool.aclose()

    async def aclose(self) -> None:
        """Close every per-destination pool (end of life): no pool is built afterwards."""
        with self._lock:
            self._closed = True
        await self.clear()
