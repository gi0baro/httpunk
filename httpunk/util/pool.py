"""Composable connection pools — `httpunk.util`'s analogue of hyper-util's
`client::pool::{singleton, cache, map}`.

Connection reuse is inherently runtime-bound (it manages live connections), so —
unlike the vendored proxy matcher — it lives in Python. hyper-util layers these as
tower `Service`s; we keep the same *concepts and names* as concrete helpers, with
**no** `Service`/`MakeService` abstraction (the decided approach, PLAN §11.6):

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

import threading
from urllib.parse import urlsplit

from .. import _backend


class Canceled(Exception):  # noqa: N818 - `Canceled` is hyper-util's exact name (singleton.rs)
    """A `Singleton` waiter's coalesced connection attempt was ditched because the
    in-flight connect (driven by another caller) failed. Retry by calling again
    (mirrors hyper-util's singleton `Canceled`)."""


class Singleton:
    """Shares a single connection across all callers, coalescing concurrent
    `get()`s onto one connect — the HTTP/2 pattern (one multiplexed connection).
    Mirrors hyper-util `pool::singleton::Singleton` (State: empty → making → made).
    """

    def __init__(self, connector, *, backend=None):
        self._connector = connector
        self._backend = _backend.resolve(backend)
        self._lock = threading.Lock()  # guards the state machine (no await held)
        self._state = "empty"  # empty | making | made
        self._conn = None
        self._error = None
        self._ready = None  # event signalling the current making round is done

    async def get(self, dst=None):
        """The shared connection, connecting once. Concurrent callers during the
        connect wait for it; if that connect fails, the driver raises the real error
        and the waiters get `Canceled`."""
        with self._lock:
            if self._state == "made":
                return self._conn
            if self._state == "empty":
                self._state = "making"
                self._ready = self._backend.event()
                self._error = None
                ready, driver = self._ready, True
            else:  # making — wait for the driver
                ready, driver = self._ready, False

        if driver:
            try:
                conn = await self._connector(dst)
                await conn.__aenter__()  # HTTP handshake — the pool owns the lifetime
            except BaseException as exc:
                with self._lock:
                    self._state = "empty"  # ditch the round so the next get() retries
                    self._error = exc
                ready.set()
                raise
            with self._lock:
                self._conn, self._state = conn, "made"
            ready.set()
            return conn

        await ready.wait()
        with self._lock:
            if self._state == "made":
                return self._conn
        raise Canceled("the connection attempt this call was waiting on failed")

    async def retain(self, predicate):
        """Drop (and close) the shared connection if `predicate(conn)` is False —
        the eviction hook for a dead/stale connection. No-op while empty/making."""
        with self._lock:
            if self._state == "made" and not predicate(self._conn):
                conn, self._conn, self._state = self._conn, None, "empty"
            else:
                conn = None
        if conn is not None:
            await conn.__aexit__(None, None, None)

    def is_empty(self):
        """True iff no connection has been made (or is being made)."""
        with self._lock:
            return self._state == "empty"

    async def aclose(self):
        """Close the shared connection and reset to empty."""
        with self._lock:
            conn, self._conn, self._state = self._conn, None, "empty"
        if conn is not None:
            await conn.__aexit__(None, None, None)


class Cache:
    """A set of idle connections reused via `checkout()` — the HTTP/1 pattern (one
    request at a time per connection). Mirrors hyper-util `pool::cache::Cache`: a
    checkout hands back an idle connection (or makes one) and, on release, returns
    it to the idle set. Release is a lease context manager (the Python stand-in for
    hyper-util's drop-returns-to-cache)."""

    def __init__(self, connector, *, backend=None):
        self._connector = connector
        self._backend = _backend.resolve(backend)
        self._lock = threading.Lock()
        self._idle = []

    def checkout(self, dst=None):
        """A lease over a connection: `async with cache.checkout(dst) as conn: ...`.
        On a clean exit the connection returns to the idle set for reuse; if the body
        raised, it is closed instead (a failed exchange may have left it unusable)."""
        return _Lease(self, dst)

    async def _acquire(self, dst):
        with self._lock:
            conn = self._idle.pop() if self._idle else None
        if conn is not None:
            return conn
        conn = await self._connector(dst)
        await conn.__aenter__()  # HTTP handshake — the pool owns the lifetime
        return conn

    def _checkin(self, conn):
        with self._lock:
            self._idle.append(conn)

    async def retain(self, predicate):
        """Keep only the idle connections `predicate(conn)` returns True for; close
        the rest. The eviction hook for idle/stale connections."""
        with self._lock:
            keep, drop = [], []
            for conn in self._idle:
                (keep if predicate(conn) else drop).append(conn)
            self._idle = keep
        for conn in drop:
            await conn.__aexit__(None, None, None)

    def is_empty(self):
        """True iff no idle connections are cached."""
        with self._lock:
            return not self._idle

    async def aclose(self):
        """Close every idle connection."""
        with self._lock:
            conns, self._idle = self._idle, []
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
        if exc_type is None:
            self._cache._checkin(self._conn)  # reusable -> back to the idle set
        else:
            await self._conn.__aexit__(None, None, None)  # error during use -> close
        return False


def _default_key(url):
    parts = urlsplit(url)
    return (parts.scheme, parts.hostname, parts.port)


class Map:
    """Routes a destination URL to a per-key inner pool, built lazily. Mirrors
    hyper-util `pool::map::Map`: a customizable key extractor + a factory that
    builds the inner pool for a new key. The inner pool is whatever the caller
    chooses per destination (a `Singleton`, a `Cache`, …); `Map` only owns the
    keyed lookup + lifecycle."""

    def __init__(self, make_pool, *, key=_default_key):
        self._make_pool = make_pool  # (url) -> a pool (Singleton | Cache | ...)
        self._key = key  # (url) -> hashable key; default (scheme, host, port)
        self._lock = threading.Lock()
        self._pools = {}

    def pool_for(self, url):
        """The inner pool for `url`'s key, creating it via the factory on first use."""
        k = self._key(url)
        with self._lock:
            pool = self._pools.get(k)
            if pool is None:
                pool = self._make_pool(url)
                self._pools[k] = pool
            return pool

    def is_empty(self):
        """True iff no per-destination pools exist yet."""
        with self._lock:
            return not self._pools

    async def aclose(self):
        """Close every per-destination pool."""
        with self._lock:
            pools, self._pools = list(self._pools.values()), {}
        for pool in pools:
            await pool.aclose()
