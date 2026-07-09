"""httpunk.util — composable HTTP utilities above the codec, mirroring the
**non-legacy** surface of `hyper-util` (the pieces `reqwest` itself builds on; see
PLAN.md §11):

- `connect(url, ...)` — client connect + ALPN negotiation (≈ `client::pool::negotiate`).
- `auto.serve(transport, ...)` — an auto h1-or-h2 server (≈ `server::conn::auto`).
- `GracefulShutdown` — a shutdown coordinator (≈ `server::graceful`).
- `pool.{Singleton,Cache,Map}` — composable connection pools (≈ `client::pool::{singleton,cache,map}`).
- `proxy.{Matcher,Intercept}` — proxy selection (≈ `client::proxy::matcher`, vendored in Rust).

The runtime-bound utilities (connect/auto/graceful/pool) are Python; the pure sans-IO
proxy matcher is vendored. The friendly `reqwest`-style client stays downstream.
"""

from . import auto, pool, proxy
from .client import connect
from .graceful import GracefulShutdown


__all__ = ["GracefulShutdown", "auto", "connect", "pool", "proxy"]
