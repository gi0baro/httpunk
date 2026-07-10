# httpunk

*Low-level HTTP/1 and HTTP/2 for Python, built on [hyper](https://hyper.rs).*

httpunk is a low-level HTTP/1 and HTTP/2 client **and** server library for Python. It wraps
the Rust [hyper](https://github.com/hyperium/hyper) stack (hyper, [h2](https://github.com/hyperium/h2)
and hyper-util) as a native extension and drives it with a thin async orchestration layer in
Python. The protocol machinery is the real hyper codebase; httpunk gives you Pythonic
connection/server objects on top of it.

It is deliberately *low-level*, in the same spirit as hyper's own `client::conn` /
`server::conn`: you bring your own connected transport, you build requests and read responses
by hand, and there are no per-verb shortcuts or hidden header magic. If you want a batteries-
included HTTP client, this is the layer such a client would be built on — not the client
itself.

> **Note**
> httpunk is in an early, alpha stage. The API may change between releases.

> **Note**
> httpunk runs on CPython 3.10+ and PyPy. The high-performance `tonio` backend requires
> free-threaded CPython 3.14+; the `asyncio` backend runs everywhere. See
> [Backends](#backends).

## In a nutshell

A client request over the `asyncio` backend:

```python
import asyncio

from httpunk import Backend
from httpunk.util import connect


async def main():
    # connect() dials the socket, does TLS + ALPN, and returns the matching
    # (un-entered) HTTP/2 or HTTP/1 connection.
    async with await connect("https://www.example.com", backend=Backend.asyncio) as conn:
        resp = await conn.request("GET", "/", headers={"host": "www.example.com"})
        print(resp.status)                    # 200
        print(resp.headers["content-type"])   # b'text/html; charset=UTF-8'
        print(await resp.read())              # b'<!doctype html>...'


asyncio.run(main())
```

A server that speaks both HTTP/1 and HTTP/2, embedded in an asyncio loop:

```python
import asyncio

import httpunk.asyncio


class Echo(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        body = await request.read()
        await request.respond(200, headers={"content-type": "text/plain"}, body=body)


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(Echo, "0.0.0.0", 8000)
    async with server:
        await server.serve_forever()


asyncio.run(main())
```

## Installation

```
pip install httpunk
```

httpunk ships pre-built wheels containing the Rust extension, so a Rust toolchain is not
required to install.

The `asyncio` backend has no extra dependencies. The `tonio` backend pulls in
[tonio](https://github.com/gi0baro/tonio) and is only installed automatically on free-threaded
CPython 3.14+ (non-Windows).

## Features

- **HTTP/1 and HTTP/2**, client and server, backed by the real hyper/h2 protocol code.
- **Bring your own transport** — connections and servers are handed an already-connected
  (client) or already-accepted (server) transport, so dialing, TLS and socket ownership stay
  in your hands (or in `httpunk.util`).
- **Protocol-neutral messages** — the same `Request` / `Response` / `HeaderMap` types work
  across HTTP/1 and HTTP/2, so callers can treat the two interchangeably.
- **Two interchangeable backends** — `tonio` (a multi-threaded runtime for free-threaded
  Python) and `asyncio` (the standard library), selected explicitly per connection/server.
- **`asyncio` embedding** — reusable `asyncio.Protocol` server classes bring HTTP/2 to
  asyncio-based hosts (uvicorn/hypercorn-style) that otherwise only speak HTTP/1.
- **Batteries in `httpunk.util`** — connect + ALPN negotiation, h1/h2 auto-detection,
  connection pooling, graceful shutdown, and proxy-environment matching.

## Usage

### Backends

Everything that does I/O runs on a *backend*. There is **no default**: `tonio` needs
free-threaded CPython 3.14+, while `asyncio` runs everywhere, so you must choose one and pass
it explicitly.

```python
from httpunk import Backend

Backend.asyncio   # the standard-library asyncio backend (available everywhere)
Backend.tonio     # the tonio runtime backend (free-threaded CPython 3.14+)
```

Every connection, server and `httpunk.util` helper takes a `backend=` argument, which accepts
a `Backend` member (the recommended form) or an already-created backend instance:

```python
from httpunk import Backend, H2Connection

conn = H2Connection(transport, authority="example.com:443", backend=Backend.asyncio)
```

### Client

A client connection is created over a transport you have already connected. `H1Connection` and
`H2Connection` share the same surface, so code written against one works against the other.

```python
from httpunk import Backend, H1Connection, H2Connection, Request

# `transport` is any connected transport from your chosen backend
# (e.g. `await AsyncioBackend().connect_tcp(host, port)`), or use
# `httpunk.util.connect()` which dials + negotiates for you.
async with H2Connection(transport, authority="example.com:443", backend=Backend.asyncio) as conn:
    # Build a request explicitly and send it:
    resp = await conn.send_request(Request("GET", "/", headers={"host": "example.com"}))
    # ...or use the request() convenience:
    resp = await conn.request("GET", "/", headers={"host": "example.com"})
```

Entering the connection with `async with` runs the protocol handshake; leaving it closes the
transport.

`Request` is protocol-neutral: `Request(method, target, *, headers=None, body=None,
trailers=None)`. The request-target is sent **verbatim** (a path, an absolute URL, or an
authority for `CONNECT`) — httpunk never rewrites it or auto-adds a `Host` header, so you
supply headers explicitly. This is the same low-level contract as hyper's `client::conn`.

`Response` exposes `status`, `headers` (a `HeaderMap`), and a lazily-streamed body:

```python
resp = await conn.request("GET", "/data")
resp.status                       # int, e.g. 200
resp.headers["content-type"]      # header values are bytes

# Read the whole body...
data = await resp.read()

# ...or stream it chunk by chunk:
async for chunk in resp.aiter_bytes():
    ...

resp.trailers                     # a HeaderMap of trailing headers, or None
```

A response can be used as an async context manager to guarantee release (cancelling the body
if it wasn't fully read):

```python
async with await conn.request("GET", "/big") as resp:
    async for chunk in resp.aiter_bytes():
        ...
```

#### Streaming request bodies and trailers

`body` may be `bytes`, or a sync/async iterable of `bytes` (streamed as it is produced).
`trailers` are header fields sent after the body — chunked trailers on HTTP/1, a trailing
`HEADERS` frame on HTTP/2:

```python
async def chunks():
    yield b"hello "
    yield b"world"

resp = await conn.request(
    "POST", "/upload",
    headers={"host": "example.com", "content-type": "application/octet-stream"},
    body=chunks(),
    trailers={"x-checksum": "..."},
)
```

#### Readiness

`conn.ready()` waits until the connection can accept a request (an HTTP/2 stream slot is free,
or the single in-flight HTTP/1 exchange has finished). `conn.closed` is a synchronous liveness
check — useful for evicting a dead connection from a pool.

### Server

A server is created over a transport you have already accepted from a listener. Iterate it to
handle incoming requests; `H1Server` and `H2Server` share the same accept loop.

```python
from httpunk import Backend, H1Server

async with H1Server(transport, backend=Backend.asyncio) as server:
    async for request in server:
        body = await request.read()
        await request.respond(200, headers={"content-type": "text/plain"}, body=body)
```

Each `request` carries `method`, `target`/`path`, `headers`, and a streamable body
(`request.read()` / `request.aiter_bytes()`). Answer it with `request.respond(status, *,
headers=None, body=None)`. On HTTP/2 you can also abort a single stream with
`request.reset()` instead of responding (e.g. when a handler fails) — the connection and its
other streams keep running.

HTTP/1 serves one request/response at a time (the loop won't yield the next until the current
one is answered); HTTP/2 multiplexes, so for concurrent handling you would spawn a task per
request. The [`httpunk.asyncio`](#httpunkasyncio) protocols handle that for you.

Servers support cooperative graceful shutdown via `server.graceful_shutdown()` (see
[`GracefulShutdown`](#graceful-shutdown) for coordinating this across many connections).

### Headers

`HeaderMap` is a dict-like, multi-value-aware header container (reused from the Rust `http`
crate). Names are case-insensitive; **values are returned as `bytes`**.

```python
from httpunk import HeaderMap

h = HeaderMap({"content-type": "text/plain"})
h["content-type"]            # b'text/plain'
h.get("x-missing")           # None
h.add("set-cookie", "a=1")   # append (multi-value)
h.add("set-cookie", "b=2")
h.get_all("set-cookie")      # [b'a=1', b'b=2']
"content-type" in h          # True
```

Anywhere a `headers=` argument is accepted you can pass a `HeaderMap`, a mapping, or an
iterable of `(name, value)` pairs.

### Errors

httpunk's exceptions derive from a common `H2Error` base (defined in Rust, shared with the h2
state machine):

```
H2Error
├── H2ProtocolError      connection-level protocol violation (-> GOAWAY)
├── H2StreamError        stream-level protocol violation (-> RST_STREAM)
├── H2UserError          local API misuse
├── H2FlowControlError   flow-control window over/underflow
├── ConnectionClosedError    transport closed / IO error with work in flight
├── GoAwayError          the peer sent GOAWAY
└── StreamResetError     the peer sent RST_STREAM for a stream
```

`GoAwayError` carries `last_stream_id`, `error_code` and `debug_data`; `StreamResetError`
carries `stream_id` and `error_code`. Error codes are `H2Reason` members (an `IntEnum`, so
they compare equal to plain ints) for known codes, or a raw int otherwise.

```python
from httpunk import GoAwayError, StreamResetError, H2Error

try:
    resp = await conn.request("GET", "/")
    await resp.read()
except StreamResetError as exc:
    print("stream reset:", exc.stream_id, exc.error_code)
except GoAwayError as exc:
    # streams above last_stream_id were not processed and are safe to retry
    print("server going away:", exc.last_stream_id)
except H2Error:
    ...
```

### `httpunk.util`

`httpunk.util` collects the higher-level conveniences a real client/server host needs. Unlike
the core, these carry no wire-protocol fidelity constraint — they mirror hyper-util's shapes.

#### connect

`connect(url, *, backend, alpn=("h2", "http/1.1"), ssl_context=None)` dials `url`, negotiates
the protocol, and returns the matching **un-entered** connection (with `authority` set from the
URL):

- `https` → TLS with ALPN; `h2` upgrades to `H2Connection`, anything else falls back to
  `H1Connection`.
- `http` → plain TCP → `H1Connection`.

```python
from httpunk import Backend
from httpunk.util import connect

async with await connect("https://example.com", backend=Backend.asyncio) as conn:
    resp = await conn.request("GET", "/", headers={"host": "example.com"})
```

#### auto.serve

`auto.serve(transport, *, backend, only=None, cancel=None)` sniffs an accepted transport's
opening bytes and returns the matching **un-entered** `H1Server` or `H2Server` — the accepting-
side analogue of `connect`. Pass `only="h1"` / `only="h2"` to force a protocol.

```python
from httpunk import Backend
from httpunk.util import auto

server = await auto.serve(transport, backend=Backend.asyncio)
async with server:
    async for request in server:
        await request.respond(200, body=b"ok")
```

#### Connection pools

`httpunk.util.pool` provides three composable pools. A *connector* is an async callable
returning an un-entered connection (typically `lambda dst: connect(dst)`); the pool owns the
connection's lifetime.

- **`Singleton`** — coalesces concurrent callers onto **one shared** connection (the HTTP/2
  pattern). `await pool.get()` returns the shared connection, connecting once.
- **`Cache`** — a set of idle connections checked out and returned for reuse (the HTTP/1
  pattern). `async with cache.checkout() as conn:` leases one.
- **`Map`** — routes a destination URL to a per-key inner pool, built lazily.

```python
from httpunk import Backend
from httpunk.util import connect, pool

shared = pool.Singleton(lambda dst: connect(dst, backend=Backend.asyncio), backend=Backend.asyncio)
conn = await shared.get("https://example.com")
resp = await conn.request("GET", "/", headers={"host": "example.com"})
```

All pools expose `retain(predicate)` (evict connections a predicate rejects), `is_empty()` and
`aclose()`.

#### Graceful shutdown

`GracefulShutdown` coordinates a graceful shutdown across many connections (hyper-util's
`server::graceful::GracefulShutdown`). `watch(server, serve)` registers a connection and
returns the coroutine that drives it; `shutdown()` signals every watched connection and waits
for them to drain.

```python
from httpunk.util import GracefulShutdown

graceful = GracefulShutdown(backend=Backend.asyncio)

async def serve(server):
    async with server:
        async for request in server:
            await handle(request)

# spawn `graceful.watch(server, serve)` per accepted connection, then on shutdown:
await graceful.shutdown()
```

#### Proxy matching

`httpunk.util.proxy` exposes the vendored proxy matcher (`*_PROXY` / `NO_PROXY` environment
rules):

```python
from httpunk.util import proxy

matcher = proxy.Matcher.from_env()
intercept = matcher.intercept("https://example.com")
if intercept is not None:
    print(intercept.uri)   # the proxy to use for this URL
```

### `httpunk.asyncio`

`httpunk.asyncio` provides reusable `asyncio.Protocol` server classes so you can embed httpunk
in any asyncio-based server. Unlike the HTTP/1-only protocols shipped by uvicorn/hypercorn,
these also bring **HTTP/2**. Subclass one and implement `handle(request)`:

- `H1ServerProtocol` / `H2ServerProtocol` — force the protocol.
- `AutoServerProtocol` — detect HTTP/1 vs HTTP/2 from the client's opening bytes.

```python
import asyncio

import httpunk.asyncio


class MyServer(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(MyServer, "0.0.0.0", 8000)
    async with server:
        await server.serve_forever()


asyncio.run(main())
```

Each protocol supports `graceful_shutdown()` and `wait_closed()`. For host-coordinated
shutdown, `ServerConnections` tracks live connections and drains them together:

```python
from httpunk.asyncio import ServerConnections

conns = ServerConnections()
server = await loop.create_server(conns.track(MyServer), host, port)
# ... on shutdown:
server.close()                     # stop accepting new connections
await conns.shutdown(timeout=30)   # drain in-flight, force-close stragglers
```

## License

httpunk is released under the BSD 3-Clause License.
