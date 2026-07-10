# httpunk

httpunk is a Rust-powered async HTTP library for Python.
It's powered by the [hyper](https://github.com/hyperium/hyper) stack and Rust crates like [http](https://github.com/hyperium/http).

httpunk is deliberately *low-level*: you bring your own connected transport, and
build requests and read responses directly. It's meant to be a solid, performant
base for building HTTP clients and servers on top of.

httpunk's API mirrors hyper's wherever possible.

> **Note:** httpunk is in an early, alpha stage.

> **Note:** httpunk was built with substantial help from LLMs, under human supervision.

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

httpunk supports additional backends beyond `asyncio`, but they require extra dependencies.
Enable one via the relevant extra:

```
pip install httpunk[tonio]
```

## Features

- **HTTP/1 and HTTP/2**, client and server implementations
- **Protocol-neutral structures** such as `Request`, `Response`, `HeaderMap`
- **Multiple backend support**: `asyncio` and `tonio` (with `trio` targeted for future releases)
- **AsyncIO ready-to-go protocols**: extensible `asyncio.Protocol` classes (H1, H2, Auto)
- **Batteries in the `util` module**: connect and ALPN negotiation, h1/h2 auto-detection, connection pooling, graceful shutdown, proxy-environment matching.

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
supply headers explicitly.

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
request. The [AsyncIO](#asyncio-utilities) protocols handle that for you.

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

httpunk's exceptions all derive from a common `HTTPunkError` root. `ConnectionClosedError`
is **protocol-neutral** — raised on both HTTP/1 and HTTP/2 when the transport closes with
work in flight — so it sits directly under the root. Every **HTTP/2-specific** error shares
the `H2Error` sub-base:

```
HTTPunkError
├── ConnectionClosedError    transport closed / IO error with work in flight  (HTTP/1 + HTTP/2)
└── H2Error                  base for HTTP/2 protocol errors
    ├── H2ProtocolError      connection-level protocol violation (-> GOAWAY)
    ├── H2StreamError        stream-level protocol violation (-> RST_STREAM)
    ├── H2UserError          local API misuse
    ├── H2FlowControlError   flow-control window over/underflow
    ├── GoAwayError          the peer sent GOAWAY
    └── StreamResetError     the peer sent RST_STREAM for a stream
```

Catch `H2Error` for HTTP/2 protocol failures, `ConnectionClosedError` for a dropped
transport, or `HTTPunkError` for anything httpunk raises.

`GoAwayError` carries `last_stream_id`, `error_code` and `debug_data`; `StreamResetError`
carries `stream_id` and `error_code`. Error codes are `H2Reason` members (an `IntEnum`, so
they compare equal to plain ints) for known codes, or a raw int otherwise.

```python
from httpunk import ConnectionClosedError, GoAwayError, HTTPunkError, StreamResetError

try:
    resp = await conn.request("GET", "/")
    await resp.read()
except StreamResetError as exc:
    print("stream reset:", exc.stream_id, exc.error_code)
except GoAwayError as exc:
    # streams above last_stream_id were not processed and are safe to retry
    print("server going away:", exc.last_stream_id)
except ConnectionClosedError:
    print("transport dropped")
except HTTPunkError:
    ...
```

### Utilities

`httpunk.util` collects the higher-level conveniences a real client/server host needs. Unlike
the core, these carry no wire-protocol fidelity constraint.

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

#### Auto protocol

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

`GracefulShutdown` coordinates a graceful shutdown across many connections.
`watch(server, serve)` registers a connection and returns the coroutine that drives it;
`shutdown()` signals every watched connection and waits for them to drain.

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

### AsyncIO utilities

`httpunk.asyncio` provides reusable `asyncio.Protocol` classes so you
can embed httpunk in any asyncio program.

**Server protocols** — subclass one and implement `handle(request)`:

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

**Client protocols** — the mirror of the server ones, for `loop.create_connection`. Once the
connection is up, `await proto.ready()` returns the httpunk client connection to send requests
on. Configuration (`authority`/`scheme`) is passed via a factory closure, since
`create_connection` calls the factory with no arguments.

- `H1ClientProtocol` / `H2ClientProtocol` — force the protocol.
- `AutoClientProtocol` — pick HTTP/1 vs HTTP/2 from the TLS ALPN result (plain TCP → HTTP/1).

```python
import asyncio
import ssl

import httpunk.asyncio


async def main():
    loop = asyncio.get_running_loop()
    ctx = ssl.create_default_context()
    ctx.set_alpn_protocols(["h2", "http/1.1"])
    transport, proto = await loop.create_connection(
        lambda: httpunk.asyncio.H2ClientProtocol(authority="example.com:443", scheme="https"),
        "example.com", 443, ssl=ctx, server_hostname="example.com",
    )
    conn = await proto.ready()                 # await handshake -> H2Connection
    resp = await conn.request("GET", "/", headers={"host": "example.com"})
    print(resp.status, await resp.read())
    await proto.aclose()


asyncio.run(main())
```

## License

httpunk is released under the BSD 3-Clause License.
