"""Low-level HTTP/2 connection API — h2: client.rs (`SendRequest` + `handshake`).

`H2Connection` is the per-connection handle, the Python analogue of hyper's
`client::conn::http2`: it collapses `http2::handshake` + the spawned `Connection`
driver + `SendRequest` into one async-context-managed object. The core method is
`send_request(Request) -> Response` (≈ `SendRequest::send_request`); `get`/`request`
are thin ergonomic wrappers over it. This layer is low-level by design — no pool,
connector, or high-level client (those live downstream; see PLAN.md §3.3).

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15.
"""

from ..types import Request, Response
from .connection import Connection
from .share import H2ResponseBody


class H2Connection:
    """An HTTP/2 client connection over a caller-supplied, already-connected
    `transport` (BYO transport, like hyper's `client::conn::http2::handshake(io)`;
    dialing / TLS / ALPN are the caller's or `httpunk.util`'s job). Use as an
    async context manager; the driver's read-pump runs for the lifetime of the
    `async with` block, and the transport is closed on exit.

    `authority` (e.g. ``"example.com:443"``) builds the :authority pseudo-header
    for requests given a bare path; requests with an absolute-URL target carry
    their own authority.
    """

    def __init__(self, transport, *, authority=None, backend=None, initial_window_size=None):
        self._conn = Connection(
            transport, authority=authority, backend=backend, initial_window_size=initial_window_size
        )

    async def __aenter__(self):
        await self._conn.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._conn.close()
        return False

    async def ready(self):
        """Wait until the connection can accept a new request — it's alive (not
        failed, no GOAWAY received) and a MAX_CONCURRENT_STREAMS slot is free —
        then return. Raises if the connection has failed or the peer sent GOAWAY.
        Mirrors h2's `SendRequest::ready` (client.rs L401; underlying
        `poll_ready` at L367).

        Best-effort / non-reserving, like h2's `poll_ready`: a concurrent
        `send_request` may still take the slot first, so `send_request` re-applies
        the same backpressure. Calling `ready()` first is therefore optional — it
        just lets a caller pre-flight capacity/liveness without opening a stream.
        """
        await self._conn.streams.wait_until_ready()

    async def send_request(self, request):
        """Send `request` and return its `Response` once the head arrives.

        h2: client.rs `SendRequest::send_request` (L512). Open the stream + send
        HEADERS, stream the body, then await the response head.
        """
        # A bodyless request carries END_STREAM on HEADERS (h2 `send_request`
        # with `end_of_stream`), not a trailing empty DATA frame. A HEAD request
        # response never has a body regardless of content-length.
        end_stream = request.body is None
        is_head = request.method.upper() == "HEAD"
        stream = await self._conn.streams.open_stream(
            request.method,
            self._resolve(request.target),
            request.headers,
            end_stream=end_stream,
            is_head=is_head,
        )
        if not end_stream:
            await self._conn.streams.send_body(stream, request.body)

        await stream.headers_evt.wait()
        if stream.error is not None:
            raise stream.error
        if self._conn.error is not None:
            raise self._conn.error
        return Response(stream.status, stream.headers, H2ResponseBody(stream, self._conn.streams))

    def _resolve(self, target):
        # An absolute URL passes through; a bare path is resolved against the
        # connection's authority (the codec splits it into :scheme/:authority/:path).
        if "://" in target:
            return target
        if self._conn.authority is None:
            raise ValueError(
                "request target is a bare path but the connection has no authority; "
                "pass authority=... to H2Connection or use an absolute-URL target"
            )
        return f"http://{self._conn.authority}{target}"

    # ----- ergonomic wrappers (build a Request, call send_request) -----

    async def request(self, method, target, *, headers=None, body=None):
        return await self.send_request(Request(method, target, headers=headers, body=body))

    async def get(self, target, *, headers=None):
        return await self.request("GET", target, headers=headers)
