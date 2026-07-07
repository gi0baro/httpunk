"""Low-level HTTP/1 connection API — the `http1` analogue of `h2/client.py`.

`H1Connection` is the per-connection handle over a caller-supplied transport
(BYO transport, like hyper's `client::conn::http1`). It exposes the **same**
surface as `H2Connection` — `send_request(Request) -> Response`, `ready`, and the
`get`/`request` wrappers — so a caller can treat h1 and h2 connections identically.

Cross-reference: hyper `client::conn::http1` (`SendRequest`/`Connection`).
"""

from ..types import Request
from .connection import Connection


class H1Connection:
    """An HTTP/1 client connection over a caller-supplied, already-connected
    `transport`. Use as an async context manager; the transport is closed on exit.
    Serves one request/response at a time (no pipelining); keep-alive connections
    are reused for subsequent requests.

    Like hyper's `client::conn::http1`, this is low-level: the request-target is
    sent verbatim and the caller supplies the `Host` header (we never auto-add
    one). `authority` is accepted for API symmetry with `H2Connection` but is not
    used to rewrite the target.
    """

    def __init__(self, transport, *, authority=None, backend=None):
        self._conn = Connection(transport, authority=authority, backend=backend)

    async def __aenter__(self):
        await self._conn.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._conn.close()
        return False

    async def ready(self):
        """Wait until the connection can accept a request (h1 has no stream slots;
        this waits for the single in-flight request/response to finish). Mirrors
        h2's `conn.ready`."""
        await self._conn.wait_idle()

    async def send_request(self, request):
        """Send `request` and return its `Response` once the head arrives.
        Mirrors h2's `send_request` (hyper `SendRequest::send_request`).

        The request-target is sent **verbatim** (hyper's low-level contract): a
        path (``"/thing"``) is origin-form, an absolute URL (``"http://…"``) is
        absolute-form for a proxy, and an authority (``"host:port"``) is
        authority-form for CONNECT. The caller supplies the ``Host`` header (we
        never auto-add it), exactly like hyper's `client::conn::http1`.
        """
        return await self._conn.send_request(request.method, request.target, request.headers, request.body)

    # ----- ergonomic wrappers (build a Request, call send_request) -----

    async def request(self, method, target, *, headers=None, body=None):
        return await self.send_request(Request(method, target, headers=headers, body=body))

    async def get(self, target, *, headers=None):
        return await self.request("GET", target, headers=headers)
