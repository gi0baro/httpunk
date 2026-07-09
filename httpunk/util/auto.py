"""Auto h1-or-h2 server — `httpunk.util`'s analogue of hyper-util's
`server::conn::auto`: serve an already-accepted transport as h1 **or** h2 by
sniffing the client's opening bytes.

An h2 client opens with the fixed 24-byte connection preface
(`PRI * HTTP/2.0\\r\\n\\r\\nSM\\r\\n\\r\\n`, RFC 7540 §3.5); an h1 request opens with a
method token and can never begin with that prefix. So peek up to `len(PREFACE)`
bytes and compare against `PREFACE[:n]` (hyper-util's `H2_PREFACE` check).

Peeking must not consume bytes the codec needs, so the transport is wrapped in a
`_PrewoundTransport` that replays the peeked bytes before reading live — the same
leftover-replay mechanism as `H1Upgraded`, and hyper-util's `Rewind`. The h2 server
then consumes the replayed preface in `_before_frames`; the h1 server parses the
replayed request line.
"""

from ..h1.server import H1Server
from ..h2.connection import PREFACE
from ..h2.server import H2Server


class _PrewoundTransport:
    """A transport wrapper that replays `prewound` bytes (already read off the
    socket while sniffing) before delegating to the real transport — so protocol
    detection doesn't swallow bytes the codec needs. Everything except the
    replaying `receive_some` (e.g. `send_all`, `close`, `.socket`/`._ssl` for the
    backend's non-blocking peek) forwards to the wrapped transport unchanged.

    Mirrors hyper-util `server::conn::auto`'s `Rewind` IO adapter.
    """

    def __init__(self, transport, prewound):
        self._transport = transport
        self._prewound = bytes(prewound)

    async def receive_some(self, max_bytes=65536):
        if self._prewound:
            chunk, self._prewound = self._prewound[:max_bytes], self._prewound[max_bytes:]
            return chunk
        return await self._transport.receive_some(max_bytes)

    def __getattr__(self, name):
        # Forward send_all / close / socket / _ssl / … to the wrapped transport.
        return getattr(self._transport, name)


async def serve(transport, *, backend=None, only=None):
    """Sniff `transport` and return the matching **un-entered** server (`H2Server`
    or `H1Server`) over a prewound transport that replays the sniffed bytes.

    `only="h1"` / `only="h2"` forces the protocol without sniffing (≈ hyper-util's
    `http1_only` / `http2_only`); a forced server reads the raw transport directly
    (nothing was peeked). Returned un-entered like `connect` — the caller drives it
    with `async with server: async for req in server: ...`.
    """
    if only == "h2":
        return H2Server(transport, backend=backend)
    if only == "h1":
        return H1Server(transport, backend=backend)
    if only is not None:
        raise ValueError(f"only must be None, 'h1', or 'h2' (got {only!r})")

    # Peek up to the full preface, stopping early the moment the bytes diverge
    # from it (→ definitely h1) or the peer stops sending (EOF).
    buf = b""
    while len(buf) < len(PREFACE):
        chunk = await transport.receive_some(len(PREFACE) - len(buf))
        if not chunk:
            break  # EOF before a full preface -> treat as h1 (a truncated request)
        buf += chunk
        if not PREFACE.startswith(buf):
            break  # diverged from the h2 preface -> h1

    prewound = _PrewoundTransport(transport, buf)
    if buf == PREFACE:
        return H2Server(prewound, backend=backend)
    return H1Server(prewound, backend=backend)
