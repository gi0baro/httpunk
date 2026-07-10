"""Client connect + ALPN negotiation — `httpunk.util`'s analogue of hyper-util's
`client::pool::negotiate` ("decide between two protocols by an intermediate value":
ALPN upgrade to h2, else fall back to h1).

This sits *above* the codec, so — unlike the core — there is no wire-protocol
fidelity constraint; the reference is hyper-util's non-legacy behavior/shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from .. import _backend
from ..h1.client import H1Connection
from ..h2.client import H2Connection


if TYPE_CHECKING:
    import ssl


_DEFAULT_ALPN = ("h2", "http/1.1")
_DEFAULT_PORTS = {"https": 443, "http": 80}


async def connect(
    url: str,
    *,
    backend: _backend.BackendLike | None = None,
    alpn: tuple[str, ...] = _DEFAULT_ALPN,
    ssl_context: ssl.SSLContext | None = None,
) -> H2Connection | H1Connection:
    """Connect to `url` and return the matching **un-entered** low-level connection
    (`H2Connection` or `H1Connection`), with `authority` set from the URL.

    - **https** → TLS-dial with ALPN; `selected == "h2"` is the *upgrade* to
      `H2Connection`, anything else (incl. no ALPN, per RFC 7301) is the *fallback*
      to `H1Connection`.
    - **http** → plain TCP → `H1Connection` (no ALPN on cleartext; h2c is out of
      scope, matching hyper-util).

    The connection is returned un-entered so the caller owns its lifetime
    (`async with await connect(...) as conn: ...`): `connect` does the TLS
    handshake, `__aenter__` does the HTTP handshake. Composable — this is the seam
    a pool / friendly client builds on (§11.6).
    """
    backend = _backend.resolve(backend)
    parts = urlsplit(url)
    scheme = parts.scheme
    if scheme not in _DEFAULT_PORTS:
        raise ValueError(f"unsupported scheme {scheme!r} (expected 'http' or 'https')")
    host = parts.hostname
    if host is None:
        raise ValueError(f"no host in URL {url!r}")
    port = parts.port or _DEFAULT_PORTS[scheme]
    authority = f"{host}:{port}"

    if scheme == "https":
        stream, selected = await backend.connect_tls(host, port, alpn=alpn, ssl_context=ssl_context)
        if selected == "h2":
            return H2Connection(stream, authority=authority, scheme="https", backend=backend)
        return H1Connection(stream, authority=authority, backend=backend)

    stream = await backend.connect_tcp(host, port)
    return H1Connection(stream, authority=authority, backend=backend)
