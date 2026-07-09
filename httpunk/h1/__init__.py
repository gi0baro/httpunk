"""HTTP/1 client — Python driver over the Rust `H1Codec` (vendored hyper h1
sans-IO core). Mirrors the `httpunk/h2/` module layout."""

from .client import H1Connection as H1Connection
from .server import H1Server as H1Server, ServerRequest as ServerRequest
from .share import H1Upgraded as H1Upgraded
