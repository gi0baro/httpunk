"""HTTP/2 response body — the h2 backend for the protocol-neutral `Response`
(`httpunk/types.py`). h2: share.rs's `RecvStream`.

Body chunks arrive on the stream's queue (fed by the read pump); reading one
releases its recv-window capacity through the connection state (which turns it
into WINDOW_UPDATEs). Closing an unfinished body sends RST_STREAM(CANCEL) so the
peer stops sending (h2: `RecvStream` + `SendStream::send_reset`).

Cross-reference: `h2 ...` comments cite hyperium/h2 0.4.19.
"""


class H2ResponseBody:
    """The `Response` body backend for an HTTP/2 stream."""

    upgraded = None  # h2 has no HTTP/1-style Upgrade / CONNECT tunnel

    def __init__(self, stream, conn):
        self._stream = stream
        self._conn = conn

    @property
    def trailers(self):
        """Trailing headers (a `HeaderMap`) if the peer sent a trailers frame
        after the body, else None. h2: the `Trailers` event on `RecvStream`."""
        return self._stream.trailers

    def aiter_bytes(self):
        """Yield response body chunks as they arrive (h2 share.rs `RecvStream::data`);
        each consumed chunk releases recv-window capacity (recv.rs `release_capacity`)."""
        return self._conn._aiter_body(self._stream)

    async def aclose(self):
        """Cancel the stream if its body wasn't fully read (sends RST_STREAM). Safe to
        call more than once.

        h2: share.rs `SendStream::send_reset` (L355) — dropping a `RecvStream` with an
        unfinished body resets the stream so the peer stops sending. A fully-received
        body needs no RST (h2 guards its Drop-reset on `!eos`) but still returns every
        buffered-but-unread byte's connection window (`release_closed_capacity`,
        streams.rs L1670-1676): aclose is this driver's deterministic drop hook. One
        locked step in Rust (`aclose_body`), idempotent."""
        self._conn._aclose_body(self._stream)
