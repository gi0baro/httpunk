"""HTTP/2 response body — the h2 backend for the protocol-neutral `Response`
(`httpunk/types.py`). h2: share.rs's `RecvStream`.

Body chunks arrive on the stream's queue (fed by the read-pump); reading one
releases its recv-window capacity via the stream manager (which turns it into
WINDOW_UPDATEs). Closing an unfinished body sends RST_STREAM(CANCEL) so the peer
stops sending (h2: `RecvStream` + `SendStream::send_reset`).

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15.
"""

from .._httpunk import H2Reason


class H2ResponseBody:
    """The `Response` body backend for an HTTP/2 stream."""

    upgraded = None  # h2 has no HTTP/1-style Upgrade / CONNECT tunnel

    def __init__(self, stream, manager):
        self._stream = stream
        self._manager = manager

    @property
    def trailers(self):
        """Trailing headers (a `HeaderMap`) if the peer sent a trailers frame
        after the body, else None. h2: the `Trailers` event on `RecvStream`."""
        return self._stream.trailers

    async def aiter_bytes(self):
        """Yield response body chunks as they arrive.

        h2: share.rs `RecvStream::data`; each consumed chunk releases recv-window
        capacity (proto/streams/recv.rs `release_capacity` L458), which the
        manager turns into WINDOW_UPDATE(s).
        """
        while True:
            chunk = await self._stream.body_recv.receive()
            if chunk is None:  # EOF sentinel (end of stream, cancel, or error)
                break
            await self._manager.release_capacity(self._stream, len(chunk))
            yield chunk
        if self._stream.error is not None:
            raise self._stream.error

    async def aclose(self):
        """Cancel the stream if its body wasn't fully read (sends RST_STREAM).
        Safe to call more than once.

        h2: share.rs `SendStream::send_reset` (L355) — dropping a `RecvStream`
        with an unfinished body resets the stream so the peer stops sending.
        """
        st = self._stream
        if not st.state.is_closed() and not st.state.is_recv_end_stream():
            await self._manager.reset_stream(st, H2Reason.CANCEL)
