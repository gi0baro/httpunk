"""HTTP/2 response body — the h2 backend for the protocol-neutral `Response`
(`httpunk/types.py`). h2: share.rs's `RecvStream`.

Body chunks arrive on the stream's queue (fed by the read-pump); reading one
releases its recv-window capacity via the stream manager (which turns it into
WINDOW_UPDATEs). Closing an unfinished body sends RST_STREAM(CANCEL) so the peer
stops sending (h2: `RecvStream` + `SendStream::send_reset`).

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15.
"""

from .._httpunk import H2Reason
from ..exceptions import fresh_exc


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
            item = await self._stream.body_recv.receive()
            if item is None:  # EOF sentinel (end of stream, cancel, or error)
                break
            chunk, budgeted = item  # h2 0.4.19 DataEvent: (payload, is_budgeted)
            if budgeted:
                self._manager.release_data_frame(self._stream, len(chunk))  # return its buffering charge (#935)
            await self._manager.release_capacity(self._stream, len(chunk))
            yield chunk
        if self._stream.error is not None:
            # A copy per raise — many readers may surface the one stored stream/conn
            # error, and a shared raised instance would accumulate every consumer's
            # frames onto its traceback (exceptions.fresh_exc).
            raise fresh_exc(self._stream.error) from self._stream.error

    async def aclose(self):
        """Cancel the stream if its body wasn't fully read (sends RST_STREAM).
        Safe to call more than once.

        h2: share.rs `SendStream::send_reset` (L355) — dropping a `RecvStream`
        with an unfinished body resets the stream so the peer stops sending.

        Only a fully-received body short-circuits (h2 guards its Drop-reset on
        `!eos` the same way). A CLOSED state does NOT: `reset_stream`'s closed
        branch is the idempotent repair path for a previous reset that was
        interrupted before completing, so a retried aclose must reach it rather
        than no-op with the concurrency slot still leaked.
        """
        st = self._stream
        if st.state.is_recv_end_stream():
            # Body fully received — nothing to cancel on the wire: upstream's
            # Drop-reset (`maybe_cancel`, streams.rs L1686) is a no-op after
            # EOS. But its drop path does a SECOND, unconditional thing once
            # the last handle is gone: `release_closed_capacity` (streams.rs
            # L1670-1676 -> recv.rs L502-522) returns every buffered-but-unread
            # byte's connection window (WINDOW_UPDATE(0)) and, since h2 0.4.19,
            # the frames' framing-budget charges (`clear_recv_buffer`) — "no
            # one can access it anymore". aclose is this driver's deterministic
            # drop hook (no ref-counting), so the release happens here. In a
            # full-duplex exchange with the request body still uploading this
            # runs slightly earlier than upstream's ref==0 point — a
            # runtime-forced divergence, and semantically safe: it credits
            # received-and-discarded data the send half can't touch. The F22
            # `recv_reclaimed` flag makes a straggling reader release a no-op,
            # so nothing is ever credited twice.
            conn_wu = self._manager._reclaim_stream_accounting(st)
            if conn_wu:
                conn = self._manager._conn
                conn.enqueue_frame(conn.codec.serialize_window_update(0, conn_wu))
            return
        await self._manager.reset_stream(st, H2Reason.CANCEL)
