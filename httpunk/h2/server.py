"""HTTP/2 server — the accepting-side analogue of the client
(`client.py` + `connection.py` + `streams.py`), mirroring h2's `server.rs`.

Only the orchestration is new: the vendored codec (`H2Codec("server")`), the
stream state machine (`H2StreamState`), flow control (`H2FlowControl`) and the
SETTINGS sync (`settings.py`) are the same sans-IO core the client uses — the
codec is symmetric, so a server *receives* requests (HEADERS with `:method`/
`:path`) and *sends* responses (`serialize_response_headers`) with no Rust
changes. `ServerStreamManager` subclasses the role-agnostic `StreamManager`
(streams.py) — flow control, reset handling, SETTINGS application and per-frame
recv dispatch are all inherited (h2 keeps this in one place shared by both
roles); only the server role hooks + the response send live here.

Low-level by design (like `hyper::server::conn`): one connection over a
caller-supplied, already-accepted transport; the caller accepts the socket, does
TLS/ALPN, and runs its own accept loop. Usage:

    async with H2Server(transport) as server:
        async for request in server:            # each is a ServerRequest
            body = await request.read()
            await request.respond(200, headers={"content-type": "text/plain"}, body=b"hi")

Requests arrive as they are opened; a caller may `spawn` a handler per request to
serve them concurrently (h2 multiplexes). `request.respond` sends the response,
whose body is flow-control-gated on the client's windows.

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15.
"""

from .._common import BaseServer, read_all
from ..exceptions import (
    ConnectionClosedError,
    H2ProtocolError,
    H2Reason,
)
from ..http import HeaderMap
from .connection import PREFACE, H2ConnectionBase
from .settings import LocalSettings, Settings
from .stream import Stream
from .streams import _RESET_STREAM_SECS, StreamManager, _StreamError


_DEFAULT_MAX_CONCURRENT = 100  # our advertised SETTINGS_MAX_CONCURRENT_STREAMS


class ServerRequest:
    """An incoming request + the handle to respond to it (h2: the `(Request,
    SendResponse)` pair yielded by the server `Connection`)."""

    def __init__(self, stream, manager, *, method, scheme, authority, path, headers):
        self.method = method  # str, e.g. "GET"
        self.scheme = scheme  # str | None
        self.authority = authority  # str | None (the :authority pseudo-header)
        self.path = path  # str | None (the :path pseudo-header)
        self.target = path  # alias, symmetric with the client's Request.target
        self.headers = headers  # httpunk.http.HeaderMap
        self._stream = stream
        self._manager = manager

    @property
    def trailers(self):
        """Trailing request headers (a `HeaderMap`) if the client sent a trailers
        frame after the body, else None."""
        return self._stream.trailers

    async def aiter_bytes(self):
        """Yield request body chunks as they arrive; each consumed chunk releases
        recv-window capacity (-> WINDOW_UPDATE), mirroring the client's read side."""
        while True:
            chunk = await self._stream.body_recv.receive()
            if chunk is None:  # EOF (end of stream, reset, or connection failure)
                break
            await self._manager.release_capacity(self._stream, len(chunk))
            yield chunk
        if self._stream.error is not None:
            raise self._stream.error

    def read(self):
        return read_all(self.aiter_bytes())

    def respond(self, status, *, headers=None, body=None):
        """Send the response: HEADERS (+ body, flow-control-gated). `body` is None,
        `bytes`, or a (sync/async) iterable of `bytes`. h2: `SendResponse`."""
        return self._manager.send_response(self._stream, status, headers, body)

    def __repr__(self):
        return f"ServerRequest(method={self.method!r}, path={self.path!r})"


class ServerStreamManager(StreamManager):
    """The accepting side: takes client-initiated streams, delivers requests,
    receives request bodies, sends responses. h2 `server::Peer` + the recv-stream
    count in `Counts`. All flow control / reset / SETTINGS logic is the shared
    `StreamManager`; only the role hooks + the response send live here."""

    def __init__(self, conn, *, max_concurrent_streams, initial_window_size):
        super().__init__(conn)
        # Highest client stream id we've *seen* (h2 recv `next_stream_id`): a new
        # stream must be a larger odd id. Distinct from `_last_processed_id`, the
        # GOAWAY last-stream-id.
        self._last_recv_id = 0
        # Highest client stream id we actually accepted+delivered (h2
        # `last_processed_id`, recv.rs L167): reported in GOAWAY so the client
        # knows which streams were processed (a REFUSED stream must NOT count).
        self._last_processed_id = 0
        self._max_concurrent = max_concurrent_streams  # our advertised limit
        self._our_initial_window_size = initial_window_size
        # Delivery queue of incoming ServerRequests to the accept loop.
        self._incoming_send, self._incoming_recv = conn.backend.queue()
        # Graceful shutdown: once set, refuse new streams; when the last in-flight
        # stream closes, end the accept loop (the connection's completion signal).
        self._graceful = False

    # ===== role hooks (h2 server `Peer` / `Dyn`) =====

    def _ensure_not_idle(self, sid):
        """A frame on a stream the client has never opened (idle): an even id (the
        client can't open one) or an odd id above the highest we've seen. h2:
        proto/peer.rs `ensure_can_open` (L76) / streams.rs `ensure_not_idle`."""
        if sid % 2 == 0 or sid > self._last_recv_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"frame on idle stream {sid}")

    def _recv_headers_target(self, frame):
        # h2: streams.rs `recv_headers` -> recv.rs `open` (L127) on the server
        # peer. An existing stream => trailers (the shared caller handles it); a
        # locally- or recently-reset stream => swallow late frames; a valid new
        # odd id => open the request stream + deliver it (return None: the head is
        # fully handled here); a decreased id => connection PROTOCOL_ERROR (recv.rs
        # `open` L127 -> `library_go_away` L140); a wrong-parity id is caught by
        # `ensure_can_open` (peer.rs L76).
        sid = frame.stream_id
        st = self._streams.get(sid)
        if st is not None:
            if st.state.is_local_error():
                return None  # locally reset: swallow late frames "for some time"
            return st  # existing stream -> the shared trailers path
        reset_at = self._reset_streams.get(sid)
        if reset_at is not None:
            if self._conn.backend.monotonic() - reset_at <= _RESET_STREAM_SECS:
                return None  # recently reset -> swallow (else a late trailers tears down the conn)
            del self._reset_streams[sid]
        # A new request: must be a strictly-increasing client-initiated (odd) id.
        if sid % 2 == 0 or sid <= self._last_recv_id:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), f"invalid new stream id {sid}")
        self._last_recv_id = sid
        if self._graceful:
            # Graceful shutdown in progress: refuse streams opened after our GOAWAY
            # with REFUSED_STREAM (they must NOT count toward last_processed_id) —
            # h2 refuses streams above the GOAWAY last-stream-id.
            raise _StreamError(sid, int(H2Reason.REFUSED_STREAM))
        if len(self._streams) >= self._max_concurrent:
            # Over the limit we advertised: refuse just this stream with
            # REFUSED_STREAM (h2 recv.rs `open` L145 -> counts.rs
            # `can_inc_num_recv_streams` L100).
            raise _StreamError(sid, int(H2Reason.REFUSED_STREAM))
        st = Stream(
            sid,
            self._conn.backend,
            send_window=self._peer.initial_window_size,
            recv_window=self._recv_init,
        )
        # state.rs `recv_open`: receiving the request HEADERS opens the stream.
        st.state.recv_open(eos=frame.end_stream, informational=False)
        self._streams[sid] = st
        # Now processed (h2 recv.rs L167 `last_processed_id`) — only accepted
        # streams count toward the GOAWAY last-stream-id, not refused ones.
        self._last_processed_id = sid
        self._apply_content_length(st, frame)  # may raise _StreamError
        req = ServerRequest(
            st,
            self,
            method=frame.method,
            scheme=frame.scheme,
            authority=frame.authority,
            path=frame.path,
            headers=frame.headers,
        )
        self._incoming_send.send(req)
        if frame.end_stream:
            # recv_open already closed the recv half; deliver EOF (no request body).
            st.body_send.send(None)
        return None  # the request head is fully handled

    def _on_fail(self):
        self._incoming_send.send(None)  # end the accept loop

    def _stop_accepting(self):
        # End the accept loop after a graceful drain: unlike a peer-driven close
        # (where the read-pump's EOF path calls `_on_fail`), a server-initiated
        # shutdown must itself signal "no more requests" so `next_request` returns
        # None and the caller's `async for` exits.
        self._incoming_send.send(None)

    def _release_slot(self, st):
        # The server tracks no MAX_CONCURRENT slot (it gates on len(self._streams)),
        # but during a graceful shutdown the connection is "done" once the last
        # in-flight stream closes — end the accept loop so the caller's serve loop
        # returns and closes the connection (h2 `poll` returns Ready when drained
        # after GOAWAY).
        if self._graceful and not self._streams:
            self._stop_accepting()

    # ===== sending responses (h2 server.rs SendResponse::send_response) =====

    async def send_response(self, st, status, headers, body):
        # h2: server.rs `SendResponse::send_response` (L1236); state transition =
        # state.rs `send_open` (sending response HEADERS on the recv-opened stream).
        if st.state.is_closed():
            raise ConnectionClosedError("stream already closed")
        hdrs = headers if isinstance(headers, HeaderMap) else HeaderMap(headers)
        end_stream = body is None
        st.state.send_open(eos=end_stream)  # send response HEADERS
        await self._conn.send_frame(
            self._conn.codec.serialize_response_headers(st.id, status, hdrs, end_stream=end_stream)
        )
        if end_stream:
            self._close_stream(st)  # bodyless response — HEADERS closed the send half
            return
        await self.send_body(st, body)  # inherited: END_STREAM on the final DATA, then close

    def next_request(self):
        return self._incoming_recv.receive()


class ServerConnection(H2ConnectionBase):
    """The server protocol driver: consumes the client preface, accepts requests,
    and reports the last-processed stream in GOAWAY. All the read-pump / dispatch /
    SETTINGS / GOAWAY machinery is the shared `H2ConnectionBase`."""

    def __init__(self, transport, *, backend=None, max_concurrent_streams, initial_window_size=None):
        self._max_concurrent_streams = max_concurrent_streams
        self._initial_window_size = initial_window_size
        self._preface_buf = b""
        self._preface_ok = False
        super().__init__(
            transport,
            backend=backend,
            codec_role="server",
            settings=Settings(LocalSettings(initial_window_size=initial_window_size)),
        )
        self.streams = ServerStreamManager(
            self, max_concurrent_streams=max_concurrent_streams, initial_window_size=initial_window_size
        )

    async def start(self):
        # h2: server.rs `handshake` (L365) — the server's connection preface is just
        # its SETTINGS (RFC 7540 §3.5); no readiness wait (it serves requests as they
        # arrive). The client's 24-byte preface is stripped in `_before_frames`.
        settings = {"enable_push": False, "max_concurrent_streams": self._max_concurrent_streams}
        if self._initial_window_size is not None:
            settings["initial_window_size"] = self._initial_window_size
        await self._begin(b"", settings)

    def _before_frames(self, data):
        # Strip the fixed 24-byte client preface (RFC 7540 §3.5) before framing
        # (h2 reads it in server.rs L1427-1441). Returns None until it's complete.
        if self._preface_ok:
            return data
        self._preface_buf += data
        if len(self._preface_buf) < len(PREFACE):
            return None
        if self._preface_buf[: len(PREFACE)] != PREFACE:
            raise H2ProtocolError(int(H2Reason.PROTOCOL_ERROR), "bad client connection preface")
        rest = self._preface_buf[len(PREFACE) :]
        self._preface_buf = b""
        self._preface_ok = True
        return rest

    def _goaway_last_stream_id(self):
        # The last request we actually *processed* (h2 `last_processed_id`), so the
        # client knows which requests were handled and which (higher, incl. refused)
        # are safe to retry.
        return self.streams._last_processed_id

    def next_request(self):
        # Exposed on the driver (like the h1 server) so the `BaseServer` accept
        # iterator is protocol-uniform.
        return self.streams.next_request()

    async def graceful_shutdown(self):
        # h2 `Connection::graceful_shutdown` (proto/connection.rs `go_away`): a
        # non-blocking signal — send GOAWAY with NO_ERROR and our last-processed id
        # so the client opens no new streams, and refuse any it opens anyway
        # (`_recv_headers_target`). In-flight streams finish normally; once the last
        # one closes the accept loop ends (`_release_slot`) and the caller's serve
        # loop closes the connection. Does NOT wait or close here — the caller
        # drives the connection to completion (mirrors hyper-util's coordinator).
        self.streams._graceful = True
        await self.send_frame(self.codec.serialize_go_away(self._goaway_last_stream_id(), int(H2Reason.NO_ERROR)))
        if not self.streams._streams:
            self.streams._stop_accepting()  # nothing in flight -> end the accept loop now


class H2Server(BaseServer):
    """An HTTP/2 server connection over a caller-supplied, already-accepted
    `transport` (BYO transport, like hyper's `server::conn::http2`; accepting the
    socket / TLS / ALPN and the accept loop are the caller's job). The
    async-context-manager + accept-iterator come from `BaseServer` (identical to
    `H1Server`).

    Use as an async context manager and iterate incoming requests:

        async with H2Server(transport) as server:
            async for request in server:
                await request.respond(200, body=b"hi")
    """

    def __init__(
        self, transport, *, backend=None, max_concurrent_streams=_DEFAULT_MAX_CONCURRENT, initial_window_size=None
    ):
        self._conn = ServerConnection(
            transport,
            backend=backend,
            max_concurrent_streams=max_concurrent_streams,
            initial_window_size=initial_window_size,
        )
