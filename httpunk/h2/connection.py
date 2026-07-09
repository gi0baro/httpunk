"""HTTP/2 protocol driver — h2: proto/connection.rs.

The thin connection core: owns the transport + shared codec, runs the read-pump,
dispatches inbound frames to the stream manager, drives the SETTINGS handshake
(`settings.py`), and answers PING / handles GOAWAY. All per-stream logic and flow
control live in the stream manager (streams.py); the public request API lives in
`client.py`.

`H2ConnectionBase` is the **role-agnostic** driver, mirroring h2's single
`proto::Connection` (the codec is symmetric). The client `Connection` and the
server `ServerConnection` (server.py) subclass it; the role differences are three
hooks: the connection preface (`_before_frames` — the client *sends* it, the
server *consumes* it), the GOAWAY last-stream-id (`_goaway_last_stream_id`), and
the client-only readiness signal (`_signal_ready`).

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15 (see
src/h2/UPSTREAM_VERSION). This is an *adaptation*: h2 drives everything from one
polled `Connection` future; we use a coroutine read-pump.
"""

import contextlib

from .. import _backend
from .._httpunk import (
    H2Codec,
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as SettingsFrame,
    H2FrameStreamError as StreamErrorFrame,
    H2FrameWindowUpdate as WindowUpdate,
    H2StreamError,
)
from ..exceptions import ConnectionClosedError, GoAwayError, H2Error, H2Reason
from .settings import Action
from .streams import _StreamError


_READ_SIZE = 65536

# The HTTP/2 client connection preface (RFC 7540 §3.5). A fixed, protocol-level
# constant (not HPACK, not runtime-specific) — it belongs with the h2 driver that
# sends it, not the transport backend.
PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

# h2 `StreamId::MAX` (u32::MAX >> 1). A GOAWAY carrying this as last-stream-id is the
# phase-1 "graceful, keep going" signal, not a real last-processed id.
_MAX_STREAM_ID = 2**31 - 1


class H2ConnectionBase:
    """The role-agnostic protocol driver for one transport (h2 `proto::Connection`).
    Subclassed by the client `Connection` and the server `ServerConnection`, which
    supply the connection state (`self.streams`) + the role hooks."""

    def __init__(self, transport, *, backend, codec_role, settings):
        # `transport` is a caller-supplied, already-connected byte stream (BYO
        # transport, like hyper's `client`/`server` conn). Subclasses set
        # `self.streams` after building their role state.
        self.backend = _backend.resolve(backend)
        self.codec = H2Codec(codec_role)
        self.error = None
        self._scope = self.backend.scope()
        # A SEPARATE scope for background request-body writers (client full-duplex
        # send, F6), kept out of the read-pump's `_scope`: writers are torn down
        # first in `close()`, so the pump's scope stays pump-only and its teardown
        # timing is unaffected (the pump's parked-recv close race is delicate).
        self._write_scope = self.backend.scope()
        self._send_lock = self.backend.lock()
        self._transport = transport
        self._settings = settings  # SETTINGS sync (proto/settings.rs)
        self._goaway_replied = False  # sent our acknowledging GOAWAY after a peer GOAWAY (F23)

    # ----- role hooks -----

    def _before_frames(self, data):
        """Transform received bytes before framing. Default: identity — the client
        *sends* the connection preface, so has none to strip. The server overrides
        to consume the 24-byte client preface, returning None until it's complete."""
        return data

    def _goaway_last_stream_id(self):
        """The last-stream-id for our GOAWAY. The client processes no peer-initiated
        streams -> 0; the server overrides with the last request it processed."""
        return 0

    def _signal_ready(self):
        """Client-only: unblock `connect()` once the peer's initial SETTINGS land
        (or the handshake fails). No-op on the server (no readiness gate)."""

    async def _on_pong(self, frame):
        """Received a PING ack (PONG). Base: ignore (we send no pings by default). The
        server overrides it to drive phase 2 of graceful shutdown — its shutdown PING's
        ack triggers the final GOAWAY with the real last-processed stream id."""

    # ----- lifecycle -----

    async def _begin(self, preface, settings):
        # Shared handshake start (h2 client.rs/server.rs `handshake`): open the
        # scope, flush the connection preface (client: 24-byte preface; server:
        # empty) + our initial SETTINGS, and spawn the read-pump.
        await self._scope.__aenter__()
        await self._write_scope.__aenter__()
        await self.send_frame(preface + self.codec.serialize_settings(**settings))
        # Advertise our (larger-than-default) connection recv window right after the
        # preface, before any peer data (h2 sends this initial WINDOW_UPDATE(0) as
        # part of connection setup, from `initial_connection_window_size`).
        await self.streams.raise_connection_window()
        self._scope.spawn(self._read_pump())

    async def close(self):
        # Close the transport *first*: the read-pump is almost always parked in
        # `transport.receive_some`, and closing makes that return EOF so the pump
        # exits on its own — deterministic, rather than relying on cancellation to
        # interrupt a native socket poll (which races and can leave the scope's
        # `__aexit__` waiting on a task that never wakes → the intermittent
        # teardown hang). The EOF also unblocks the peer's read loop. `cancel()`
        # then covers the rare case where the pump is parked elsewhere.
        # Tear down background body writers FIRST (they're abortable — parked on flow
        # control / send, not a native recv poll), so the pump's scope is pump-only
        # when we close the transport + join it (keeping that delicate race baseline).
        self._write_scope.cancel()
        await self._write_scope.__aexit__(None, None, None)
        if self._transport is not None:
            self.backend.close_transport(self._transport)
        self._scope.cancel()
        await self._scope.__aexit__(None, None, None)
        # Guarantee every straggler waiter is woken. The pump normally calls `fail_all`
        # itself on EOF, but that races the `cancel()` above — if cancellation reaches
        # the pump before it processes the close-induced EOF, `fail_all` is skipped and a
        # concurrent waiter (parked on a stream event) hangs forever (F44). The pump has
        # now been joined, so this runs sequentially: it's a no-op when the pump already
        # failed everyone (streams are popped as they're aborted), and wakes the
        # stragglers when it didn't.
        self.streams.fail_all(self.error or ConnectionClosedError("connection closed"))

    async def send_frame(self, data):
        async with self._send_lock:
            await self._transport.send_all(data)

    async def _maybe_goaway_reply(self):
        """If the peer has GOAWAY'd us with a REAL last-stream-id and no streams remain,
        send our acknowledging GOAWAY(NO_ERROR, last-processed-id) exactly once and
        report that the pump should stop (F23). No-op otherwise.

        A phase-1 graceful GOAWAY (`last_stream_id == 2^31-1`) is explicitly NOT a
        trigger — it means "I'm shutting down, keep your in-flight work going" and is
        followed by a shutdown PING then the real GOAWAY. Reacting to it by stopping the
        pump would skip answering that PING and stall the peer's two-phase graceful
        (h2's `should_close_on_idle` excludes `StreamId::MAX` for the same reason)."""
        if (
            self._goaway_replied
            or self.streams._goaway is None
            or self.streams._streams
            or self.streams._goaway_last_id is None
            or self.streams._goaway_last_id >= _MAX_STREAM_ID
        ):
            return False
        self._goaway_replied = True
        await self.send_frame(self.codec.serialize_go_away(self._goaway_last_stream_id(), int(H2Reason.NO_ERROR)))
        return True

    # ----- inbound -----

    async def _read_pump(self):
        # h2: the read side of proto/connection.rs `poll2` (L318) — pull frames
        # from the transport and dispatch. (We loop in a coroutine; h2 polls.)
        try:
            while True:
                data = await self._transport.receive_some(_READ_SIZE)
                if not data:  # EOF
                    self._fail(ConnectionClosedError("connection closed by peer"))
                    break
                data = self._before_frames(data)  # server strips the client preface
                if data is None:
                    continue  # preface not complete yet
                for frame in self.codec.receive(data):
                    try:
                        await self._dispatch(frame)
                    except _StreamError as se:
                        # Stream-level violation: RST just that stream, keep going.
                        await self.streams.reset_on_error(se.stream_id, se.reason)
                    except H2StreamError as se:
                        # Stream-level error from the Rust state machine (`Error::Reset`):
                        # args = (stream_id, reason, initiator). A REMOTE-initiated reset is
                        # the peer's own RST_STREAM surfacing — h2 returns Ok and sends
                        # nothing (connection.rs L448-462); echoing it back (and counting it
                        # toward the ENHANCE_YOUR_CALM cap) is wrong. Only send library/user
                        # resets (F25).
                        if se.args[2] != "remote":
                            await self.streams.reset_on_error(se.args[0], se.args[1])
                # After a peer GOAWAY, once every in-flight stream has finished, send our
                # acknowledging GOAWAY(NO_ERROR, last-processed-id) and stop serving —
                # h2's poll loop does exactly this (`go_away_now(NO_ERROR)` once
                # `error.is_some()` and `!has_streams()`, connection.rs L287-295), rather
                # than lingering until the peer closes the socket (F23). The transport's
                # FIN follows on the caller's `__aexit__` (httpunk's user-driven lifecycle).
                if await self._maybe_goaway_reply():
                    break
        except H2Error as exc:
            # A protocol/flow violation we detected (bad state, HPACK/CONTINUATION,
            # flow control, bad preface): notify the peer with GOAWAY, then tear down.
            await self._send_goaway(exc)
            self._fail(exc)
        except OSError as exc:
            # A raw transport error — an abrupt peer RST (ConnectionResetError), a
            # broken pipe, etc. Surface it as a clean `ConnectionClosedError` (an
            # H2Error) so callers see the protocol error type, not a bare socket errno
            # (h2 maps Io errors this way; cf. the explicit EOF branch above).
            self._fail(ConnectionClosedError(f"connection closed: {exc}"))
        except Exception as exc:  # any other unexpected error (cancellation is BaseException)
            self._fail(exc)

    async def _send_goaway(self, exc):
        # h2: proto/connection.rs `go_away_now` (L409) / `go_away_now_data` (L415).
        # The last-stream-id is role-specific (client: 0, no peer-initiated streams;
        # server: the last request it processed). Reason comes from the error.
        reason = exc.args[0] if exc.args and isinstance(exc.args[0], int) else int(H2Reason.PROTOCOL_ERROR)
        # Best effort: the connection is going down regardless of whether this lands.
        with contextlib.suppress(Exception):
            await self.send_frame(self.codec.serialize_go_away(self._goaway_last_stream_id(), reason))

    async def _dispatch(self, frame):
        # h2: the frame match in proto/connection.rs `recv_frame` (L518).
        if isinstance(frame, SettingsFrame):
            await self._on_settings(frame)
        elif isinstance(frame, Headers):
            self.streams.recv_headers(frame)
        elif isinstance(frame, Data):
            await self.streams.recv_data(frame)
        elif isinstance(frame, WindowUpdate):
            self.streams.recv_window_update(frame)
        elif isinstance(frame, Ping):
            if frame.ack:
                await self._on_pong(frame)  # a PING ack — drives graceful phase 2 on the server
            else:
                await self.send_frame(self.codec.serialize_ping_ack(frame.data))
        elif isinstance(frame, GoAway):
            # Graceful: streams <= last_stream_id keep running; new/higher ones
            # are refused. The pump continues until the peer closes (EOF).
            self.streams.handle_go_away(
                frame.last_stream_id,
                GoAwayError(frame.last_stream_id, frame.error_code, frame.debug_data),
            )
        elif isinstance(frame, RstStream):
            await self.streams.recv_reset(frame)
        elif isinstance(frame, StreamErrorFrame):
            # The codec detected a stream-level violation (malformed header block,
            # invalid dependency): RST just that stream (h2 `Error::library_reset`).
            raise _StreamError(frame.stream_id, frame.error_code)
        # Priority: accepted and ignored (we don't act on prioritization).

    async def _on_settings(self, frame):
        # h2: proto/settings.rs `recv_settings` (L41) drives the sync; the value
        # application is delegated to the streams manager.
        action, payload = self._settings.recv_settings(frame)
        if action is Action.APPLY_LOCAL:
            self.streams.apply_local_settings(payload)
        elif action is Action.ACK_AND_APPLY:
            await self.send_frame(self.codec.serialize_settings_ack())
            peer_frame, is_initial = self._settings.take_remote()
            self.streams.apply_remote_settings(peer_frame)
            if is_initial:
                self._signal_ready()  # connection fully established (client unblocks connect())

    def _fail(self, exc):
        # h2: proto/connection.rs `handle_poll2_result` (L430) -> the connection
        # error fans out to every stream via streams.rs `Streams::handle_error` (L362).
        self.error = exc
        self.streams.fail_all(exc)
        self._signal_ready()  # unblock connect() if the handshake never completed (client); no-op (server)
