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
        self._send_lock = self.backend.lock()
        self._transport = transport
        self._settings = settings  # SETTINGS sync (proto/settings.rs)

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

    # ----- lifecycle -----

    async def _begin(self, preface, settings):
        # Shared handshake start (h2 client.rs/server.rs `handshake`): open the
        # scope, flush the connection preface (client: 24-byte preface; server:
        # empty) + our initial SETTINGS, and spawn the read-pump.
        await self._scope.__aenter__()
        await self.send_frame(preface + self.codec.serialize_settings(**settings))
        self._scope.spawn(self._read_pump())

    async def close(self):
        # Close the transport *first*: the read-pump is almost always parked in
        # `transport.receive_some`, and closing makes that return EOF so the pump
        # exits on its own — deterministic, rather than relying on cancellation to
        # interrupt a native socket poll (which races and can leave the scope's
        # `__aexit__` waiting on a task that never wakes → the intermittent
        # teardown hang). The EOF also unblocks the peer's read loop. `cancel()`
        # then covers the rare case where the pump is parked elsewhere.
        if self._transport is not None:
            self.backend.close_transport(self._transport)
        self._scope.cancel()
        await self._scope.__aexit__(None, None, None)

    async def send_frame(self, data):
        async with self._send_lock:
            await self._transport.send_all(data)

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
                        # Stream-level error from the Rust state machine
                        # (`Error::Reset`): args = (stream_id, reason, initiator).
                        await self.streams.reset_on_error(se.args[0], se.args[1])
        except H2Error as exc:
            # A protocol/flow violation we detected (bad state, HPACK/CONTINUATION,
            # flow control, bad preface): notify the peer with GOAWAY, then tear down.
            await self._send_goaway(exc)
            self._fail(exc)
        except Exception as exc:  # transport error, etc. (cancellation is BaseException)
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
            if not frame.ack:
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
