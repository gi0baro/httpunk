"""HTTP/2 protocol driver — h2: proto/connection.rs.

The thin connection core: owns the transport + shared codec, runs the read-pump,
dispatches inbound frames to the `StreamManager`, drives the SETTINGS handshake
(`settings.py`), and answers PING / handles GOAWAY. All per-stream logic and
flow control live in the `StreamManager` (streams.py); the public request API
lives in `client.py`.

Cross-reference: `h2 ...` comments cite hyperium/h2 v0.4.15 (see
src/h2/UPSTREAM_VERSION). This is an *adaptation*: h2 drives everything from one
polled `Connection` future; we use a coroutine read-pump.
"""

import contextlib

from .._backend.tonio import TonioBackend
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
from .settings import Action, LocalSettings, Settings
from .streams import StreamManager, _StreamError


_READ_SIZE = 65536

# The HTTP/2 client connection preface (RFC 7540 §3.5). A fixed, protocol-level
# constant (not HPACK, not runtime-specific) — it belongs with the h2 driver that
# sends it, not the transport backend.
PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


class Connection:
    """The protocol driver for one transport. Created and driven by the public
    `client.H2Connection`."""

    def __init__(self, transport, *, authority=None, backend=None, initial_window_size=None):
        # `transport` is a caller-supplied, already-connected byte stream (BYO
        # transport, like hyper's `client::conn`). `authority` is used to build
        # the :authority pseudo-header for requests given a bare path.
        self.authority = authority
        self.backend = backend or TonioBackend()
        self.codec = H2Codec("client")
        self.error = None

        self._scope = self.backend.scope()
        self._send_lock = self.backend.lock()
        self._transport = transport
        self._initial_window_size = initial_window_size  # our advertised per-stream recv window
        # Signalled once the peer's initial SETTINGS have been applied, so
        # requests respect the peer's limits/window from the first one.
        self._ready_evt = self.backend.event()
        # SETTINGS sync (proto/settings.rs). We advertise SETTINGS_ENABLE_PUSH=0.
        self._settings = Settings(LocalSettings(initial_window_size=initial_window_size))

        self.streams = StreamManager(self)

    async def connect(self):
        # h2: client.rs `handshake` (L1220) — over the caller-supplied transport,
        # flush the client preface + our initial SETTINGS, spawn the driver, then
        # wait for the peer's initial SETTINGS (its connection preface) before
        # we're ready for requests. (Dialing/TLS/ALPN are the caller's job.)
        await self._scope.__aenter__()
        settings = {"enable_push": False}
        if self._initial_window_size is not None:
            settings["initial_window_size"] = self._initial_window_size
        await self.send_frame(PREFACE + self.codec.serialize_settings(**settings))
        self._scope.spawn(self._read_pump())
        await self._ready_evt.wait()
        if self.error is not None:
            raise self.error

    async def close(self):
        # Close the transport *first*: the read-pump is almost always parked in
        # `transport.receive_some`, and closing makes that return EOF so the pump
        # exits on its own — deterministic, rather than relying on cancellation to
        # interrupt a native socket poll (which races and can leave the scope's
        # `__aexit__` waiting on a task that never wakes → the intermittent
        # teardown hang). The EOF also unblocks the peer's read loop. `cancel()`
        # then covers the rare case where the pump is parked elsewhere.
        if self._transport is not None:
            self._transport.close()
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
            # flow control): notify the peer with GOAWAY, then tear down.
            await self._send_goaway(exc)
            self._fail(exc)
        except Exception as exc:  # transport error, etc. (cancellation is BaseException)
            self._fail(exc)

    async def _send_goaway(self, exc):
        # h2: proto/connection.rs `go_away_now` (L409) / `go_away_now_data` (L415).
        # Client GOAWAY carries last_stream_id = 0 (we process no peer-initiated
        # streams — no server push). Reason comes from the error if it has one.
        reason = exc.args[0] if exc.args and isinstance(exc.args[0], int) else int(H2Reason.PROTOCOL_ERROR)
        # Best effort: the connection is going down regardless of whether this lands.
        with contextlib.suppress(Exception):
            await self.send_frame(self.codec.serialize_go_away(0, reason))

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
                self._ready_evt.set()  # connection is now fully established

    def _fail(self, exc):
        # h2: proto/connection.rs `handle_poll2_result` (L430) -> the connection
        # error fans out to every stream via streams.rs `Streams::handle_error` (L362).
        self.error = exc
        self.streams.fail_all(exc)
        self._ready_evt.set()  # unblock connect() if the handshake never completed
