"""SETTINGS synchronization — a port of h2's `proto/settings.rs`.

`Settings` tracks the local/remote SETTINGS handshake state machine (h2's
`Local` enum + pending-remote slot). The *application* of setting values
(HPACK table sizes, flow-control windows, stream limits) is performed by the
connection driver using the frames this returns — mirroring h2, where
`settings.rs` delegates apply to the codec + streams manager.

`PeerSettings` / `LocalSettings` hold the negotiated values with RFC 7540 §6.5.2
defaults.

Cross-reference: all `h2 ...` comments below cite hyperium/h2 v0.4.15 (the
vendored version — see src/h2/UPSTREAM_VERSION), path relative to its `src/`.
"""

from enum import Enum

from .._httpunk import H2ProtocolError, H2Reason as Reason, H2UserError


# RFC 7540 §6.5.2 defaults.
DEFAULT_HEADER_TABLE_SIZE = 4096
DEFAULT_INITIAL_WINDOW_SIZE = 65_535
DEFAULT_MAX_FRAME_SIZE = 16_384


class _Local(Enum):
    TO_SEND = "to_send"
    WAITING_ACK = "waiting_ack"
    SYNCED = "synced"


class Action(Enum):
    """What `recv_settings` tells the driver to do next."""

    APPLY_LOCAL = "apply_local"  # peer ACKed our SETTINGS; apply our own values
    ACK_AND_APPLY = "ack_and_apply"  # peer sent SETTINGS; write ACK, then apply theirs


class PeerSettings:
    """The peer's SETTINGS as currently known to us (their limits on what we send)."""

    __slots__ = (
        "header_table_size",
        "initial_window_size",
        "max_frame_size",
        "max_concurrent_streams",
        "enable_push",
        "max_header_list_size",
    )

    def __init__(self):
        self.header_table_size = DEFAULT_HEADER_TABLE_SIZE
        self.initial_window_size = DEFAULT_INITIAL_WINDOW_SIZE
        self.max_frame_size = DEFAULT_MAX_FRAME_SIZE
        self.max_concurrent_streams = None  # unlimited
        self.enable_push = True
        self.max_header_list_size = None

    def update(self, frame):
        """Fold a received peer SETTINGS frame into the current values.

        Returns the old initial_window_size if it changed (so the driver can
        adjust open streams' send windows per RFC 7540 §6.9.2), else None.

        h2 applies these values across several sites: proto/streams/send.rs
        `apply_remote_settings` (L478-560, incl. the §6.9.2 initial-window-size
        delta) and proto/streams/counts.rs `apply_remote_settings` (L180).
        """
        window_delta_old = None
        if frame.header_table_size is not None:
            self.header_table_size = frame.header_table_size
        if frame.max_frame_size is not None:
            self.max_frame_size = frame.max_frame_size
        if frame.max_concurrent_streams is not None:
            self.max_concurrent_streams = frame.max_concurrent_streams
        if frame.enable_push is not None:
            self.enable_push = frame.enable_push
        if frame.max_header_list_size is not None:
            self.max_header_list_size = frame.max_header_list_size
        if frame.initial_window_size is not None and frame.initial_window_size != self.initial_window_size:
            window_delta_old = self.initial_window_size
            self.initial_window_size = frame.initial_window_size
        return window_delta_old


class LocalSettings:
    """The SETTINGS we sent, applied once the peer ACKs them.

    h2 applies these in proto/settings.rs `recv_settings` ACK branch (L52-77:
    codec recv frame/header-list/table sizes) + proto/streams/recv.rs
    `apply_local_settings` (L563).
    """

    __slots__ = ("header_table_size", "initial_window_size", "max_frame_size", "max_header_list_size")

    def __init__(
        self,
        *,
        header_table_size=None,
        initial_window_size=None,
        max_frame_size=None,
        max_header_list_size=None,
    ):
        self.header_table_size = header_table_size
        self.initial_window_size = initial_window_size
        self.max_frame_size = max_frame_size
        self.max_header_list_size = max_header_list_size


class Settings:
    """Local/remote SETTINGS synchronization state machine.

    h2: proto/settings.rs — `Settings` struct (L7-17), `Local` enum (L20-28),
    `new` (L31-39). Constructed with the local SETTINGS we already flushed
    during the handshake, so we start in `WaitingAck`.
    """

    def __init__(self, local: LocalSettings):
        self._state = _Local.WAITING_ACK
        self._local = local
        self._remote_pending = None
        self._has_received_remote_initial = False

    def recv_settings(self, frame):
        """Handle a received SETTINGS frame. Returns `(Action, payload)`:
        - ACK  -> (APPLY_LOCAL, LocalSettings) : apply our own settings now.
        - data -> (ACK_AND_APPLY, frame)       : write ACK, then apply the peer's.

        h2: proto/settings.rs `recv_settings` (L41-88); ACK branch L53-80.
        """
        if frame.ack:
            if self._state is not _Local.WAITING_ACK:
                # We ACK'd nothing outstanding — remote is buggy or malicious.
                raise H2ProtocolError(int(Reason.PROTOCOL_ERROR), "received unexpected settings ack")
            self._state = _Local.SYNCED
            return (Action.APPLY_LOCAL, self._local)
        # We always ACK before reading further frames, so this must be empty.
        assert self._remote_pending is None, "unACK'd remote SETTINGS still pending"
        self._remote_pending = frame
        return (Action.ACK_AND_APPLY, frame)

    def take_remote(self):
        """Consume the pending remote SETTINGS. Returns `(frame, is_initial)`.

        h2: proto/settings.rs `mark_remote_initial_settings_as_received`
        (L105-110); the ACK-then-apply ordering lives in `poll_send` (L111-168).
        """
        frame = self._remote_pending
        self._remote_pending = None
        is_initial = not self._has_received_remote_initial
        self._has_received_remote_initial = True
        return frame, is_initial

    def send_settings(self, local: LocalSettings):
        """Queue a new local SETTINGS to send (only valid once Synced).

        h2: proto/settings.rs `send_settings` (L90-100).
        """
        if self._state in (_Local.TO_SEND, _Local.WAITING_ACK):
            raise H2UserError("send_settings_while_pending", "sending SETTINGS before the previous ACK")
        self._state = _Local.TO_SEND
        self._local = local

    @property
    def has_received_remote_initial(self):
        return self._has_received_remote_initial
