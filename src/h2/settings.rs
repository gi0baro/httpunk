//! SETTINGS synchronization — the mirror of h2 `proto/settings.rs` (0.4.19).
//!
//! `Settings` tracks the local SETTINGS handshake (h2's `Local` enum: `ToSend` ->
//! `WaitingAck` -> `Synced`) and whether the peer's initial SETTINGS has landed;
//! `PeerSettings` holds the peer's values with the RFC 7540 §6.5.2 defaults. As
//! upstream, the *application* of the values (HPACK table sizes, flow-control
//! windows, stream limits) is not here: `settings.rs` delegates it to the codec and
//! the streams layer (`Inner::apply_remote_settings` / `apply_local_settings` in
//! conn.rs), and both live under the connection's one mutex, so a remote SETTINGS
//! is ACKed and folded into every open stream's window in one step.

use pyo3::prelude::*;

use vendor_h2::frame::{self, Reason};

use super::codec::SettingsValues;
use super::errors::H2ProtocolError;

/// h2 settings.rs `Local` (L20-28): the state of our own SETTINGS.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Local {
    /// Our SETTINGS is to be queued (h2 `Local::ToSend`).
    ToSend,
    /// Queued; the peer's ACK is outstanding (`Local::WaitingAck`).
    WaitingAck,
    /// ACKed and applied (`Local::Synced`).
    Synced,
}

/// h2 settings.rs `Settings` (L7-17): the local handshake state machine. Constructed
/// with the values we advertise, before they are queued (`ToSend`).
pub(super) struct Settings {
    local: SettingsValues,
    state: Local,
    /// h2 `has_received_remote_initial` (L105-110).
    has_received_remote_initial: bool,
}

impl Settings {
    pub(super) fn new(local: SettingsValues) -> Self {
        Settings {
            local,
            state: Local::ToSend,
            has_received_remote_initial: false,
        }
    }

    /// The values we advertise (applied for *receiving* once the peer ACKs).
    pub(super) fn local(&self) -> &SettingsValues {
        &self.local
    }

    /// Our SETTINGS frame was queued: `ToSend` -> `WaitingAck` (h2 `poll_send` L111-168).
    pub(super) fn mark_sent(&mut self) {
        self.state = Local::WaitingAck;
    }

    /// The peer ACKed our SETTINGS (h2 `recv_settings` ACK branch, L52-80): our values
    /// now take effect for receiving (the caller applies them). An ACK with nothing
    /// outstanding is a connection PROTOCOL_ERROR (L57-63: "received unexpected
    /// settings ack").
    pub(super) fn recv_ack(&mut self) -> PyResult<()> {
        if self.state != Local::WaitingAck {
            return Err(H2ProtocolError::new_err((
                Some(u32::from(Reason::PROTOCOL_ERROR)),
                "received unexpected settings ack".to_string(),
            )));
        }
        self.state = Local::Synced;
        Ok(())
    }

    /// The peer sent SETTINGS (h2 `recv_settings` L81-88 + `mark_remote_initial_
    /// settings_as_received` L105-110). Returns whether it was the peer's initial one
    /// (the connection is then fully established). The caller queues the ACK before
    /// applying the values, as h2's `poll_send` does.
    pub(super) fn recv_remote(&mut self) -> bool {
        let initial = !self.has_received_remote_initial;
        self.has_received_remote_initial = true;
        initial
    }
}

/// The peer's SETTINGS as currently known to us (their limits on what we send),
/// with the RFC 7540 §6.5.2 defaults. h2 spreads these across the codec
/// (`set_max_send_frame_size`, header table), `send.rs` (`init_window_sz`) and
/// `counts.rs` (`max_send_streams`); one struct here, applied by the same code.
pub(super) struct PeerSettings {
    pub header_table_size: u32,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_concurrent_streams: Option<u32>,
}

impl PeerSettings {
    pub(super) fn new() -> Self {
        PeerSettings {
            header_table_size: frame::DEFAULT_SETTINGS_HEADER_TABLE_SIZE as u32,
            initial_window_size: frame::DEFAULT_INITIAL_WINDOW_SIZE,
            max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
            max_concurrent_streams: None,
        }
    }
}
