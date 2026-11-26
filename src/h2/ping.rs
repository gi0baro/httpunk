//! The client's HTTP/2 keep-alive: hyper proto/h2/ping.rs (1.13.0) `KeepAlive` and the
//! keep-alive half of its `Shared`, run synchronously inside the connection state
//! (`conn.rs`). hyper's `Sleep` is the instant it fires (`sleep`): polling it is
//! `now < sleep`, `timer.reset` is an assignment. The PING is h2's user PING, which
//! the state owns (`Inner::send_user_ping`), so sending it is the caller's. BDP is not
//! ported.

use std::time::{Duration, Instant};

/// DIVERGENCE: hyper takes any interval and timeout. The timer task sleeps the timeout
/// between two questions on an idle connection, and at most the interval while a PING
/// is out (`next_poll`), so a shorter one would have it wake that often, and zero would
/// spin it: both are raised to this (maintainer's decision).
const MIN_DURATION: Duration = Duration::from_secs(1);

/// ping.rs:155-166, with the keep-alive fields of `Shared` (ping.rs:119-138).
pub(super) struct KeepAlive {
    /// If no frames are received in this amount of time, a PING frame is sent.
    interval: Duration,
    /// After sending a keepalive PING, the connection will be closed if a pong is
    /// not received in this amount of time.
    timeout: Duration,
    /// If true, sends pings even when there are no active streams.
    while_idle: bool,
    state: KeepAliveState,
    sleep: Instant,
    last_read_at: Instant,
    /// `Shared::ping_sent_at.is_some()`: its instant is read only by BDP (ping.rs:283-287).
    ping_sent: bool,
}

/// ping.rs:168-172.
enum KeepAliveState {
    Init,
    Scheduled(Instant),
    PingSent,
}

/// ping.rs:179-180.
pub(super) struct KeepAliveTimedOut;

/// What `maybe_ping` leaves to its caller.
pub(super) enum MaybePing {
    /// Nothing is due.
    No,
    /// hyper wakes its own task to be polled again (ping.rs:469).
    Again,
    /// The PING is due: send it (ping.rs:477, `Shared::send_ping`).
    Send,
}

impl KeepAlive {
    /// ping.rs:55, 63-72.
    pub(super) fn new(interval: Duration, timeout: Duration, while_idle: bool) -> Self {
        let now = Instant::now();
        KeepAlive {
            interval: interval.max(MIN_DURATION),
            timeout: timeout.max(MIN_DURATION),
            while_idle,
            state: KeepAliveState::Init,
            sleep: now + interval,
            last_read_at: now,
            ping_sent: false,
        }
    }

    /// hyper builds the keep-alive at the handshake (proto/h2/client.rs:171-176), so
    /// its clock starts there, not at the connection's construction.
    pub(super) fn start(&mut self, now: Instant) {
        self.sleep = now + self.interval;
        self.last_read_at = now;
    }

    /// ping.rs:350-354 `Shared::update_last_read_at`.
    pub(super) fn update_last_read_at(&mut self, now: Instant) {
        self.last_read_at = now;
    }

    /// ping.rs:346-348 `Shared::is_ping_sent`.
    pub(super) fn is_ping_sent(&self) -> bool {
        self.ping_sent
    }

    /// ping.rs:283-291, keep-alive parts: the pong of our PING arrived.
    pub(super) fn ponged(&mut self, now: Instant) {
        self.ping_sent = false;
        self.update_last_read_at(now);
    }

    /// ping.rs:435-452.
    pub(super) fn maybe_schedule(&mut self, is_idle: bool) {
        match self.state {
            KeepAliveState::Init => {
                if !self.while_idle && is_idle {
                    return;
                }
                self.schedule();
            }
            KeepAliveState::PingSent => {
                if self.is_ping_sent() {
                    return;
                }
                self.schedule();
            }
            KeepAliveState::Scheduled(..) => (),
        }
    }

    /// ping.rs:454-458.
    fn schedule(&mut self) {
        let interval = self.last_read_at + self.interval;
        self.state = KeepAliveState::Scheduled(interval);
        self.sleep = interval;
    }

    /// ping.rs:460-484. The PING is h2's, which the connection state owns: sending it
    /// is the caller's (`MaybePing::Send`), with `set_ping_sent` once it is queued.
    pub(super) fn maybe_ping(&mut self, now: Instant, is_idle: bool) -> MaybePing {
        match self.state {
            KeepAliveState::Scheduled(at) => {
                if now < self.sleep {
                    return MaybePing::No;
                }
                // check if we've received a frame while we were scheduled
                if self.last_read_at + self.interval > at {
                    self.state = KeepAliveState::Init;
                    return MaybePing::Again;
                }
                if !self.while_idle && is_idle {
                    return MaybePing::No;
                }
                self.state = KeepAliveState::PingSent;
                self.sleep = now + self.timeout;
                MaybePing::Send
            }
            KeepAliveState::Init | KeepAliveState::PingSent => MaybePing::No,
        }
    }

    /// ping.rs:336-337 `Shared::send_ping`, the PING queued.
    pub(super) fn set_ping_sent(&mut self) {
        self.ping_sent = true;
    }

    /// ping.rs:486-497.
    pub(super) fn maybe_timeout(&self, now: Instant) -> Result<(), KeepAliveTimedOut> {
        match self.state {
            KeepAliveState::PingSent => {
                if now < self.sleep {
                    return Ok(());
                }
                Err(KeepAliveTimedOut)
            }
            KeepAliveState::Init | KeepAliveState::Scheduled(..) => Ok(()),
        }
    }

    /// How long the timer task may sleep before asking again, after a poll.
    ///
    /// DIVERGENCE: hyper's ponger is also polled on every wake of its connection task;
    /// the timer task (`H2ConnectionBase._keep_alive`) is woken by nothing but this
    /// answer (runtime-forced), so the answer is never later than the next instant
    /// hyper's `Sleep` could fire: the pong can move it to `pong + interval`
    /// (ping.rs:290-293), and a stream opening while the PING is gated by idleness
    /// schedules it for `last_read_at + interval` or pings at once (`open_stream`).
    pub(super) fn next_poll(&self, now: Instant) -> Duration {
        match self.state {
            KeepAliveState::Scheduled(at) if at > now => at - now,
            KeepAliveState::PingSent => {
                self.sleep.saturating_duration_since(now).min(self.interval)
            }
            // Gated by idleness (ping.rs:438-440).
            KeepAliveState::Init => {
                let at = self.last_read_at + self.interval;
                if at > now { at - now } else { self.timeout }
            }
            // Elapsed, gated by idleness (ping.rs:472-475).
            KeepAliveState::Scheduled(..) => self.timeout,
        }
    }
}
