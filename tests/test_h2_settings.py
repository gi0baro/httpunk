"""SETTINGS synchronization state machine (port of h2 proto/settings.rs)."""

from types import SimpleNamespace

import pytest

from httpunk._httpunk import H2ProtocolError, H2UserError
from httpunk.h2.settings import Action, LocalSettings, PeerSettings, Settings
from httpunk.h2.streams import StreamManager


def _frame(ack=False, **values):
    """A stand-in for the Rust `Settings` frame event."""
    defaults = {
        "header_table_size": None,
        "initial_window_size": None,
        "max_frame_size": None,
        "max_concurrent_streams": None,
        "enable_push": None,
        "max_header_list_size": None,
    }
    defaults.update(values)
    return SimpleNamespace(ack=ack, **defaults)


def test_ack_applies_local_then_synced():
    s = Settings(LocalSettings(header_table_size=8192))
    action, local = s.recv_settings(_frame(ack=True))
    assert action is Action.APPLY_LOCAL
    assert local.header_table_size == 8192
    # A second ACK is unexpected (nothing outstanding) -> protocol error.
    with pytest.raises(H2ProtocolError):
        s.recv_settings(_frame(ack=True))


def test_remote_settings_stored_for_ack_and_apply():
    s = Settings(LocalSettings())
    frame = _frame(initial_window_size=100_000, max_concurrent_streams=128)
    action, pending = s.recv_settings(frame)
    assert action is Action.ACK_AND_APPLY
    assert pending is frame

    taken, is_initial = s.take_remote()
    assert taken is frame
    assert is_initial is True
    assert s.has_received_remote_initial

    # A second remote SETTINGS is no longer the initial one.
    s.recv_settings(_frame(max_frame_size=32_768))
    _, is_initial2 = s.take_remote()
    assert is_initial2 is False


def test_send_settings_requires_synced():
    s = Settings(LocalSettings())  # starts WaitingAck
    with pytest.raises(H2UserError):
        s.send_settings(LocalSettings(initial_window_size=1))
    s.recv_settings(_frame(ack=True))  # -> Synced
    s.send_settings(LocalSettings(initial_window_size=1))  # now allowed


def test_peer_settings_defaults_and_update():
    ps = PeerSettings()
    assert ps.initial_window_size == 65_535
    assert ps.max_frame_size == 16_384
    assert ps.max_concurrent_streams is None

    # First update: window changes from the default.
    old = ps.update(_frame(initial_window_size=1_000_000, max_concurrent_streams=100, header_table_size=8192))
    assert old == 65_535
    assert ps.initial_window_size == 1_000_000
    assert ps.max_concurrent_streams == 100
    assert ps.header_table_size == 8192

    # A frame that doesn't touch the window returns None (no adjustment needed).
    assert ps.update(_frame(max_frame_size=32_768)) is None
    assert ps.max_frame_size == 32_768


class _FakeSendFlow:
    def __init__(self):
        self.inc = []
        self.dec = []

    def inc_window(self, n):
        self.inc.append(n)

    def dec_send_window(self, n):
        self.dec.append(n)


class _FakeStream:
    def __init__(self, send_closed):
        self.state = SimpleNamespace(is_send_closed=lambda: send_closed)
        self.send_flow = _FakeSendFlow()
        self.window_evt = SimpleNamespace(set=lambda: None)


def test_adjust_send_windows_skips_send_closed_streams():
    """A SETTINGS_INITIAL_WINDOW_SIZE change adjusts open streams' send windows but
    SKIPS send-closed ones — matching h2 (its decrease branch guards
    is_send_closed()), which avoids pointlessly adjusting a window we'll never use and a
    needless inc_window overflow teardown on the increase side (F41)."""
    mgr = object.__new__(StreamManager)  # bypass __init__: we only exercise _streams
    open_st = _FakeStream(send_closed=False)
    closed_st = _FakeStream(send_closed=True)
    mgr._streams = {1: open_st, 3: closed_st}

    mgr._adjust_send_windows(1000, 3000)  # increase by 2000
    assert open_st.send_flow.inc == [2000]  # applied to the open stream
    assert closed_st.send_flow.inc == []  # send-closed stream skipped

    mgr._adjust_send_windows(3000, 1000)  # decrease by 2000
    assert open_st.send_flow.dec == [2000]
    assert closed_st.send_flow.dec == []  # skipped here too
