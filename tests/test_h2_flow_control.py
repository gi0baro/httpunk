"""Flow-control window math. Vendored from h2 (proto/streams/flow_control.rs)
and exposed as `H2FlowControl`; these tests pin its behaviour."""

import pytest

from httpunk._httpunk import H2FlowControl, H2FlowControlError


DEFAULT_INITIAL_WINDOW_SIZE = 65_535
MAX_WINDOW_SIZE = (1 << 31) - 1


def test_new_is_zero():
    fc = H2FlowControl()
    assert fc.window_size() == 0
    assert fc.available() == 0
    assert not fc.has_unavailable()


def test_inc_window_and_assign_capacity():
    fc = H2FlowControl()
    fc.inc_window(DEFAULT_INITIAL_WINDOW_SIZE)
    fc.assign_capacity(DEFAULT_INITIAL_WINDOW_SIZE)
    assert fc.window_size() == DEFAULT_INITIAL_WINDOW_SIZE
    assert fc.available() == DEFAULT_INITIAL_WINDOW_SIZE
    assert not fc.has_unavailable()


def test_inc_window_overflow_is_flow_control_error():
    fc = H2FlowControl()
    fc.inc_window(MAX_WINDOW_SIZE)
    with pytest.raises(H2FlowControlError):
        fc.inc_window(1)


def test_send_data_decrements_both_windows():
    fc = H2FlowControl()
    fc.inc_window(1000)
    fc.assign_capacity(1000)
    fc.send_data(400)
    assert fc.window_size() == 600
    assert fc.available() == 600


def test_send_data_requires_capacity():
    fc = H2FlowControl()
    fc.inc_window(100)
    with pytest.raises(H2FlowControlError):
        fc.send_data(200)


def test_unclaimed_capacity_threshold():
    fc = H2FlowControl()
    fc.assign_capacity(1000)
    fc.inc_window(400)
    # unclaimed = 1000 - 400 = 600; threshold = 400/2 = 200; 600 >= 200 -> emit.
    assert fc.unclaimed_capacity() == 600


def test_unclaimed_capacity_below_threshold_returns_none():
    fc = H2FlowControl()
    fc.assign_capacity(1000)
    fc.inc_window(900)
    # unclaimed = 100; threshold = 900/2 = 450; 100 < 450 -> None.
    assert fc.unclaimed_capacity() is None


def test_dec_recv_window_can_go_negative():
    fc = H2FlowControl()
    fc.inc_window(100)
    fc.assign_capacity(100)
    fc.dec_recv_window(150)
    assert fc.window_size() == 0  # as_size clamps negative to 0
    assert fc.available() == -50


# ----- connection-level send bookkeeping (h2 prioritize.rs) -----
#
# `FlowControl::send_data` decrements both `window_size` and `available`. h2 opens
# each send window with its capacity assigned (`Prioritize::new`,
# `recv_connection_window_update`, `try_assign_capacity`); httpunk reserves at the
# send instead and keeps `available` equal to the window. These tests pin that
# `available` never drifts: a peer that keeps granting window can be sent more
# than 2 GiB on one connection and one stream (i32 range of a `Window`).

_MAX_FRAME = (1 << 24) - 1
_GIB = 1 << 30


def _client_state():
    from httpunk._httpunk import H2Streams

    st = H2Streams(
        "client",
        initial_window_size=DEFAULT_INITIAL_WINDOW_SIZE,
        connection_window=DEFAULT_INITIAL_WINDOW_SIZE,
        max_frame_size=16_384,
        max_header_list_size=16_384,
        max_send_buf_size=1 << 31,
    )
    st.begin()
    return st


def _feed(st, data):
    """Parse peer bytes and dispatch the frames the way the read pump does."""
    from httpunk._httpunk import H2FrameSettings, H2FrameWindowUpdate

    for frame in st.receive(data):
        if isinstance(frame, H2FrameSettings):
            st.recv_settings(frame)
        elif isinstance(frame, H2FrameWindowUpdate):
            st.recv_window_update(frame)
        else:
            raise AssertionError(f"unexpected frame {frame!r}")


def test_sending_past_2gib_on_one_connection_and_stream():
    from httpunk._httpunk import H2Codec

    st = _client_state()
    peer = H2Codec("server")
    # The peer opens the largest windows and frames the protocol allows.
    _feed(st, peer.serialize_settings(initial_window_size=MAX_WINDOW_SIZE, max_frame_size=_MAX_FRAME))
    _feed(st, peer.serialize_window_update(0, MAX_WINDOW_SIZE - DEFAULT_INITIAL_WINDOW_SIZE))
    assert st.conn_send_window == MAX_WINDOW_SIZE

    sid = st.open_stream("POST", "/", None, False, False, object(), scheme="http", authority="peer")
    assert st.stream_send_window(sid) == MAX_WINDOW_SIZE
    chunk = bytes(_MAX_FRAME)
    sent = 0
    while sent <= 2 * _GIB + _MAX_FRAME:  # past the i32 range of a `Window`
        v = st.send_data(sid, chunk, 0, False)
        assert v.stopped is None and v.done and v.sent == _MAX_FRAME
        sent += v.sent
        st.take_pending()  # the write pump: drain, then credit the send buffer back
        st.credit_written()
        # The peer read the frame: it hands the window back on both levels.
        _feed(st, peer.serialize_window_update(0, _MAX_FRAME))
        _feed(st, peer.serialize_window_update(sid, _MAX_FRAME))
        assert st.conn_send_window == MAX_WINDOW_SIZE
        assert st.stream_send_window(sid) == MAX_WINDOW_SIZE

    v = st.send_data(sid, b"", 0, True)
    assert v.stopped is None and v.done


def test_initial_window_size_delta_moves_capacity_with_the_window():
    from httpunk._httpunk import H2Codec

    st = _client_state()
    peer = H2Codec("server")
    _feed(st, peer.serialize_settings(initial_window_size=MAX_WINDOW_SIZE, max_frame_size=_MAX_FRAME))
    _feed(st, peer.serialize_window_update(0, MAX_WINDOW_SIZE - DEFAULT_INITIAL_WINDOW_SIZE))
    sid = st.open_stream("POST", "/", None, False, False, object(), scheme="http", authority="peer")
    chunk = bytes(_MAX_FRAME)
    v = st.send_data(sid, chunk, 0, False)
    assert v.sent == _MAX_FRAME
    st.take_pending()
    st.credit_written()

    # The peer shrinks the stream window below what is in flight (RFC 9113 §6.9.2):
    # the window goes negative and the capacity is claimed back with it.
    _feed(st, peer.serialize_settings(initial_window_size=DEFAULT_INITIAL_WINDOW_SIZE))
    assert st.stream_send_window(sid) == 0  # clamped: 65535 - 16 MiB < 0
    v = st.send_data(sid, chunk, 0, False)
    assert v.sent == 0 and not v.done  # no window now

    # ...and grows it again: the delta is assigned as capacity too, so the next
    # send is reserved against the full window, not a stale `available`.
    _feed(st, peer.serialize_settings(initial_window_size=MAX_WINDOW_SIZE))
    assert st.stream_send_window(sid) == MAX_WINDOW_SIZE - _MAX_FRAME
    v = st.send_data(sid, chunk, 0, True)
    assert v.stopped is None and v.done and v.sent == _MAX_FRAME
