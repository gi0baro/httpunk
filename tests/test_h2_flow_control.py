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


# ----- send turns (h2 prioritize.rs `pending_send` / `pending_capacity`) -----
#
# h2's connection task frames one DATA per stream per turn in `pending_send` order,
# and hands connection window to the streams waiting for it in request order
# (`assign_connection_capacity`). httpunk keeps the same two FIFOs of stream ids:
# only the front of `pending_send` frames, for up to `send_burst_frames` frames per
# turn, and a WINDOW_UPDATE(0) admits the first waiter to the rotation. Without them
# the sender that wins the state lock wins every time: eight concurrent 1 MB
# responses to a client with the RFC default 64 KB connection window went out one
# whole body after another, and with browser-wide windows in 400 KB bursts.

_FRAME = 16_384
_BURST = 4  # conn.rs DEFAULT_SEND_BURST_FRAMES: DATA frames per turn while others are active


def _frame(st, raw):
    (frame,) = list(st.receive(raw))
    return frame


def _queued_state():
    """Client state whose stream windows are wide and whose connection window is
    the default 64 KB: the connection is the only limit."""
    from httpunk._httpunk import H2Codec

    st = _client_state()
    peer = H2Codec("server")
    _feed(st, peer.serialize_settings(initial_window_size=MAX_WINDOW_SIZE))
    return st, peer


def _open(st, handle):
    # The client claims a MAX_CONCURRENT slot before opening (`_acquire_slot`); a
    # stream closed here gives it back, so it must have been counted.
    assert st.try_claim_slot()
    return st.open_stream("POST", "/", None, False, False, handle, scheme="http", authority="peer")


def _drain(st, sid, chunk):
    """Send `chunk` frame by frame until the state says no, then flush and credit as
    the write pump would (which also ends a pause at the burst bound); return the
    offset reached."""
    off = 0
    while off < len(chunk):
        v = st.send_data(sid, chunk, off, False)
        assert v.stopped is None
        if not v.sent:
            break
        off += v.sent
    st.take_pending()
    st.credit_written()
    return off


def test_connection_window_is_handed_out_in_request_order():
    st, peer = _queued_state()
    h1, h3, h5 = object(), object(), object()
    s1, s3, s5 = _open(st, h1), _open(st, h3), _open(st, h5)
    chunk = bytes(4 * DEFAULT_INITIAL_WINDOW_SIZE)

    # Nobody waits: a connection WINDOW_UPDATE wakes nobody.
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, 1))) == []
    assert st.conn_send_window == DEFAULT_INITIAL_WINDOW_SIZE + 1

    # s1 takes the whole connection window and, still wanting more, lines up.
    off1 = _drain(st, s1, chunk)
    assert off1 == DEFAULT_INITIAL_WINDOW_SIZE + 1 and st.conn_send_window == 0
    # s3 and s5 ask next: no window now, they line up behind s1.
    assert st.send_data(s3, chunk, 0, False).sent == 0
    assert st.send_data(s5, chunk, 0, False).sent == 0

    # The peer returns one frame of connection window: only the FRONT is woken...
    wake = st.recv_window_update(_frame(st, peer.serialize_window_update(0, _FRAME)))
    assert wake == [h1]
    # ...a stream behind it that tries anyway gets nothing even though window exists.
    assert st.conn_send_window == _FRAME
    assert st.send_data(s3, chunk, 0, False).sent == 0
    # The front takes it all and goes to the back of the line.
    v = st.send_data(s1, chunk, off1, False)
    assert v.sent == _FRAME and not v.done and st.conn_send_window == 0
    # Next update: s3's turn, then s5's, then s1 again — h2's rotation.
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, _FRAME))) == [h3]
    assert st.send_data(s3, chunk, 0, False).sent == _FRAME
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, _FRAME))) == [h5]
    assert st.send_data(s5, chunk, 0, False).sent == _FRAME
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, _FRAME))) == [h1]
    assert st.send_data(s1, chunk, off1 + _FRAME, False).sent == _FRAME


def test_front_completing_hands_the_connection_window_on():
    from httpunk._httpunk import H2_FLAG_CAPACITY

    st, peer = _queued_state()
    h1, h3 = object(), object()
    s1, s3 = _open(st, h1), _open(st, h3)
    chunk = bytes(DEFAULT_INITIAL_WINDOW_SIZE + 100)

    off1 = _drain(st, s1, chunk)
    assert off1 == DEFAULT_INITIAL_WINDOW_SIZE
    assert st.send_data(s3, chunk, 0, False).sent == 0
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, 1000))) == [h1]

    # s1's chunk completes with window to spare: the turn passes to s3, and the
    # verdict says so (the driver asks whom to wake).
    v = st.send_data(s1, chunk, off1, False)
    assert v.sent == 100 and v.done
    assert v.flags & H2_FLAG_CAPACITY
    assert st.next_capacity_wake() is h3
    assert st.send_data(s3, chunk, 0, False).sent == 900
    # s3 took the rest: nobody to wake now, the next WINDOW_UPDATE(0) does it.
    assert st.next_capacity_wake() is None


def test_front_whose_own_window_runs_out_gives_up_the_turn():
    from httpunk._httpunk import H2_FLAG_CAPACITY, H2Codec

    # Default 64 KB stream windows; a 1 MB connection window.
    st = _client_state()
    peer = H2Codec("server")
    h1, h3 = object(), object()
    s1, s3 = _open(st, h1), _open(st, h3)
    chunk = bytes(2 * DEFAULT_INITIAL_WINDOW_SIZE)

    # s1 spends the connection window (= its stream window): both hit zero together.
    assert _drain(st, s1, chunk) == DEFAULT_INITIAL_WINDOW_SIZE
    assert st.conn_send_window == 0 and st.stream_send_window(s1) == 0
    assert st.send_data(s3, chunk, 0, False).sent == 0  # lines up (its own window is open)
    # s1 is NOT in line — its own window is the limit — so s3 is the front.
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, 1 << 20))) == [h3]
    assert st.send_data(s3, chunk, 0, False).sent == _FRAME
    # s1's stream window returns while it is out of line: it sends when it asks, no turn
    # needed (the connection has room) — and s3's turn is not disturbed.
    st.recv_window_update(_frame(st, peer.serialize_window_update(s1, _FRAME)))
    v = st.send_data(s1, chunk, DEFAULT_INITIAL_WINDOW_SIZE, False)
    assert v.sent == 0  # s3 still holds the turn: it has window and a chunk to finish
    # s3 sends the rest of its stream window (a fresh chunk from offset 0: what the
    # window still allows) and runs out of it.
    assert _drain(st, s3, chunk) == DEFAULT_INITIAL_WINDOW_SIZE - _FRAME
    assert st.stream_send_window(s3) == 0
    # s3 gave the turn up when its own window ran out: s1 is next.
    assert st.next_capacity_wake() is h1
    assert st.send_data(s1, chunk, DEFAULT_INITIAL_WINDOW_SIZE, False).sent == _FRAME
    assert not (st.send_data(s3, chunk, DEFAULT_INITIAL_WINDOW_SIZE, False).flags & H2_FLAG_CAPACITY)


def test_front_reset_hands_the_connection_window_on():
    from httpunk._httpunk import H2_FLAG_CAPACITY

    st, peer = _queued_state()
    h1, h3 = object(), object()
    s1, s3 = _open(st, h1), _open(st, h3)
    chunk = bytes(4 * DEFAULT_INITIAL_WINDOW_SIZE)

    _drain(st, s1, chunk)
    assert st.send_data(s3, chunk, 0, False).sent == 0
    assert st.recv_window_update(_frame(st, peer.serialize_window_update(0, _FRAME))) == [h1]
    # The front is cancelled before it takes its turn: the reset's verdict passes the
    # turn on, so s3 does not wait for a WINDOW_UPDATE that may never come.
    v = st.reset_stream(s1, 8, "user")
    assert v.flags & H2_FLAG_CAPACITY
    assert st.next_capacity_wake() is h3
    assert st.send_data(s3, chunk, 0, False).sent == _FRAME


def test_send_turns_rotate_after_burst_frames():
    from httpunk._httpunk import H2_FLAG_CAPACITY

    st, peer = _queued_state()
    _feed(st, peer.serialize_window_update(0, MAX_WINDOW_SIZE - DEFAULT_INITIAL_WINDOW_SIZE))
    h1, h3 = object(), object()
    s1 = _open(st, h1)
    chunk = bytes(4 * _BURST * _FRAME)
    turn = _BURST * _FRAME

    # Alone on the connection, a stream is never bounded: the whole chunk in one loop.
    assert _drain(st, s1, chunk) == len(chunk)

    # A second stream is active but has not lined up (it has not run yet): the
    # front pauses at the bound — no hand-off, nobody to hand to — until the flush
    # credit, which is what gives the other stream the thread.
    s3 = _open(st, h3)
    off = 0
    for _ in range(_BURST):
        off += st.send_data(s1, chunk, off, False).sent
    assert off == turn
    v = st.send_data(s1, chunk, off, False)
    assert v.sent == 0 and not v.flags & H2_FLAG_CAPACITY
    assert st.send_data(s3, chunk, 0, False).sent == 0  # s3 lines up behind the paused front
    st.take_pending()
    st.credit_written()  # the pump flushed: s1's pause ends

    # Two streams with chunks: one turn each, hand-off through the verdict.
    for _ in range(_BURST):
        off += st.send_data(s1, chunk, off, False).sent
    assert off == 2 * turn
    v = st.send_data(s1, chunk, off, False)
    assert v.sent == 0 and v.flags & H2_FLAG_CAPACITY  # turn over
    assert st.next_capacity_wake() is h3
    off3 = 0
    for _ in range(_BURST):
        off3 += st.send_data(s3, chunk, off3, False).sent
    assert off3 == turn
    v = st.send_data(s3, chunk, off3, False)
    assert v.sent == 0 and v.flags & H2_FLAG_CAPACITY
    assert st.next_capacity_wake() is h1
    # s1 finishes its chunk (one turn left) and leaves the rotation: s3's turn.
    assert _drain(st, s1, chunk[: 3 * turn][off:]) == turn
    assert st.next_capacity_wake() is h3
