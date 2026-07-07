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
