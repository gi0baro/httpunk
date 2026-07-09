"""Stream state machine transitions. The machine is vendored from h2
(proto/streams/state.rs) and exposed as `H2StreamState`; these tests pin its
behaviour against the h2 state diagram. Pure logic, no I/O."""

import pytest

from httpunk._httpunk import (
    H2ProtocolError,
    H2Reason as Reason,
    H2StreamError,
    H2StreamState,
    H2UserError,
)


def test_default_is_idle():
    s = H2StreamState()
    assert s.is_idle()
    assert not s.is_closed()
    assert s.is_recv_headers()  # Idle can receive HEADERS


def test_client_request_with_body_then_response():
    s = H2StreamState()
    s.send_open(eos=False)  # HEADERS without END_STREAM opens the send half
    assert s.is_send_streaming()
    assert not s.is_send_closed()
    assert s.is_recv_headers()

    s.send_close()  # DATA with END_STREAM closes the send half
    assert s.is_send_closed()
    assert not s.is_closed()

    initial = s.recv_open(eos=False, informational=False)  # response HEADERS
    assert initial is False
    assert s.is_recv_streaming()

    s.recv_close()  # response END_STREAM
    assert s.is_closed()
    assert s.is_recv_end_stream()
    assert not s.is_reset()


def test_bodyless_get_headers_with_eos_reaches_half_closed_local():
    s = H2StreamState()
    s.send_open(eos=True)
    assert s.is_send_closed()
    assert s.is_recv_headers()
    s.recv_open(eos=True, informational=False)  # full bodyless response
    assert s.is_closed()
    assert s.is_recv_end_stream()
    assert not s.is_reset()


def test_server_receives_request():
    s = H2StreamState()
    initial = s.recv_open(eos=False, informational=False)
    assert initial is True
    assert s.is_recv_streaming()
    assert s.is_recv_headers() is False


def test_informational_1xx_keeps_awaiting_headers():
    s = H2StreamState()
    s.send_open(eos=False)
    s.recv_open(eos=False, informational=True)  # 1xx interim
    assert s.is_recv_headers()
    assert not s.is_recv_streaming()
    s.recv_open(eos=False, informational=False)  # final headers
    assert s.is_recv_streaming()


def test_reserve_remote_then_push_response():
    s = H2StreamState()
    s.reserve_remote()  # Idle -> ReservedRemote (push promise)
    assert s.is_recv_headers()
    s.recv_open(eos=False, informational=False)  # -> HalfClosedLocal(Streaming)
    assert s.is_recv_streaming()
    assert s.is_send_closed()


def test_recv_reset_marks_reset_not_end_stream():
    s = H2StreamState()
    s.send_open(eos=False)
    s.recv_reset(stream_id=1, reason=Reason.CANCEL, queued=False)
    assert s.is_closed()
    assert s.is_reset()
    assert s.is_remote_reset()
    assert not s.is_recv_end_stream()


def test_invalid_transitions_raise():
    with pytest.raises(H2ProtocolError):
        H2StreamState().recv_close()  # protocol error from Idle

    s = H2StreamState()
    s.send_open(eos=False)
    with pytest.raises(H2UserError):
        s.send_open(eos=False)  # already streaming

    s2 = H2StreamState()
    s2.send_open(eos=False)
    with pytest.raises(H2UserError):
        s2.reserve_local()  # only valid from Idle


def test_ensure_recv_open_reflects_close_cause():
    s = H2StreamState()
    s.send_open(eos=True)
    s.recv_open(eos=True, informational=False)  # Closed(EndStream)
    assert s.ensure_recv_open() is False

    # A stream the peer reset is a *stream-level* error (Error::Reset), not a
    # connection error: it surfaces as H2StreamError carrying (stream_id, reason,
    # initiator), so the driver RSTs just that stream instead of GOAWAY-ing.
    s2 = H2StreamState()
    s2.send_open(eos=False)
    s2.recv_reset(stream_id=1, reason=Reason.REFUSED_STREAM, queued=False)
    with pytest.raises(H2StreamError) as exc:
        s2.ensure_recv_open()
    assert exc.value.args == (1, int(Reason.REFUSED_STREAM), "remote")


def test_set_reset_is_local_error():
    s = H2StreamState()
    s.send_open(eos=False)
    s.set_reset(stream_id=1, reason=Reason.CANCEL, initiator="user")
    assert s.is_reset()
    assert s.is_local_error()
    assert not s.is_remote_reset()


def test_scheduled_reset():
    s = H2StreamState()
    s.send_open(eos=False)
    s.set_scheduled_reset(Reason.REFUSED_STREAM)
    assert s.is_scheduled_reset()
    assert s.get_scheduled_reset() == Reason.REFUSED_STREAM
    assert s.is_local_error()
