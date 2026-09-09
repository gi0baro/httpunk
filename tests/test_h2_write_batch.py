"""The h2 write batch crosses to the transport without a copy (BOUNDARY_NOTES.md
§2.4 V2): `take_pending` hands out the pending buffer itself, read-only, through a
memoryview; releasing it returns the buffer to the state. Driven at the state
level, no I/O, no backend."""

import pytest

from httpunk._httpunk import H2Codec, H2FrameSettings, H2FrameWindowUpdate, H2Streams


DEFAULT_WINDOW = 65_535
MAX_WINDOW = (1 << 31) - 1


def _client_state():
    st = H2Streams(
        "client",
        initial_window_size=DEFAULT_WINDOW,
        connection_window=DEFAULT_WINDOW,
        max_frame_size=16_384,
        max_header_list_size=16_384,
        max_send_buf_size=1 << 20,
    )
    st.begin()
    return st


def _feed(st, data):
    for frame in st.receive(data):
        if isinstance(frame, H2FrameSettings):
            st.recv_settings(frame)
        elif isinstance(frame, H2FrameWindowUpdate):
            st.recv_window_update(frame)
        else:
            raise AssertionError(frame)


def _open_stream(st):
    peer = H2Codec("server")
    _feed(st, peer.serialize_settings(initial_window_size=MAX_WINDOW))
    _feed(st, peer.serialize_window_update(0, MAX_WINDOW - DEFAULT_WINDOW))
    return st.open_stream("POST", "/", None, False, False, object(), scheme="http", authority="peer")


def test_batch_is_a_read_only_view_of_the_pending_buffer():
    st = _client_state()
    batch, stopping = st.take_pending()
    assert isinstance(batch, memoryview)
    assert batch.readonly and batch.contiguous and batch.itemsize == 1
    assert stopping is False
    # The handshake: the client preface, then our SETTINGS, then the WINDOW_UPDATE(0).
    assert bytes(batch).startswith(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    with pytest.raises(TypeError):
        batch[0] = 0
    assert not st.has_pending()
    batch.release()


def test_batches_carry_exactly_the_frames_queued_between_takes():
    st = _client_state()
    st.take_pending()[0].release()
    sid = _open_stream(st)
    st.take_pending()[0].release()  # the SETTINGS ack + HEADERS
    peer = H2Codec("server")
    expected = H2Codec("client")
    expected.receive(peer.serialize_settings())  # the decoder wants the peer's preface first
    for i in range(50):
        chunk = bytes([i]) * (1000 + i)
        v = st.send_data(sid, chunk, 0, False)
        assert v.done and v.sent == len(chunk)
        batch, _ = st.take_pending()
        assert len(batch) == 9 + len(chunk)  # one DATA frame: header + payload
        assert bytes(batch)[9:] == chunk
        batch.release()
        st.credit_written()
    assert not st.has_pending()


def test_a_view_kept_alive_keeps_its_bytes_while_later_batches_flow():
    # The holder owns the buffer: a caller that holds a view past the next take (a
    # slow transport, a test) still sees its own batch, untouched, and later batches
    # simply use other memory.
    st = _client_state()
    st.take_pending()[0].release()
    sid = _open_stream(st)
    st.take_pending()[0].release()
    st.send_data(sid, b"first" * 100, 0, False)
    kept, _ = st.take_pending()
    snapshot = bytes(kept)
    st.credit_written()
    for _ in range(20):
        st.send_data(sid, b"later" * 300, 0, False)
        batch, _ = st.take_pending()
        assert bytes(batch)[9:] == b"later" * 300
        batch.release()
        st.credit_written()
    assert bytes(kept) == snapshot
    kept.release()


def test_release_returns_the_buffer_and_an_empty_take_is_empty():
    st = _client_state()
    batch, _ = st.take_pending()
    batch.release()
    empty, stopping = st.take_pending()
    assert len(empty) == 0 and bytes(empty) == b"" and stopping is False
    empty.release()


def test_stop_flag_rides_the_take():
    st = _client_state()
    st.stop_pump()
    batch, stopping = st.take_pending()
    assert stopping is True
    batch.release()
