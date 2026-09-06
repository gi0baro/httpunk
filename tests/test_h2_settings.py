"""SETTINGS synchronization (h2 proto/settings.rs) + value application (streams.rs
`apply_remote_settings` / `apply_local_settings`), inside the Rust connection state.
Socket-free: frames are minted through a codec round trip and fed to
`recv_settings` exactly as the read pump does."""

import pytest

from httpunk._backend.asyncio import AsyncioBackend
from httpunk._httpunk import H2Codec, H2ProtocolError, H2Streams
from httpunk.h2.client import Connection
from httpunk.h2.stream import Stream
from httpunk.http import HeaderMap


def _settings_frame(ack=False, **values):
    server = H2Codec("server")
    raw = server.serialize_settings_ack() if ack else server.serialize_settings(**values)
    [frame] = H2Codec("client").receive(raw)
    return frame


def _client():
    # Constructed only; never connected. The backend only builds the events, and the
    # asyncio one exists on every interpreter (these tests run on the GIL builds too).
    conn = Connection(None, backend=AsyncioBackend())
    conn.begin()  # queues the preface + our SETTINGS: the state is now WaitingAck
    return conn


def test_ack_applies_local_then_synced():
    conn = _client()
    initial, wake, _flags = conn.recv_settings(_settings_frame(ack=True))
    assert (initial, wake) == (False, [])
    # A second ACK is unexpected (nothing outstanding) -> protocol error.
    with pytest.raises(H2ProtocolError):
        conn.recv_settings(_settings_frame(ack=True))


def test_ack_before_our_settings_is_a_protocol_error():
    conn = Connection(None, backend=AsyncioBackend())  # `begin()` not called: nothing sent, nothing to ACK
    with pytest.raises(H2ProtocolError):
        conn.recv_settings(_settings_frame(ack=True))


def test_remote_settings_acked_applied_and_initial_once():
    conn = _client()
    conn.take_pending()  # drop the handshake bytes
    initial, _wake, _flags = conn.recv_settings(
        _settings_frame(initial_window_size=100_000, max_concurrent_streams=128)
    )
    assert initial is True  # the peer's first SETTINGS: the client is now ready
    assert conn.peer_initial_window_size == 100_000
    assert conn.peer_max_concurrent_streams == 128
    assert conn.stream_limit == 128  # the client gates on it
    # The ACK was queued BEFORE the values were applied (h2 `poll_send` order).
    [ack] = H2Codec("server").receive(conn.take_pending()[0])
    assert ack.ack
    # A second remote SETTINGS is no longer the initial one.
    initial2, _wake, _flags = conn.recv_settings(_settings_frame(max_frame_size=32_768))
    assert initial2 is False
    assert conn.peer_max_frame_size == 32_768


def test_peer_settings_defaults():
    conn = Connection(None, backend=AsyncioBackend())
    assert conn.peer_initial_window_size == 65_535
    assert conn.peer_max_frame_size == 16_384
    assert conn.peer_max_concurrent_streams is None


def test_adjust_send_windows_skips_send_closed_streams():
    """A SETTINGS_INITIAL_WINDOW_SIZE change adjusts open streams' send windows but
    SKIPS send-closed ones — matching h2 (its decrease branch guards
    is_send_closed()), which avoids pointlessly adjusting a window we'll never use and a
    needless inc_window overflow teardown on the increase side (F41)."""
    conn = _client()
    closed, streaming = Stream(conn.backend), Stream(conn.backend)
    assert conn.try_claim_slot() and conn.try_claim_slot()
    # Stream 1: END_STREAM on HEADERS -> send-closed; stream 3: a body follows -> streaming.
    assert conn.open_stream("GET", "http://x/", HeaderMap(), True, False, closed) == 1
    assert conn.open_stream("POST", "http://x/", HeaderMap(), False, False, streaming) == 3
    assert conn.stream_send_window(1) == conn.stream_send_window(3) == 65_535

    _initial, wake, _flags = conn.recv_settings(_settings_frame(initial_window_size=100_000))  # +34_465
    assert conn.stream_send_window(1) == 65_535  # send-closed stream skipped
    assert conn.stream_send_window(3) == 100_000  # applied to the open stream
    assert wake == [streaming]  # only its sender is woken

    conn.recv_settings(_settings_frame(initial_window_size=50_000))  # -50_000
    assert conn.stream_send_window(1) == 65_535  # skipped here too
    assert conn.stream_send_window(3) == 50_000


def test_constructor_validates_ranges():
    with pytest.raises(ValueError):
        H2Streams(
            "server",
            initial_window_size=1,
            connection_window=1,
            max_frame_size=100,  # below the RFC minimum (16384)
            max_header_list_size=1,
            max_send_buf_size=1,
        )
