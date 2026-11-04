"""Derived wire checks: hyper has no tests of these client/server builder setters.

The raw h2 builder uses in hyper tests/server.rs:2310,2444,2518,2598 exercise
CONNECT upgrades, not hyper's builder options. Check the advertised bytes and
reset retention without runtime scheduling instead.
"""

import pytest

from httpunk._backend.asyncio import AsyncioBackend
from httpunk._httpunk import H2Codec, H2FrameSettings, H2FrameWindowUpdate, H2ProtocolError, H2StreamError
from httpunk.h2.client import H2Connection
from httpunk.h2.server import H2Server
from httpunk.http import HeaderMap


_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


def _connection(role, **options):
    public = H2Connection if role == "client" else H2Server
    return public(None, backend=AsyncioBackend(), **options)._conn


def _begin(conn, role):
    conn.begin()
    wire = bytes(conn.take_pending()[0])
    if role == "client":
        assert wire.startswith(_PREFACE)
        wire = wire[len(_PREFACE) :]
    peer = H2Codec("server" if role == "client" else "client")
    return peer, peer.receive(wire)


@pytest.mark.parametrize("role", ["client", "server"])
@pytest.mark.parametrize("configured", [False, True])
def test_builder_advertises_settings(role, configured):
    # hyper proto/h2/client.rs:83-135 and proto/h2/server.rs:62-79,135-151.
    options = (
        {
            "header_table_size": 8192,
            "initial_connection_window_size": 2 * 1024 * 1024,
            "max_concurrent_streams": 7,
            "max_frame_size": 32_768,
            "max_header_list_size": 65_536,
        }
        if configured
        else {}
    )
    conn = _connection(role, **options)
    _peer, frames = _begin(conn, role)
    settings = next(frame for frame in frames if isinstance(frame, H2FrameSettings))
    update = next(frame for frame in frames if isinstance(frame, H2FrameWindowUpdate))
    assert settings.header_table_size == (8192 if configured else None)
    assert settings.max_concurrent_streams == (7 if configured else (None if role == "client" else 200))
    assert settings.max_frame_size == (32_768 if configured else 16_384)
    assert settings.max_header_list_size == (65_536 if configured else 16_384)
    assert settings.enable_push == (False if role == "client" else None)
    stream_window = (2 if role == "client" else 1) * 1024 * 1024
    assert settings.initial_window_size == stream_window
    target = 2 * 1024 * 1024 if configured else (5 if role == "client" else 1) * 1024 * 1024
    assert (update.stream_id, update.increment) == (0, target - 65_535)


def test_client_none_max_frame_size_is_not_advertised():
    # hyper client/conn/http2.rs:391-394; proto/h2/client.rs:116-118.
    conn = _connection("client", max_frame_size=None)
    _peer, frames = _begin(conn, "client")
    settings = next(frame for frame in frames if isinstance(frame, H2FrameSettings))
    assert settings.max_frame_size is None


@pytest.mark.parametrize("role", ["client", "server"])
def test_builder_header_table_limit_waits_for_ack(role):
    # h2 proto/settings.rs:66-68: the decoder accepts the old table size until ACK.
    conn = _connection(role, header_table_size=0)
    peer, _frames = _begin(conn, role)
    preface = _PREFACE if role == "server" else b""
    [settings] = conn.receive(preface + peer.serialize_settings())
    conn.recv_settings(settings)

    # HPACK table-size update to 4096 followed by static :status=200 / request fields.
    block = b"\x3f\xe1\x1f" + (b"\x88" if role == "client" else b"\x82\x86\x84")

    def headers(sid):
        return len(block).to_bytes(3, "big") + b"\x01\x04" + sid.to_bytes(4, "big") + block

    [head] = conn.receive(headers(1))
    assert head.stream_id == 1
    [ack] = conn.receive(peer.serialize_settings_ack())
    conn.recv_settings(ack)
    with pytest.raises(H2ProtocolError):
        conn.receive(headers(3))


@pytest.mark.parametrize("limit,duration,retained", [(1, 3600.0, True), (0, 3600.0, False), (1, 0.0, False)])
def test_client_builder_reset_retention_controls_late_data(limit, duration, retained):
    # h2 proto/streams/recv.rs:1030-1058: cap new entries and expire after duration.
    # Zero duration tests expiry without sleeps or a scheduling-dependent deadline.
    conn = _connection("client", max_concurrent_reset_streams=limit, reset_stream_duration=duration)
    peer, _frames = _begin(conn, "client")
    [settings] = conn.receive(peer.serialize_settings())
    conn.recv_settings(settings)
    assert conn.try_claim_slot()
    sid = conn.open_stream("GET", "http://x/", HeaderMap(), True, False, object())
    conn.reset_stream(sid, 8, "user")
    [data] = conn.receive(peer.serialize_data(sid, b"late", end_stream=False))
    if retained:
        assert conn.recv_data(data).handle is None
    else:
        with pytest.raises(H2StreamError):
            conn.recv_data(data)
