"""h2 builder options: ports of h2 0.4.20's builder tests, plus a derived check of the
header table limit timing (no upstream test)."""

import pytest
from _net import h2_client_against, h2_peer_against
from tonio.colored import Event, scope

from httpunk import H2Reason
from httpunk._backend.asyncio import AsyncioBackend
from httpunk._httpunk import (
    H2Codec,
    H2FrameHeaders as Headers,
    H2FrameRstStream as RstStream,
    H2FrameWindowUpdate as WindowUpdate,
    H2ProtocolError,
)
from httpunk.h2.client import H2Connection
from httpunk.h2.server import H2Server


_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
# h2's default windows, which upstream's frame sequences assume; hyper's profile is larger.
_H2_DEFAULT_WINDOWS = {"initial_window_size": 65_535, "initial_connection_window_size": 65_535}


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


async def _expect_reset(peer, stream_id, reason):
    frame = await peer.expect(RstStream)
    assert (frame.stream_id, frame.error_code) == (stream_id, reason)


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


@pytest.mark.tonio
async def test_client_builder_header_table_size():
    # h2 tests/h2-tests/tests/client_request.rs:1852 client_builder_header_table_size.
    async def server(peer):
        # Upstream compares the whole frame to h2's defaults; httpunk sends hyper's
        # profile, so only the configured setting is asserted.
        assert (await peer.handshake()).header_table_size == 10000
        request = await peer.expect(Headers, WindowUpdate)
        assert request.end_stream
        assert (request.stream_id, request.method, request.scheme, request.authority, request.path) == (
            1,
            "GET",
            "https",
            "example.com",
            "/",
        )
        await peer.send_response_headers(1, 200, end_stream=True)

    async with h2_client_against(server, scheme="https", authority="example.com", header_table_size=10000) as conn:
        assert (await conn.request("GET", "/")).status == 200


@pytest.mark.tonio
@pytest.mark.parametrize("size", [0, 10000])
async def test_server_builder_header_table_size(size):
    # h2 tests/h2-tests/tests/server.rs:131 server_builder_header_table_size.
    methods = []
    ended = Event()

    async def serve(transport):
        async with H2Server(transport, header_table_size=size) as server:
            async for request in server:
                methods.append(request.method)
                await request.respond(200)
        ended.set()

    # Upstream's client sends h2's default SETTINGS; the extra ENABLE_PUSH=0 is inert here.
    async with h2_peer_against(serve) as peer:
        # Upstream compares the whole frame to h2's defaults; httpunk sends hyper's
        # profile, so only the configured setting is asserted.
        assert (await peer.handshake()).header_table_size == size
        peer.codec.set_send_header_table_size(size)
        await peer.send_request_headers(1, "GET", "https://example.com/", end_stream=True)
        response = await peer.expect(Headers, WindowUpdate)
        assert (response.stream_id, response.status, response.end_stream) == (1, 200, True)
        # Upstream ends the accept loop by dropping its mock IO; httpunk reports a
        # bare EOF as an error, so a graceful GOAWAY ends it cleanly instead.
        await peer.send_go_away(1, H2Reason.NO_ERROR)
        await ended.wait()
    assert methods == ["GET"]


@pytest.mark.tonio
async def test_rst_stream_expires():
    # h2 tests/h2-tests/tests/stream_states.rs:986 rst_stream_expires.
    # Upstream resets with a 10 ms duration and sleeps 15 ms; a zero duration expires
    # the entry once the clock moves, which the PING round trip guarantees: no sleep.
    finished = Event()

    async def server(peer):
        await peer.handshake()
        request = await peer.expect(Headers)
        assert (request.stream_id, request.end_stream) == (1, True)
        c = peer.codec
        await peer.send(c.serialize_response_headers(1, 200), c.serialize_data(1, bytes(16_384)))
        await _expect_reset(peer, 1, H2Reason.CANCEL)
        await peer.ping_pong()
        await peer.send_data(1, bytes(16_384), end_stream=True)
        update = await peer.expect(WindowUpdate)
        assert (update.stream_id, update.increment) == (0, 16_384 * 2)
        await _expect_reset(peer, 1, H2Reason.STREAM_CLOSED)
        finished.set()

    options = {"authority": "example.com", "reset_stream_duration": 0.0, **_H2_DEFAULT_WINDOWS}
    async with h2_client_against(server, **options) as conn:
        response = await conn.request("GET", "/")
        assert response.status == 200
        # Reading a chunk before the drop orders the DATA before the RST: with a
        # zero duration, DATA landing after it would draw STREAM_CLOSED early.
        async for _chunk in response.aiter_bytes():
            break
        await response.aclose()
        await finished.wait()


@pytest.mark.tonio
async def test_rst_stream_max():
    # h2 tests/h2-tests/tests/stream_states.rs:1037 rst_stream_max.
    first_sent = Event()
    finished = Event()

    async def server(peer):
        await peer.handshake()
        for sid in (1, 3):
            request = await peer.expect(Headers)
            assert (request.stream_id, request.end_stream) == (sid, True)
            first_sent.set()
        c = peer.codec
        await peer.send(
            c.serialize_response_headers(1, 200),
            c.serialize_data(1, bytes(16)),
            c.serialize_response_headers(3, 200),
            c.serialize_data(3, bytes(16)),
        )
        await _expect_reset(peer, 1, H2Reason.CANCEL)
        await _expect_reset(peer, 3, H2Reason.CANCEL)
        # Older streams trump newer streams: 1 is still being ignored.
        await peer.send_data(1, bytes(16), end_stream=True)
        await peer.ping_pong()
        # 3 has been evicted, so it gets a reset.
        await peer.send_data(3, bytes(16), end_stream=True)
        await _expect_reset(peer, 3, H2Reason.STREAM_CLOSED)
        finished.set()

    options = {"authority": "example.com", "max_concurrent_reset_streams": 1, **_H2_DEFAULT_WINDOWS}
    async with h2_client_against(server, **options) as conn:
        responses = [None, None]

        async def get(index):
            responses[index] = await conn.request("GET", "/")

        async with scope() as gets:
            gets.spawn(get(0))
            await first_sent.wait()
            gets.spawn(get(1))
        # Upstream drops the responses in stream order; reading a chunk first
        # orders each DATA before its RST, as in test_rst_stream_expires.
        for response in responses:
            async for _chunk in response.aiter_bytes():
                break
            await response.aclose()
        await finished.wait()
