"""The HTTP/2 keep-alive: hyper's tests (tests/client.rs, tests/server.rs, v1.13.0),
ported."""

import pytest
from _net import _client_against, h2_client_against, h2_peer_against, open_h2, read_to_eof
from tonio.colored import Event, sleep
from tonio.colored.time import timeout

from httpunk import ConnectionClosedError, H2KeepAliveTimeoutError, H2Reason
from httpunk._httpunk import H2FrameGoAway as GoAway, H2FrameHeaders as Headers, H2FramePing as Ping
from httpunk.h2 import H2Server


# hyper's values, which are also the shortest httpunk keeps for both durations.
_INTERVAL = 1.0
_TIMEOUT = 1.0
_SLEEP = 4.0
_KEEP_ALIVE = {"keep_alive_interval": _INTERVAL, "keep_alive_timeout": _TIMEOUT}
_PINGS = 3
_PINGS_WITHIN = 5.0
# h2 frame/ping.rs:17, the payload of the keep-alive PING.
_USER_PING = bytes([0x3B, 0x7C, 0xDB, 0x7A, 0x0B, 0x87, 0x16, 0xB4])


async def _silent(peer):
    # httpunk's `connect()` waits for the server's SETTINGS (a marked divergence): the
    # peer completes the handshake, then reads and never writes.
    await read_to_eof(peer.stream, allow_reset=True)


@pytest.mark.tonio
async def test_keep_alive_detects_unresponsive_server():
    # hyper tests/client.rs:2731 http2_keep_alive_detects_unresponsive_server (v1.13.0).
    async with h2_client_against(_silent, keep_alive_while_idle=True, **_KEEP_ALIVE) as conn:
        await sleep(_SLEEP)
        assert conn.closed


@pytest.mark.tonio
async def test_keep_alive_not_while_idle():
    # hyper tests/client.rs:2762 http2_keep_alive_not_while_idle (v1.13.0).
    async with h2_client_against(_silent, **_KEEP_ALIVE) as conn:
        await sleep(_SLEEP)
        await conn.ready()


@pytest.mark.tonio
async def test_keep_alive_closes_open_streams():
    # hyper tests/client.rs:2796 http2_keep_alive_closes_open_streams (v1.13.0).
    async with h2_client_against(_silent, **_KEEP_ALIVE) as conn:
        with pytest.raises(H2KeepAliveTimeoutError):
            await conn.request("GET", "/")
        with pytest.raises(ConnectionClosedError):
            await conn.ready()


@pytest.mark.tonio
async def test_keep_alive_with_responsive_server():
    # hyper tests/client.rs:2836 http2_keep_alive_with_responsive_server (v1.13.0).
    # A raw peer, not `H2Server`: httpunk's server resets a request body still arriving
    # once its response is complete, which would end the stream and leave the client
    # idle, with no PING to answer.
    async def server(peer):
        sid = (await peer.recv(Headers)).stream_id
        await peer.send_response_headers(sid, 200, end_stream=True)
        while (ping := await peer.recv(Ping)) is not None:
            await peer.send_ping_ack(ping.data)

    never = Event()

    async def body():  # the request stream stays open, as upstream's unsent channel
        await never.wait()
        yield b""

    async with h2_client_against(server, **_KEEP_ALIVE) as conn:
        await conn.request("GET", "/", body=body())
        await sleep(_SLEEP)
        await conn.ready()


async def _serve_no_request(transport):
    # hyper's `unreachable_service()`: the accept loop must end without a request.
    async with H2Server(transport, **_KEEP_ALIVE) as server:
        assert await server.accept() is None


@pytest.mark.tonio
async def test_keep_alive_detects_unresponsive_client():
    # hyper tests/server.rs:3166 http2_keep_alive_detects_unresponsive_client (v1.13.0).
    # hyper asserts the serve future's timeout error; `H2Server` reports none, so the
    # port asserts that the accept loop ended, and the GOAWAY the client received.
    async with h2_peer_against(_serve_no_request) as peer:
        frames = peer.codec.receive(await read_to_eof(peer.stream))
    assert [frame.error_code for frame in frames if isinstance(frame, GoAway)] == [H2Reason.NO_ERROR]


@pytest.mark.tonio
async def test_keep_alive_with_responsive_client():
    # hyper tests/server.rs:3213 http2_keep_alive_with_responsive_client (v1.13.0).
    async def serve(transport):
        async with H2Server(transport, **_KEEP_ALIVE) as server:
            async for request in server:
                await request.respond(200)

    async with _client_against(open_h2, serve, {}) as conn:
        await sleep(_SLEEP)
        assert (await conn.request("GET", "/")).status == 200


@pytest.mark.tonio
async def test_keep_alive_count_server_pings():
    # hyper tests/server.rs:3313 http2_keep_alive_count_server_pings (v1.13.0).
    async def read_pings(peer):
        for _ in range(_PINGS):
            ping = await peer.recv(Ping)
            assert ping is not None
            assert not ping.ack and ping.data == _USER_PING
            await peer.send_ping_ack(_USER_PING)

    async with h2_peer_against(_serve_no_request) as peer:
        _, completed = await timeout(read_pings(peer), _PINGS_WITHIN)
        assert completed
