"""The HTTP/2 client keep-alive: hyper's tests (tests/client.rs, v1.13.0), ported."""

import pytest
from _net import h2_client_against, read_to_eof
from tonio.colored import Event, sleep

from httpunk import ConnectionClosedError, H2KeepAliveTimeoutError
from httpunk._httpunk import H2FrameHeaders as Headers, H2FramePing as Ping


# hyper's values, which are also the shortest httpunk keeps for both durations.
_INTERVAL = 1.0
_TIMEOUT = 1.0
_SLEEP = 4.0
_KEEP_ALIVE = {"keep_alive_interval": _INTERVAL, "keep_alive_timeout": _TIMEOUT}


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
