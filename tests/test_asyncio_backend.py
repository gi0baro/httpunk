"""Minimal step-2 smoke for the asyncio backend — proves `_AsyncioStream`'s buffer
+ a real loopback `connect_tcp` round-trip work. The full contract + e2e suite
(scope teardown, select, TLS/ALPN, driver round-trips) is step 3.
"""

import asyncio

import pytest

from httpunk._backend.asyncio import AsyncioBackend, _AsyncioStream


@pytest.mark.asyncio
async def test_stream_buffer_receive_peek_and_eof():
    s = _AsyncioStream()
    s.connection_made(None)  # no transport needed while under the read high-water mark
    s.data_received(b"hello")
    assert s.read_nowait() == b"hello"  # sync peek drains the buffer
    assert s.read_nowait() == b""  # empty now
    s.data_received(b"world")
    assert await s.receive_some(3) == b"wor"  # partial read honors max_bytes
    assert await s.receive_some() == b"ld"
    s.eof_received()
    assert await s.receive_some() == b""  # EOF -> b""


@pytest.mark.asyncio
async def test_receive_some_blocks_until_data():
    s = _AsyncioStream()
    s.connection_made(None)

    async def feed_later():
        await asyncio.sleep(0.01)
        s.data_received(b"late")

    asyncio.ensure_future(feed_later())
    assert await s.receive_some() == b"late"  # parked until data arrives


@pytest.mark.asyncio
async def test_connect_tcp_roundtrip_over_loopback():
    async def echo(reader, writer):
        data = await reader.read(1024)
        writer.write(b"echo:" + data)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    async with server:
        stream = await AsyncioBackend().connect_tcp(host, port)
        await stream.send_all(b"ping")
        got = b""
        while True:
            chunk = await stream.receive_some()
            if not chunk:
                break
            got += chunk
        stream.close()
    assert got == b"echo:ping"


# ----- contract tests for the risky seam bits (in isolation) -----


class _FakeTransport:
    """Records pause/resume_reading for the backpressure contract test."""

    def __init__(self):
        self.reading_paused = False

    def pause_reading(self):
        self.reading_paused = True

    def resume_reading(self):
        self.reading_paused = False


@pytest.mark.asyncio
async def test_read_backpressure_pauses_and_resumes():
    s = _AsyncioStream()
    s.connection_made(_FakeTransport())
    s.data_received(b"x" * (2**16))  # at/over the high-water mark -> pause reading
    assert s._transport.reading_paused
    assert await s.receive_some(2**16) == b"x" * (2**16)  # drain -> resume
    assert not s._transport.reading_paused


@pytest.mark.asyncio
async def test_select_returns_winner_and_cancels_loser():
    backend = AsyncioBackend()
    loser_finished = False

    async def winner():
        return "win"

    async def loser():
        nonlocal loser_finished
        await asyncio.sleep(10)
        loser_finished = True

    assert await backend.select(winner(), loser()) == "win"
    assert not loser_finished  # the loser was cancelled, not left running


@pytest.mark.asyncio
async def test_scope_spawn_cancel_and_join():
    backend = AsyncioBackend()
    ran = []

    async def work(tag, delay):
        try:
            await asyncio.sleep(delay)
            ran.append(tag)
        except asyncio.CancelledError:
            ran.append(f"{tag}-cancelled")
            raise

    scope = backend.scope()
    await scope.__aenter__()
    scope.spawn(work("fast", 0))
    scope.spawn(work("slow", 10))
    await asyncio.sleep(0.01)  # let "fast" finish
    scope.cancel()
    await scope.__aexit__(None, None, None)  # joins; swallows "slow"'s CancelledError
    assert "fast" in ran
    assert "slow-cancelled" in ran


@pytest.mark.asyncio
async def test_queue_send_and_receive_fifo():
    sender, receiver = AsyncioBackend().queue()
    sender.send(1)
    sender.send(2)
    assert await receiver.receive() == 1
    assert await receiver.receive() == 2
