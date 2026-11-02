"""h2 refusal counting and ordered delivery on both runtimes."""

import pytest
from _runtime import connected_pair
from conftest import TONIO_OK

from httpunk import H2Reason
from httpunk._backend import Backend
from httpunk._httpunk import (
    H2Codec,
    H2FrameGoAway as GoAway,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as Settings,
    H2FrameWindowUpdate as WindowUpdate,
)
from httpunk.exceptions import H2ProtocolError
from httpunk.h2.connection import PREFACE
from httpunk.h2.server import H2Server


async def _frames(peer, codec):
    while chunk := await peer.receive_some(65536):
        for frame in codec.receive(chunk):
            if not isinstance(frame, WindowUpdate):
                yield frame


async def _handshake(peer):
    codec = H2Codec()
    await peer.send_all(PREFACE + codec.serialize_settings())
    frames = _frames(peer, codec)
    settings_seen = ack_seen = False
    while not (settings_seen and ack_seen):
        frame = await anext(frames)
        assert isinstance(frame, Settings)
        if frame.ack:
            ack_seen = True
        else:
            assert frame.max_concurrent_streams == 0
            settings_seen = True
            await peer.send_all(codec.serialize_settings_ack())
    return codec, frames


async def _refused_requests_count_toward_local_reset_limit(runtime):
    # h2 tests/h2-tests/tests/server.rs:1536 refused_requests_count_toward_local_reset_limit.
    backend = Backend(runtime).create()
    done = backend.event()
    async with connected_pair(runtime, backend) as (peer, transport):
        server = H2Server(transport, backend=backend, max_concurrent_streams=0, max_local_error_reset_streams=2)

        async def serve():
            try:
                async with server:
                    async for _ in server:
                        pytest.fail("a refused request must not be accepted")
                    # The accept iterator ends; the driver's saved error identifies
                    # the library GOAWAY (upstream returns it from poll_accept).
                    error = server._conn.conn_error()
                    assert type(error) is H2ProtocolError
                    assert error.args[0] == H2Reason.ENHANCE_YOUR_CALM
                    assert error._debug_data == b"too_many_internal_resets"
            finally:
                done.set()

        async with backend.scope() as scope:
            scope.spawn(serve())
            codec, frames = await _handshake(peer)
            for sid in (1, 3, 5):
                await peer.send_all(
                    codec.serialize_request_headers(sid, "GET", "https://example.com/", end_stream=True)
                )
                frame = await anext(frames)
                if sid < 5:
                    assert isinstance(frame, RstStream)
                    assert (frame.stream_id, frame.error_code) == (sid, H2Reason.REFUSED_STREAM)
                else:
                    assert isinstance(frame, GoAway)
                    assert (frame.last_stream_id, frame.error_code, frame.debug_data) == (
                        0,
                        H2Reason.ENHANCE_YOUR_CALM,
                        b"too_many_internal_resets",
                    )
            await done.wait()
        assert server._conn.local_error_resets == 2


async def _consecutive_refusals_survive_write_backpressure(runtime):
    # h2 tests/h2-tests/tests/server.rs:73 consecutive_refusals_survive_write_backpressure.
    # Upstream gates a 24-byte mock writer; loopback buffers may absorb these writes.
    backend = Backend(runtime).create()
    done = backend.event()
    async with connected_pair(runtime, backend) as (peer, transport):
        server = H2Server(transport, backend=backend, max_concurrent_streams=0, max_local_error_reset_streams=None)

        async def serve():
            async with server:
                async for _ in server:
                    pytest.fail("a refused request must not be accepted")
            done.set()

        async with backend.scope() as scope:
            scope.spawn(serve())
            codec, frames = await _handshake(peer)
            count = 2000
            for n in range(count):
                await peer.send_all(
                    codec.serialize_request_headers(2 * n + 1, "GET", "https://example.com/", end_stream=True)
                )
            for n in range(count):
                frame = await anext(frames)
                assert isinstance(frame, RstStream)
                assert (frame.stream_id, frame.error_code) == (2 * n + 1, H2Reason.REFUSED_STREAM)
            payload = bytes([42]) * 8
            await peer.send_all(codec.serialize_ping(payload))
            frame = await anext(frames)
            assert isinstance(frame, Ping) and frame.ack and frame.data == payload
            # Graceful peer close gives the same clean end-of-accept assertion as
            # upstream dropping its mock IO (httpunk reports a bare EOF as an error).
            await peer.send_all(codec.serialize_go_away(0, H2Reason.NO_ERROR))
            frame = await anext(frames)
            assert isinstance(frame, GoAway) and frame.error_code == H2Reason.NO_ERROR
            await done.wait()


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_refused_requests_count_toward_local_reset_limit_tonio():
    await _refused_requests_count_toward_local_reset_limit("tonio")


@pytest.mark.asyncio
async def test_refused_requests_count_toward_local_reset_limit_asyncio():
    await _refused_requests_count_toward_local_reset_limit("asyncio")


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_consecutive_refusals_survive_write_backpressure_tonio():
    await _consecutive_refusals_survive_write_backpressure("tonio")


@pytest.mark.asyncio
async def test_consecutive_refusals_survive_write_backpressure_asyncio():
    await _consecutive_refusals_survive_write_backpressure("asyncio")
