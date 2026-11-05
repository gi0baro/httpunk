"""h2 refusal counting and ordered delivery."""

import pytest
from _net import H2Peer, tcp_pair
from tonio.colored import Event, scope

from httpunk import H2Reason
from httpunk._httpunk import H2FrameGoAway as GoAway, H2FrameRstStream as RstStream, H2FrameWindowUpdate as WindowUpdate
from httpunk.exceptions import H2ProtocolError
from httpunk.h2.server import H2Server


@pytest.mark.tonio
async def test_refused_requests_count_toward_local_reset_limit():
    # h2 tests/h2-tests/tests/server.rs:1536 refused_requests_count_toward_local_reset_limit.
    done = Event()
    async with tcp_pair() as (client, transport):
        server = H2Server(transport, max_concurrent_streams=0, max_local_error_reset_streams=2)

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

        async with scope() as s:
            s.spawn(serve())
            peer = await H2Peer.client(client)
            assert (await peer.handshake()).max_concurrent_streams == 0
            for sid in (1, 3, 5):
                await peer.send_request_headers(sid, "GET", "https://example.com/", end_stream=True)
                if sid < 5:
                    frame = await peer.expect(RstStream, WindowUpdate)
                    assert (frame.stream_id, frame.error_code) == (sid, H2Reason.REFUSED_STREAM)
                else:
                    frame = await peer.expect(GoAway, WindowUpdate)
                    assert (frame.last_stream_id, frame.error_code, frame.debug_data) == (
                        0,
                        H2Reason.ENHANCE_YOUR_CALM,
                        b"too_many_internal_resets",
                    )
            await done.wait()
        assert server._conn.local_error_resets == 2


@pytest.mark.tonio
async def test_consecutive_refusals_survive_write_backpressure():
    # h2 tests/h2-tests/tests/server.rs:73 consecutive_refusals_survive_write_backpressure.
    # Upstream gates a 24-byte mock writer; loopback buffers may absorb these writes.
    done = Event()
    async with tcp_pair() as (client, transport):
        server = H2Server(transport, max_concurrent_streams=0, max_local_error_reset_streams=None)

        async def serve():
            async with server:
                async for _ in server:
                    pytest.fail("a refused request must not be accepted")
            done.set()

        async with scope() as s:
            s.spawn(serve())
            peer = await H2Peer.client(client)
            assert (await peer.handshake()).max_concurrent_streams == 0
            count = 2000
            for n in range(count):
                await peer.send_request_headers(2 * n + 1, "GET", "https://example.com/", end_stream=True)
            for n in range(count):
                frame = await peer.expect(RstStream, WindowUpdate)
                assert (frame.stream_id, frame.error_code) == (2 * n + 1, H2Reason.REFUSED_STREAM)
            await peer.ping_pong(bytes([42]) * 8)
            # Graceful peer close gives the same clean end-of-accept assertion as
            # upstream dropping its mock IO (httpunk reports a bare EOF as an error).
            await peer.send_go_away(0, H2Reason.NO_ERROR)
            frame = await peer.expect(GoAway, WindowUpdate)
            assert frame.error_code == H2Reason.NO_ERROR
            await done.wait()
