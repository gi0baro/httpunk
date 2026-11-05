"""Ports of hyper 1.12.0's HTTP/1 max_header_size builder tests."""

import pytest
from _net import h1_client_against, read_head, read_to_eof, tcp_pair
from tonio.colored import scope

from httpunk.exceptions import H1ParseError
from httpunk.h1 import H1Server


@pytest.mark.tonio
@pytest.mark.parametrize(
    ("oversized", "max_buf_size"),
    [(True, None), (False, None), (True, 64 * 1024)],
    ids=["max_header_size_exceeded", "max_header_size_accepted", "max_header_size_with_large_max_buf_size"],
)
async def test_server_max_header_size(oversized, max_buf_size):
    # hyper tests/server.rs: max_header_size_exceeded / max_header_size_accepted /
    # max_header_size_with_large_max_buf_size.
    async with tcp_pair() as (peer, stream):
        server = H1Server(stream, max_header_size=512, max_buf_size=max_buf_size)

        async def serve():
            async with server:
                request = await server.accept()
                if oversized:
                    assert request is None
                    assert server._conn.codec.parse_error_status == 431
                else:
                    assert request is not None
                    await request.respond(200, body=b"Hello, World!")
                    assert await server.accept() is None

        async with scope() as s:
            s.spawn(serve())
            try:
                if oversized:
                    await peer.send_all(b"GET / HTTP/1.1\r\nHost: x\r\nX-Long: ")
                    await peer.send_all(b"a" * 1000)
                    await peer.send_all(b"\r\n\r\n")
                else:
                    await peer.send_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                response = await read_to_eof(peer, allow_reset=True)
                assert response.startswith(b"HTTP/1.1 431 " if oversized else b"HTTP/1.1 200 ")
            finally:
                peer.close()
                stream.close()


@pytest.mark.tonio
@pytest.mark.parametrize(
    "oversized", [True, False], ids=["client_max_header_size_exceeded", "client_max_header_size_accepted"]
)
async def test_client_max_header_size(oversized):
    # hyper tests/client.rs: client_max_header_size_exceeded / client_max_header_size_accepted.
    async def respond(peer):
        try:
            assert await read_head(peer) is not None
            if oversized:
                await peer.send_all(b"HTTP/1.1 200 OK\r\nX-Long: ")
                await peer.send_all(b"a" * 1000)
                await peer.send_all(b"\r\n\r\n")
            else:
                await peer.send_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        finally:
            peer.close()

    async with h1_client_against(respond, max_header_size=512) as conn:
        if oversized:
            with pytest.raises(H1ParseError) as caught:
                await conn.request("GET", "/a")
            assert caught.value.args == ("too_large", "message head is too large")
        else:
            response = await conn.request("GET", "/a")
            assert response.status == 200
