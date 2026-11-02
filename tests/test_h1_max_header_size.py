"""Ports of hyper 1.12.0's HTTP/1 max_header_size builder tests."""

import pytest
from _runtime import connected_pair
from conftest import TONIO_OK

from httpunk import H1Connection
from httpunk._backend import Backend
from httpunk.exceptions import H1ParseError
from httpunk.h1 import H1Server


async def _server_max_header_size(runtime, *, oversized, max_buf_size=None):
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (peer, stream):
        server = H1Server(stream, backend=backend, max_header_size=512, max_buf_size=max_buf_size)

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

        async with backend.scope() as scope:
            scope.spawn(serve())
            try:
                if oversized:
                    await peer.send_all(b"GET / HTTP/1.1\r\nHost: x\r\nX-Long: ")
                    await peer.send_all(b"a" * 1000)
                    await peer.send_all(b"\r\n\r\n")
                else:
                    await peer.send_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                response = b""
                try:
                    while chunk := await peer.receive_some(65536):
                        response += chunk
                except backend.broken_transport_errors:
                    pass  # dropping an IO with unread input may produce RST rather than FIN
                assert response.startswith(b"HTTP/1.1 431 " if oversized else b"HTTP/1.1 200 ")
            finally:
                backend.close_transport(peer)
                backend.close_transport(stream)


async def _client_max_header_size(runtime, *, oversized):
    backend = Backend(runtime).create()
    async with connected_pair(runtime, backend) as (stream, peer):

        async def respond():
            try:
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = await peer.receive_some(65536)
                    assert chunk
                    request += chunk
                if oversized:
                    await peer.send_all(b"HTTP/1.1 200 OK\r\nX-Long: ")
                    await peer.send_all(b"a" * 1000)
                    await peer.send_all(b"\r\n\r\n")
                else:
                    await peer.send_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            finally:
                backend.close_transport(peer)

        async with backend.scope() as scope:
            scope.spawn(respond())
            try:
                async with H1Connection(stream, backend=backend, max_header_size=512) as conn:
                    if oversized:
                        with pytest.raises(H1ParseError) as caught:
                            await conn.request("GET", "/a")
                        assert caught.value.args == ("too_large", "message head is too large")
                    else:
                        response = await conn.request("GET", "/a")
                        assert response.status == 200
            finally:
                backend.close_transport(stream)
                backend.close_transport(peer)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_header_size_exceeded_tonio():
    # hyper tests/server.rs: max_header_size_exceeded
    await _server_max_header_size("tonio", oversized=True)


@pytest.mark.asyncio
async def test_max_header_size_exceeded_asyncio():
    # hyper tests/server.rs: max_header_size_exceeded
    await _server_max_header_size("asyncio", oversized=True)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_header_size_accepted_tonio():
    # hyper tests/server.rs: max_header_size_accepted
    await _server_max_header_size("tonio", oversized=False)


@pytest.mark.asyncio
async def test_max_header_size_accepted_asyncio():
    # hyper tests/server.rs: max_header_size_accepted
    await _server_max_header_size("asyncio", oversized=False)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_max_header_size_with_large_max_buf_size_tonio():
    # hyper tests/server.rs: max_header_size_with_large_max_buf_size
    await _server_max_header_size("tonio", oversized=True, max_buf_size=64 * 1024)


@pytest.mark.asyncio
async def test_max_header_size_with_large_max_buf_size_asyncio():
    # hyper tests/server.rs: max_header_size_with_large_max_buf_size
    await _server_max_header_size("asyncio", oversized=True, max_buf_size=64 * 1024)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_client_max_header_size_exceeded_tonio():
    # hyper tests/client.rs: client_max_header_size_exceeded
    await _client_max_header_size("tonio", oversized=True)


@pytest.mark.asyncio
async def test_client_max_header_size_exceeded_asyncio():
    # hyper tests/client.rs: client_max_header_size_exceeded
    await _client_max_header_size("asyncio", oversized=True)


@pytest.mark.tonio
@pytest.mark.skipif(not TONIO_OK, reason="tonio needs free-threaded Python")
async def test_client_max_header_size_accepted_tonio():
    # hyper tests/client.rs: client_max_header_size_accepted
    await _client_max_header_size("tonio", oversized=False)


@pytest.mark.asyncio
async def test_client_max_header_size_accepted_asyncio():
    # hyper tests/client.rs: client_max_header_size_accepted
    await _client_max_header_size("asyncio", oversized=False)
