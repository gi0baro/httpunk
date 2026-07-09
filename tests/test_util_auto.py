"""`httpunk.util.auto.serve` — sniff an accepted transport and serve it as h1 or
h2. Unit tests drive a scripted transport (protocol detection + lossless prewind);
end-to-end loopback tests prove httpunk's own clients round-trip through the picked
server (the replayed preface / request line parses correctly).
"""

import pytest
from _client import open_h1, open_h2
from tonio.colored import scope
from tonio.colored.net import open_tcp_listeners

from httpunk.h1.server import H1Server
from httpunk.h2.connection import PREFACE
from httpunk.h2.server import H2Server
from httpunk.util import auto
from httpunk.util.auto import _PrewoundTransport


class _ScriptedTransport:
    """Feeds a fixed byte script through `receive_some` (optionally one small slice
    at a time, to exercise partial-preface reads); records sends; tracks close."""

    def __init__(self, data, *, chunk_size=None):
        self._data = bytes(data)
        self._chunk = chunk_size
        self.sent = bytearray()
        self.closed = False

    async def receive_some(self, max_bytes=65536):
        n = min(max_bytes, len(self._data))
        if self._chunk is not None:
            n = min(n, self._chunk)
        chunk, self._data = self._data[:n], self._data[n:]
        return chunk

    async def send_all(self, data):
        self.sent += data

    def close(self):
        self.closed = True


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


# ----- unit: protocol detection -----


@pytest.mark.tonio
async def test_detects_h2_from_preface():
    server = await auto.serve(_ScriptedTransport(PREFACE + b"\x00\x00\x00\x04\x00\x00\x00\x00\x00"))
    assert isinstance(server, H2Server)


@pytest.mark.tonio
async def test_detects_h1_from_request_line():
    server = await auto.serve(_ScriptedTransport(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n"))
    assert isinstance(server, H1Server)


@pytest.mark.tonio
async def test_detects_h2_across_single_byte_reads():
    # The preface arriving one byte per read must still be accumulated + matched.
    server = await auto.serve(_ScriptedTransport(PREFACE, chunk_size=1))
    assert isinstance(server, H2Server)


@pytest.mark.tonio
async def test_short_prefix_that_diverges_early_is_h1():
    # "PRX" diverges from the preface at byte 3 -> h1, without reading 24 bytes.
    t = _ScriptedTransport(b"PRX etc")
    server = await auto.serve(t)
    assert isinstance(server, H1Server)


@pytest.mark.tonio
async def test_only_forces_protocol_without_sniffing():
    # A forced server must not consume any bytes for detection.
    assert isinstance(await auto.serve(_ScriptedTransport(b""), only="h2"), H2Server)
    assert isinstance(await auto.serve(_ScriptedTransport(b""), only="h1"), H1Server)


@pytest.mark.tonio
async def test_rejects_bad_only():
    with pytest.raises(ValueError, match="only must be"):
        await auto.serve(_ScriptedTransport(b""), only="h3")


# ----- unit: prewound transport replay -----


@pytest.mark.tonio
async def test_prewound_replays_then_delegates_losslessly():
    pw = _PrewoundTransport(_ScriptedTransport(b"LIVE"), b"PEEK")
    got = b""
    for _ in range(5):
        chunk = await pw.receive_some(2)
        if not chunk:
            break
        got += chunk
    assert got == b"PEEKLIVE"


@pytest.mark.tonio
async def test_prewound_forwards_send_and_close():
    inner = _ScriptedTransport(b"")
    pw = _PrewoundTransport(inner, b"")
    await pw.send_all(b"hi")
    pw.close()
    assert inner.sent == b"hi"
    assert inner.closed


# ----- end-to-end: the picked server round-trips a real httpunk client -----


@pytest.mark.tonio
async def test_auto_serves_h2_client_end_to_end():
    listener, host, port = await _listener()
    picked = {}

    async def server_side():
        transport = await listener.accept()
        server = await auto.serve(transport)
        picked["cls"] = type(server).__name__
        async with server:
            async for req in server:
                body = await req.read()
                await req.respond(200, body=b"h2:" + req.path.encode() + b":" + body)

    async with scope() as s:
        s.spawn(server_side())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/x", body=b"hi")
            assert await resp.read() == b"h2:/x:hi"
    assert picked["cls"] == "H2Server"


@pytest.mark.tonio
async def test_auto_serves_h1_client_end_to_end():
    listener, host, port = await _listener()
    picked = {}

    async def server_side():
        transport = await listener.accept()
        server = await auto.serve(transport)
        picked["cls"] = type(server).__name__
        async with server:
            async for req in server:
                body = await req.read()
                await req.respond(200, body=b"h1:" + body)

    async with scope() as s:
        s.spawn(server_side())
        async with open_h1(host, port) as conn:
            resp = await conn.request("POST", "/y", headers={"host": host}, body=b"hey")
            assert await resp.read() == b"h1:hey"
    assert picked["cls"] == "H1Server"
