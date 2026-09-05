"""`httpunk.util.auto` (`Builder` + the `serve` shortcut) — sniff an accepted transport
and serve it as h1 or h2. Unit tests drive a scripted transport (protocol detection + lossless prewind);
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


# ----- unit: Builder (hyper-util `auto::Builder`) -----


@pytest.mark.tonio
async def test_builder_forwards_h1_and_h2_options_to_whichever_protocol_is_picked():
    builder = auto.Builder()
    # Chain across both sub-builders like hyper-util's http1()/http2() crossover.
    builder.http1().header_read_timeout(5.0).keep_alive(False).max_headers(50).max_buf_size(16384).auto_date_header(
        False
    ).title_case_headers(True).ignore_invalid_headers(True).http2().max_concurrent_streams(
        7
    ).initial_stream_window_size(123456).data_frame_budget(4096).initial_connection_window_size(
        2_000_000
    ).max_frame_size(32768).max_header_list_size(65536).max_pending_accept_reset_streams(
        5
    ).max_local_error_reset_streams(None).auto_date_header(False)

    h1 = await builder.serve_connection(_ScriptedTransport(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n"))
    assert isinstance(h1, H1Server)
    assert h1._conn._header_read_timeout == 5.0
    assert h1._conn._keep_alive_enabled is False
    assert h1._conn._max_buf_size == 16384
    assert h1._conn._codec_options == {
        "max_headers": 50,
        "ignore_invalid_headers": True,
        "title_case_headers": True,
        "date_header": False,
    }

    h2 = await builder.serve_connection(_ScriptedTransport(PREFACE))
    assert isinstance(h2, H2Server)
    assert h2._conn._max_concurrent_streams == 7
    assert h2._conn._initial_window_size == 123456
    assert h2._conn._initial_connection_window_size == 2_000_000
    assert h2._conn._max_frame_size == 32768
    assert h2._conn._max_header_list_size == 65536
    assert h2._conn.streams._max_pending_accept_reset_streams == 5
    assert h2._conn.streams._max_local_error_resets is None  # None = no limit (hyper semantics)
    assert h2._conn._auto_date_header is False


@pytest.mark.tonio
async def test_builder_none_restores_defaults_and_header_read_timeout_none_disables():
    builder = auto.Builder()
    builder.http2().max_concurrent_streams(7).max_concurrent_streams(None)  # None -> server default
    builder.http1().header_read_timeout(None)  # None -> disabled (hyper Into<Option<Duration>>)
    h2 = await builder.serve_connection(_ScriptedTransport(PREFACE))
    assert h2._conn._max_concurrent_streams == H2Server(_ScriptedTransport(b""))._conn._max_concurrent_streams
    h1 = await builder.serve_connection(_ScriptedTransport(b"GET / HTTP/1.1\r\n\r\n"))
    assert h1._conn._header_read_timeout is None


@pytest.mark.tonio
async def test_builder_only_forces_without_sniffing_and_refuses_double_force():
    t = _ScriptedTransport(PREFACE)
    assert isinstance(await auto.Builder().http1_only().serve_connection(t), H1Server)
    assert t._data == PREFACE  # nothing consumed for detection
    assert isinstance(await auto.Builder().http2_only().serve_connection(_ScriptedTransport(b"")), H2Server)
    with pytest.raises(RuntimeError, match="already forced"):
        auto.Builder().http1_only().http2_only()


@pytest.mark.tonio
async def test_builder_serve_connection_from_sub_builders():
    # hyper-util: both Http1Builder and Http2Builder expose serve_connection.
    assert isinstance(await auto.Builder().http1().serve_connection(_ScriptedTransport(PREFACE)), H2Server)
    assert isinstance(await auto.Builder().http2().serve_connection(_ScriptedTransport(b"GET /")), H1Server)


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
