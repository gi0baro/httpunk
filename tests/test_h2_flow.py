"""Phase 2 end-to-end over tonio loopback: streaming a large response body
(exercises recv flow control + WINDOW_UPDATE) and multiplexing two concurrent
streams. The inline server respects the client's flow-control windows — waiting
for WINDOW_UPDATEs before sending past them — so the recv path is exercised for
real."""

import pytest
from _client import open_h2
from tonio.colored import Event, scope, sleep
from tonio.colored.net import open_tcp_listeners
from tonio.colored.sync import Lock

from httpunk._httpunk import (
    H2Codec,
    H2FrameData as Data,
    H2FrameHeaders as Headers,
    H2FrameSettings as Settings,
    H2FrameWindowUpdate as WindowUpdate,
)
from httpunk.h2.connection import PREFACE
from httpunk.http import HeaderMap


_DEFAULT_WINDOW = 65_535


class _Server:
    """A small h2c server that honours the peer's flow-control windows."""

    def __init__(self, listener, *, settings=None):
        self.listener = listener
        self.settings = settings or {}
        self.window_updates = 0
        self.headers_seen = []  # stream ids in the order HEADERS arrived
        self.req_bodies = {}  # stream_id -> received request body
        self._wlock = Lock()  # window state
        self._slock = Lock()  # serialize socket writes
        self._window_evt = Event()
        self._conn_window = _DEFAULT_WINDOW
        self._stream_windows = {}

    async def serve(self, responder, reqscope):
        self._stream = await self.listener.accept()
        self._codec = H2Codec("server")
        await self._send(self._codec.serialize_settings(**self.settings))

        raw = b""
        while len(raw) < len(PREFACE):
            chunk = await self._stream.receive_some(65536)
            if not chunk:
                return
            raw += chunk
        assert raw[: len(PREFACE)] == PREFACE

        await self._process(self._codec.receive(raw[len(PREFACE) :]), responder, reqscope)
        while True:
            chunk = await self._stream.receive_some(65536)
            if not chunk:
                break
            await self._process(self._codec.receive(chunk), responder, reqscope)

    async def _process(self, frames, responder, reqscope):
        for f in frames:
            if isinstance(f, Settings) and not f.ack:
                await self._send(self._codec.serialize_settings_ack())
            elif isinstance(f, WindowUpdate):
                self.window_updates += 1
                async with self._wlock:
                    if f.stream_id == 0:
                        self._conn_window += f.increment
                    else:
                        self._stream_windows[f.stream_id] = (
                            self._stream_windows.get(f.stream_id, _DEFAULT_WINDOW) + f.increment
                        )
                self._window_evt.set()
            elif isinstance(f, Headers):
                self.headers_seen.append(f.stream_id)
                self._stream_windows.setdefault(f.stream_id, _DEFAULT_WINDOW)
                # A bodyless request carries END_STREAM on HEADERS (no trailing
                # empty DATA frame), so the request is already complete here.
                if f.end_stream:
                    reqscope.spawn(responder(self, f.stream_id))
            elif isinstance(f, Data):
                self.req_bodies[f.stream_id] = self.req_bodies.get(f.stream_id, b"") + f.data
                if f.end_stream:
                    # Spawn so the read loop keeps running (and can observe any
                    # further HEADERS) while the responder sends.
                    reqscope.spawn(responder(self, f.stream_id))

    async def _send(self, data):
        async with self._slock:
            await self._stream.send_all(data)

    async def send_response(self, sid, body, *, status=200, chunk=16384):
        await self._send(
            self._codec.serialize_response_headers(sid, status, HeaderMap([("content-type", b"text/plain")]))
        )
        offset = 0
        while offset < len(body):
            while self._available(sid) <= 0:  # blocked on the peer's window
                self._window_evt.clear()
                if self._available(sid) > 0:
                    break
                await self._window_evt.wait()
            async with self._wlock:
                n = min(self._conn_window, self._stream_windows.get(sid, _DEFAULT_WINDOW), chunk, len(body) - offset)
                self._conn_window -= n
                self._stream_windows[sid] -= n
            await self._send(self._codec.serialize_data(sid, body[offset : offset + n], end_stream=False))
            offset += n
        await self._send(self._codec.serialize_data(sid, b"", end_stream=True))

    def _available(self, sid):
        return min(self._conn_window, self._stream_windows.get(sid, _DEFAULT_WINDOW))


@pytest.mark.tonio
async def test_streaming_large_body():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    server = _Server(listener)
    payload = bytes(200_000)  # > default 65535 window -> requires WINDOW_UPDATEs

    async def responder(srv, sid):
        await srv.send_response(sid, payload)

    chunks = []
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            resp = await conn.get("/big")
            async for chunk in resp.aiter_bytes():
                chunks.append(chunk)
            status = resp.status
        s.cancel()

    assert status == 200
    assert b"".join(chunks) == payload
    # The body exceeds the initial window, so consuming it must reclaim capacity.
    assert server.window_updates > 0


@pytest.mark.tonio
async def test_multiplexing_two_streams():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    server = _Server(listener)

    async def responder(srv, sid):
        await srv.send_response(sid, f"stream-{sid}".encode())

    results = {}
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            done = [Event(), Event()]

            async def fetch(i, path):
                resp = await conn.get(path)
                results[path] = (resp.status, await resp.read())
                done[i].set()

            async with scope() as reqs:
                reqs.spawn(fetch(0, "/a"))
                reqs.spawn(fetch(1, "/b"))
                await done[0].wait()
                await done[1].wait()
                reqs.cancel()
        s.cancel()

    # Two concurrent streams on one connection; the client assigns ids 1 and 3
    # (odd, increasing), but which path lands on which id is a scheduling race.
    assert set(results.values()) == {(200, b"stream-1"), (200, b"stream-3")}


@pytest.mark.tonio
async def test_max_concurrent_streams_gating():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    # Advertise a limit of 1: the client must not open a second stream until the
    # first has closed.
    server = _Server(listener, settings={"max_concurrent_streams": 1})
    gating_held = []

    async def responder(srv, sid):
        if sid == 1:
            # Hold the first response briefly. A client that ignored the limit
            # would open stream 3 during this window; a gated client cannot.
            await sleep(0.05)
            gating_held.append(set(srv.headers_seen) == {1})
        await srv.send_response(sid, f"stream-{sid}".encode())

    results = {}
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            done = [Event(), Event()]

            async def fetch(i, path):
                resp = await conn.get(path)
                results[path] = await resp.read()
                done[i].set()

            async with scope() as reqs:
                reqs.spawn(fetch(0, "/a"))
                reqs.spawn(fetch(1, "/b"))
                await done[0].wait()
                await done[1].wait()
                reqs.cancel()
        s.cancel()

    assert set(results.values()) == {b"stream-1", b"stream-3"}
    # While stream 1 was still open, the client never opened stream 3.
    assert gating_held == [True]


@pytest.mark.tonio
async def test_request_body_echo():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    server = _Server(listener)

    async def responder(srv, sid):
        await srv.send_response(sid, srv.req_bodies.get(sid, b""))

    async def gen_body():  # an async iterable of chunks
        yield b"hello "
        yield b"streamed "
        yield b"body"

    bytes_result = {}
    iter_result = {}
    async with scope() as s:
        s.spawn(server.serve(responder, s))
        async with open_h2(host, port) as conn:
            r1 = await conn.request("POST", "/bytes", body=b"a fixed body")
            bytes_result["body"] = await r1.read()
            r2 = await conn.request("POST", "/stream", body=gen_body())
            iter_result["body"] = await r2.read()
        s.cancel()

    assert bytes_result["body"] == b"a fixed body"
    assert iter_result["body"] == b"hello streamed body"
