"""HTTP/2 server (`H2Server`) over a tonio loopback, driven by httpunk's own
`H2Connection` client — true end-to-end coverage of both sides of the stack:
request/response, request + response bodies, headers/pseudo-headers, streaming,
and multiplexing.
"""

import contextlib

import pytest
from _net import h2_peer_against, listen, open_h2
from tonio.colored import Event, scope, sleep

from httpunk import H2Reason, Version
from httpunk._httpunk import (
    H2FrameData as Data,
    H2FrameGoAway as GoAway,
    H2FrameHeaders as Headers,
    H2FramePing as Ping,
    H2FrameRstStream as RstStream,
    H2FrameSettings as Settings,
    H2FrameWindowUpdate as WindowUpdate,
)
from httpunk.exceptions import ConnectionClosedError, H2UserError, StreamResetError
from httpunk.h2 import H2Server
from httpunk.http import HeaderMap


async def _echo_server(listener, ready=None):
    """Accept one connection and echo each request: reply 200 with body
    `b"<METHOD> <path> -> " + request_body`."""
    transport = await listener.accept()
    async with H2Server(transport) as server:
        if ready is not None:
            ready.set()
        async for req in server:
            body = await req.read()
            reply = f"{req.method} {req.path} -> ".encode() + body
            await req.respond(200, headers={"content-type": "text/plain"}, body=reply)


@pytest.mark.tonio
async def test_server_get():
    listener, host, port = await listen()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/hello")
            assert resp.status == 200
            assert resp.headers["content-type"] == b"text/plain"
            assert await resp.read() == b"GET /hello -> "
        s.cancel()


@pytest.mark.tonio
async def test_server_post_echo_body():
    listener, host, port = await listen()
    async with scope() as s:
        s.spawn(_echo_server(listener))
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/submit", body=b"payload!")
            assert resp.status == 200
            assert await resp.read() == b"POST /submit -> payload!"
        s.cancel()


@pytest.mark.tonio
async def test_client_gets_early_response_despite_body_reset():
    """A server that responds before reading the request body resets the stream while
    the client is still uploading (RST_STREAM(NO_ERROR)). The client must still
    return the received response — the body-send error must not mask it. The body
    exceeds the flow-control window, so the send is guaranteed in-flight (blocked on a
    WINDOW_UPDATE) when the RST lands."""
    listener, host, port = await listen()

    async def serve():
        transport = await listener.accept()
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    await req.respond(200, body=b"early")  # respond WITHOUT reading the body

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/x", body=b"x" * (2 * 1024 * 1024))  # > the server's 1 MB window
            assert resp.status == 200
            assert await resp.read() == b"early"
        s.cancel()


@pytest.mark.tonio
async def test_server_streaming_response_body():
    listener, host, port = await listen()

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                await req.read()

                async def chunks():
                    yield b"a" * 50_000
                    yield b"b" * 50_000  # total 100KB > one window -> needs WINDOW_UPDATEs

                await req.respond(200, body=chunks())

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/big")
            body = await resp.read()
        s.cancel()
    assert body == b"a" * 50_000 + b"b" * 50_000


@pytest.mark.tonio
async def test_server_multiplexed_requests():
    listener, host, port = await listen()

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server, scope() as handlers:

            async def handle(req):
                await req.respond(200, body=f"{req.path}".encode())

            async for req in server:
                handlers.spawn(handle(req))  # serve concurrently (h2 multiplexing)

    results, errors = {}, []
    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            done = [Event(), Event()]

            async def fetch(i, path):
                try:
                    resp = await conn.request("GET", path)
                    results[path] = (resp.status, await resp.read())
                except BaseException as exc:
                    errors.append(exc)
                    raise
                finally:
                    done[i].set()  # set on failure too: the waits below must not hang

            async with scope() as reqs:
                reqs.spawn(fetch(0, "/a"))
                reqs.spawn(fetch(1, "/b"))
                await done[0].wait()
                await done[1].wait()
                reqs.cancel()
        s.cancel()

    assert not errors, errors
    assert results == {"/a": (200, b"/a"), "/b": (200, b"/b")}


@pytest.mark.tonio
async def test_server_request_headers():
    listener, host, port = await listen()
    seen = {}

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                seen["authority"] = req.authority
                seen["scheme"] = req.scheme
                seen["version"] = req.version
                seen["x-custom"] = req.headers.get("x-custom")
                await req.respond(204)

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/", headers={"x-custom": "abc"})
            assert resp.status == 204
            assert await resp.read() == b""
        s.cancel()
    assert seen["version"] is Version.HTTP_2  # h2 stamps HTTP_2 on every request (h2 server.rs L1674)

    assert seen["scheme"] == "http"
    assert seen["authority"] == f"{host}:{port}"
    assert seen["x-custom"] == b"abc"


# ----- adversarial: a raw client (arbitrary frames) must provoke the right
#       connection-level errors from the server -----


async def _too_big_headers(end_stream):
    done = Event()
    seen = []

    async def serve(transport):
        try:
            async with H2Server(transport, max_header_list_size=64, max_local_error_reset_streams=0) as server:
                async for req in server:
                    seen.append(req)
        finally:
            done.set()

    async with h2_peer_against(serve) as peer:
        assert (await peer.handshake()).max_header_list_size == 64
        await peer.send(
            peer.codec.serialize_request_headers(
                1, "GET", "https://example.com/", HeaderMap({"some-header": "some-value"}), end_stream=end_stream
            ),
            peer.codec.serialize_ping(b"barrier!"),
        )
        wire = []
        while not isinstance(f := await peer.recv(), Ping):
            assert f is not None
            if not isinstance(f, (Settings, WindowUpdate)):
                wire.append(f)
        assert f.ack and f.data == b"barrier!"
        assert len(wire) == (1 if end_stream else 2)
        head = wire[0]
        assert isinstance(head, Headers)
        assert head.stream_id == 1 and head.status == 431
        assert head.end_stream and head.end_headers
        assert list(head.headers.items()) == []  # No date or other regular fields.
        assert (head.method, head.scheme, head.authority, head.path) == (None, None, None, None)
        if not end_stream:
            assert isinstance(wire[1], RstStream)
            assert wire[1].stream_id == 1 and wire[1].error_code == H2Reason.PROTOCOL_ERROR
        peer.close()
        await done.wait()
        assert seen == []


@pytest.mark.tonio
async def test_too_big_headers_sends_431():
    # h2 tests/h2-tests/tests/server.rs:1031 too_big_headers_sends_431.
    await _too_big_headers(end_stream=True)


@pytest.mark.tonio
async def test_too_big_headers_sends_reset_after_431_if_not_eos():
    # h2 tests/h2-tests/tests/server.rs:1067 too_big_headers_sends_reset_after_431_if_not_eos.
    await _too_big_headers(end_stream=False)


async def _serve_forever(transport):
    with contextlib.suppress(Exception):
        async with H2Server(transport) as server:
            async for req in server:
                await req.respond(200)


@pytest.mark.tonio
async def test_server_rsts_unread_request_body_with_no_error():
    """A server that responds without consuming the request body drops the request:
    while the client is still sending (recv half open), h2 sends RST_STREAM(NO_ERROR)
    so it stops (the nginx-compat `maybe_cancel` rule). The
    unread upload used to pin the connection window with no RST ever sent."""

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    await req.respond(200)  # respond WITHOUT reading the body

    async with h2_peer_against(serve) as peer:
        # Open stream 1 with a body but never send END_STREAM (client still uploading).
        await peer.send_request_headers(1, "POST", "http://x/a", HeaderMap())
        await peer.send_data(1, b"partial upload", end_stream=False)
        rst = await peer.recv(RstStream)
        assert rst is not None and rst.stream_id == 1
        assert rst.error_code == H2Reason.NO_ERROR  # NO_ERROR (nginx-compat), not CANCEL


@pytest.mark.tonio
async def test_server_advertises_hyper_settings_profile():
    """The server ships hyper's tuned profile, not bare-h2 defaults: its initial
    SETTINGS carry MAX_CONCURRENT_STREAMS=200, a 1 MB stream window, 16 KB max frame,
    16 KB max header list, and NO ENABLE_PUSH; plus an initial WINDOW_UPDATE(0) that
    raises the connection recv window from 65535 to 1 MB."""
    async with h2_peer_against(_serve_forever) as peer:
        wu = await peer.recv(WindowUpdate)  # SETTINGS then WINDOW_UPDATE(0)
        assert wu is not None, "connection closed before the server's preface completed"
        settings = peer.settings
        assert settings.max_concurrent_streams == 200
        assert settings.initial_window_size == 1024 * 1024
        assert settings.max_frame_size == 16 * 1024
        assert settings.enable_push is None  # the server does not advertise ENABLE_PUSH
        assert wu.stream_id == 0
        assert wu.increment == 1024 * 1024 - 65535  # raise the 65535 default conn window to 1 MB


@pytest.mark.tonio
async def test_forgotten_stream_frames_swallowed_after_first_rst():
    """A frame on a forgotten (closed) stream draws ONE RST_STREAM(STREAM_CLOSED); the
    id then enters the reset store, so further frames on it are silently swallowed
    rather than each drawing another RST (the reset-amplification defence)."""
    async with h2_peer_against(_serve_forever) as peer:  # 200 (bodyless) per request
        # Complete stream 1 so the server closes + forgets it.
        await peer.send_request_headers(1, "GET", "http://x/a", HeaderMap(), end_stream=True)
        assert (await peer.recv(Headers)).stream_id == 1  # the 200 response
        # Two late DATA frames on the now-forgotten stream 1, then a fresh request (stream
        # 3) as a read barrier so we know both late frames were processed.
        await peer.send_data(1, b"late1", end_stream=False)
        await peer.send_data(1, b"late2", end_stream=False)
        await peer.send_request_headers(3, "GET", "http://x/b", HeaderMap(), end_stream=True)
        rsts = 0
        while True:
            f = await peer.recv(RstStream, Headers)
            assert f is not None, "connection closed unexpectedly"
            if isinstance(f, Headers) and f.stream_id == 3:  # barrier: stream 3 served
                break
            rsts += isinstance(f, RstStream) and f.stream_id == 1
        assert rsts == 1  # the 2nd late frame was swallowed, not answered with another RST


@pytest.mark.tonio
async def test_server_settings_options_override_hyper_profile():
    """hyper `http2::Builder` options reach the wire: `max_frame_size`,
    `max_header_list_size`, `initial_window_size` in our SETTINGS, and
    `initial_connection_window_size` as the initial WINDOW_UPDATE(0) increment."""

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(
                transport,
                max_frame_size=32_768,
                max_header_list_size=65_536,
                initial_window_size=512 * 1024,
                initial_connection_window_size=2 * 1024 * 1024,
            ) as server:
                async for req in server:
                    await req.respond(200)

    async with h2_peer_against(serve) as peer:
        wu = await peer.recv(WindowUpdate)
        assert wu is not None, "connection closed before the server's preface completed"
        settings = peer.settings
        assert settings.max_frame_size == 32_768
        assert settings.max_header_list_size == 65_536
        assert settings.initial_window_size == 512 * 1024
        assert (wu.stream_id, wu.increment) == (0, 2 * 1024 * 1024 - 65535)


def test_server_max_frame_size_out_of_range_rejected_at_construction():
    """The RFC 9113 §6.5.2 range h2's `set_max_frame_size` asserts is checked up front."""
    with pytest.raises(ValueError, match="max_frame_size"):
        H2Server(object(), max_frame_size=100)
    with pytest.raises(ValueError, match="max_frame_size"):
        H2Server(object(), max_frame_size=1 << 24)


@pytest.mark.tonio
async def test_server_max_pending_accept_reset_streams_option():
    """`max_pending_accept_reset_streams` (hyper; h2 default 20) sets the Rapid-Reset cap."""

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport, max_pending_accept_reset_streams=3):
                await Event().wait()  # never accept a request

    async with h2_peer_against(serve) as peer:
        sid = 1
        with contextlib.suppress(Exception):
            for _ in range(6):  # past the cap of 3, well under h2's default 20
                await peer.send_request_headers(sid, "GET", "http://x/p", HeaderMap())
                await peer.send_rst_stream(sid, int(H2Reason.CANCEL))
                sid += 2
        ga = await peer.recv(GoAway)
        assert ga is not None and ga.error_code == H2Reason.ENHANCE_YOUR_CALM


@pytest.mark.tonio
async def test_server_max_local_error_reset_streams_option():
    """`max_local_error_reset_streams` (hyper default 1024) caps library-initiated error
    resets before GOAWAY(ENHANCE_YOUR_CALM). Each request HEADERS carrying a
    connection-specific field is malformed -> RST_STREAM(PROTOCOL_ERROR); the cap of 1
    lets one through and GOAWAYs on the second."""

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport, max_local_error_reset_streams=1) as server:
                async for req in server:
                    await req.respond(200)

    async with h2_peer_against(serve) as peer:
        bad = HeaderMap({"connection": "keep-alive"})
        await peer.send_request_headers(1, "GET", "http://x/p", bad, end_stream=True)
        rst = await peer.recv(RstStream)
        assert rst is not None and (rst.stream_id, rst.error_code) == (1, H2Reason.PROTOCOL_ERROR)
        await peer.send_request_headers(3, "GET", "http://x/p", bad, end_stream=True)
        ga = await peer.recv(GoAway)
        assert ga is not None and ga.error_code == H2Reason.ENHANCE_YOUR_CALM


@pytest.mark.tonio
async def test_server_response_date_header_option():
    """hyper's h2 server inserts `Date` when the app set none (proto/h2/server.rs L483),
    gated by `auto_date_header`; an app-supplied `Date` is kept."""
    listener, host, port = await listen()

    async def serve(**opts):
        transport = await listener.accept()
        async with H2Server(transport, **opts) as server:
            async for req in server:
                hdrs = {"date": "Mon, 01 Jan 2024 00:00:00 GMT"} if req.path == "/mine" else None
                await req.respond(204, headers=hdrs)

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")
            assert resp.headers.get("date") is not None
            await resp.read()
            resp = await conn.request("GET", "/mine")
            assert resp.headers["date"] == b"Mon, 01 Jan 2024 00:00:00 GMT"
            await resp.read()
        s.cancel()

    async with scope() as s:
        s.spawn(serve(auto_date_header=False))
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")
            assert resp.headers.get("date") is None
            await resp.read()
        s.cancel()


# ----- push-style responses: `send_response` -> `SendStream` (h2 `SendResponse`/`SendStream`) -----


@pytest.mark.tonio
async def test_server_push_response_streams_data_frames():
    """`send_response` + `send_data(..., end_stream=True)`: HEADERS (with Date) then DATA
    frames, END_STREAM on the last; the connection keeps multiplexing afterwards.
    `detect_eof` (the h1 twin's knob) is accepted and has no effect here."""
    listener, host, port = await listen()

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200, headers={"x-path": req.path}, detect_eof=req.path == "/2")
                await stream.send_data(b"a" * 10)
                await stream.send_data(b"b" * 5, end_stream=True)

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            for path in ("/1", "/2"):
                resp = await conn.request("GET", path)
                assert resp.status == 200
                assert resp.headers["x-path"] == path.encode()
                assert resp.headers.get("date") is not None
                assert await resp.read() == b"a" * 10 + b"b" * 5
        s.cancel()


@pytest.mark.tonio
async def test_server_push_bodyless_and_trailers():
    """`end_stream=True` on `send_response` ends the stream on HEADERS (then `send_data`
    is a misuse); `send_trailers` ends a body with a trailing HEADERS frame."""
    listener, host, port = await listen()
    misuse = []

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                if req.path == "/empty":
                    stream = await req.send_response(204, end_stream=True)
                    try:
                        await stream.send_data(b"nope")
                    except RuntimeError as exc:
                        misuse.append(str(exc))
                else:
                    stream = await req.send_response(200)
                    await stream.send_data(b"payload")
                    await stream.send_trailers({"x-checksum": "abc"})

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/empty")
            assert resp.status == 204
            assert await resp.read() == b""
            resp = await conn.request("GET", "/trailers")
            assert await resp.read() == b"payload"
            assert resp.trailers["x-checksum"] == b"abc"
        s.cancel()
    assert misuse == ["response body already complete"]


@pytest.mark.tonio
async def test_server_push_send_reset_cancels_stream():
    """`send_reset()` mid-body is RST_STREAM(CANCEL) (h2 `SendStream::send_reset`): the
    client's body read fails with that reason; the connection stays usable."""
    listener, host, port = await listen()

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                stream = await req.send_response(200)
                if req.path == "/abort":
                    await stream.send_data(b"partial")
                    await stream.send_reset()
                else:
                    await stream.send_data(b"ok", end_stream=True)

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/abort")
            with pytest.raises(StreamResetError) as exc:
                await resp.read()
            assert exc.value.error_code == H2Reason.CANCEL
            resp = await conn.request("GET", "/fine")  # same connection still serves
            assert await resp.read() == b"ok"
        s.cancel()


@pytest.mark.tonio
async def test_server_respond_with_trailers():
    """`respond(trailers=)`: a trailing HEADERS frame ends the stream after the body
    (also for a bodyless response: HEADERS without END_STREAM, then the trailers);
    connection-specific trailer fields are rejected BEFORE anything is sent, so the
    stream can still be answered."""
    listener, host, port = await listen()
    rejected = []

    async def serve():
        transport = await listener.accept()
        async with H2Server(transport) as server:
            async for req in server:
                if req.path == "/bad":
                    try:
                        await req.respond(200, body=b"x", trailers={"connection": "close"})
                    except H2UserError as exc:
                        rejected.append(str(exc))
                        await req.respond(200, body=b"recovered")
                elif req.path == "/empty":
                    await req.respond(200, trailers={"x-checksum": "empty"})
                else:
                    await req.respond(200, body=b"payload", trailers={"x-checksum": "abc"})

    async with scope() as s:
        s.spawn(serve())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/body")
            assert await resp.read() == b"payload"
            assert resp.trailers["x-checksum"] == b"abc"
            resp = await conn.request("GET", "/empty")
            assert await resp.read() == b""
            assert resp.trailers["x-checksum"] == b"empty"
            resp = await conn.request("GET", "/bad")
            assert await resp.read() == b"recovered"
        s.cancel()
    assert len(rejected) == 1


# ----- post-END_STREAM reset observation (h2 `poll_reset` / `ensure_reason`) -----


@pytest.mark.tonio
async def test_server_reset_received_after_end_stream_then_respond_fails_with_reason():
    """A RST_STREAM arriving after the request's END_STREAM (a GET the client cancels)
    is observable via `reset_received()` with the peer's reason, and a later `respond()`
    fails with that same reason — hyper's first `poll_reset` window (proto/h2/server.rs L456)."""
    seen, done = [], Event()

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    seen.append(await req.reset_received())
                    try:
                        await req.respond(200, body=b"too late")
                    except StreamResetError as exc:
                        seen.append(exc.error_code)
                    done.set()

    async with h2_peer_against(serve) as peer:
        await peer.send_request_headers(1, "GET", "http://x/p", HeaderMap(), end_stream=True)
        await peer.send_rst_stream(1, int(H2Reason.CANCEL))
        await done.wait()
        assert seen == [H2Reason.CANCEL, H2Reason.CANCEL]


@pytest.mark.tonio
async def test_server_respond_fails_fast_when_peer_resets_while_awaiting_body_chunk():
    """hyper's second `poll_reset` window (`PipeToSendStream::poll`, proto/h2/mod.rs L148):
    a reset arriving while the response body is parked on the app's NEXT chunk fails
    `respond()` at once with the peer's reason — the app is not left parked forever."""
    seen, done = [], Event()

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:

                    async def body():
                        try:
                            yield b"first"
                            await Event().wait()  # the app has nothing more to say (yet)
                        finally:
                            seen.append("producer unwound")  # hyper drops the body future: cleanup runs

                    try:
                        await req.respond(200, body=body())
                    except StreamResetError as exc:
                        seen.append(exc.error_code)
                    done.set()

    async with h2_peer_against(serve) as peer:
        await peer.send_request_headers(1, "GET", "http://x/p", HeaderMap(), end_stream=True)
        assert await peer.recv(Headers) is not None
        first = await peer.recv(Data)  # streamed chunks are NOT held back
        assert first is not None and first.data == b"first" and not first.end_stream
        await peer.send_rst_stream(1, int(H2Reason.INTERNAL_ERROR))
        await done.wait()  # would hit the conftest deadline if the sender stayed parked
        # The producer was cancelled at its await and unwound BEFORE respond() raised the
        # PEER'S reason (not a synthesized CANCEL).
        assert seen == ["producer unwound", H2Reason.INTERNAL_ERROR]


@pytest.mark.tonio
async def test_server_send_stream_reports_peer_reset_reason():
    """Through the push handle a post-END_STREAM reset surfaces on the next `send_data`
    as `StreamResetError` carrying the peer's reason (h2 `ensure_reason`)."""
    seen, done = [], Event()

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    stream = await req.send_response(200)
                    try:
                        while True:
                            await stream.send_data(b"tick")
                            await sleep(0.005)
                    except StreamResetError as exc:
                        seen.append(exc.error_code)
                    done.set()

    async with h2_peer_against(serve) as peer:
        await peer.send_request_headers(1, "GET", "http://x/p", HeaderMap(), end_stream=True)
        assert await peer.recv(Data) is not None
        await peer.send_rst_stream(1, int(H2Reason.ENHANCE_YOUR_CALM))
        await done.wait()
        assert seen == [H2Reason.ENHANCE_YOUR_CALM]


@pytest.mark.tonio
async def test_server_reset_received_raises_on_connection_loss():
    """No reason to report when the connection dies under the request: `reset_received()`
    raises the connection error instead (h2 `ensure_reason` -> `Err` for non-reset closures)."""
    seen, done = [], Event()

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server:
                async for req in server:
                    try:
                        seen.append(await req.reset_received())
                    except ConnectionClosedError as exc:
                        seen.append(type(exc))
                    done.set()

    async with h2_peer_against(serve) as peer:
        await peer.send_request_headers(1, "GET", "http://x/p", HeaderMap(), end_stream=True)
        assert await peer.recv(Settings) is not None  # the request was accepted
        peer.close()  # client vanishes
        await done.wait()
        assert seen == [ConnectionClosedError]


@pytest.mark.tonio
async def test_server_goaway_on_remote_reset_flood():
    """Rapid Reset (CVE-2023-44487): a flood of HEADERS+RST_STREAM on streams the app
    never accepts. Reset pending-accept streams stop counting as concurrent, so
    MAX_CONCURRENT can't gate the flood; h2 caps them separately (20) and tears the
    connection down with GOAWAY(ENHANCE_YOUR_CALM)."""

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport):
                await Event().wait()  # hold the connection open; never accept a request

    async with h2_peer_against(serve) as peer:
        sid = 1
        with contextlib.suppress(Exception):  # server may GOAWAY + close mid-flood
            for _ in range(30):  # well past the cap of 20
                await peer.send_request_headers(sid, "GET", "http://x/p", HeaderMap())
                await peer.send_rst_stream(sid, int(H2Reason.CANCEL))
                sid += 2
        ga = await peer.recv(GoAway)
        assert ga is not None
        assert ga.error_code == H2Reason.ENHANCE_YOUR_CALM


@pytest.mark.tonio
async def test_server_goaway_on_idle_stream_data():
    """DATA on a stream the client never opened via HEADERS (idle) is a connection
    PROTOCOL_ERROR — the server GOAWAYs (h2 recv `ensure_not_idle`)."""
    async with h2_peer_against(_serve_forever) as peer:
        await peer.send_data(5, b"x", end_stream=False)  # idle stream 5
        ga = await peer.recv(GoAway)
        assert ga is not None
        assert ga.error_code == H2Reason.PROTOCOL_ERROR


@pytest.mark.tonio
async def test_server_swallows_late_headers_on_reset_stream():
    """A HEADERS frame on a stream the server just locally-reset is swallowed (h2
    reset-stream store), not a connection PROTOCOL_ERROR — so the connection
    survives and still serves later requests (Tier-1 drift #4: the server now
    consults the reset store before the decreased-id check)."""

    async def serve(transport):
        with contextlib.suppress(Exception):
            async with H2Server(transport) as server, scope() as handlers:
                async for req in server:

                    async def handle(r):
                        with contextlib.suppress(Exception):
                            await r.read()
                            await r.respond(200)

                    handlers.spawn(handle(req))

    async with h2_peer_against(serve) as peer:
        # stream 1: declare content-length 5 but send 10 bytes -> the server RSTs stream 1.
        await peer.send_request_headers(1, "POST", "http://x/a", HeaderMap([("content-length", "5")]))
        await peer.send_data(1, b"0123456789", end_stream=False)
        # A late HEADERS on the now-reset stream 1 (the client hadn't seen the RST):
        # must be swallowed, not treated as a decreased-id connection error.
        await peer.send_request_headers(1, "POST", "http://x/a", end_stream=True)
        # A fresh request on stream 3 must still be served (connection alive).
        await peer.send_request_headers(3, "GET", "http://x/b", end_stream=True)

        status, goaway = None, None
        while status is None and goaway is None and (f := await peer.recv(Headers, GoAway)) is not None:
            if isinstance(f, GoAway):
                goaway = f
            elif f.stream_id == 3:
                status = f.status

    assert goaway is None, "connection torn down instead of swallowing the late HEADERS"
    assert status == 200


@pytest.mark.tonio
async def test_server_goaway_on_rst_stream_zero():
    """RST_STREAM on stream 0 is a connection PROTOCOL_ERROR (h2 recv_reset)."""
    async with h2_peer_against(_serve_forever) as peer:
        await peer.send_rst_stream(0, H2Reason.CANCEL)
        ga = await peer.recv(GoAway)
        assert ga is not None
        assert ga.error_code == H2Reason.PROTOCOL_ERROR


@pytest.mark.tonio
async def test_h2_request_trailers_round_trip():
    """Request trailers: the client sends them as a trailing HEADERS frame
    (END_STREAM) after the DATA frames; the H2Server decodes them into `req.trailers`."""
    listener, host, port = await listen()
    seen = {}

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/", body=b"data", trailers={"x-checksum": "abc"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["body"] == b"data"
    assert seen["trailers"] is not None
    assert seen["trailers"].get("x-checksum") == b"abc"


@pytest.mark.tonio
async def test_h2_connection_specific_trailers_rejected():
    """Trailers are a HEADERS block, so connection-specific fields are rejected
    (RFC 9113 §8.2.2; h2 send.rs:80-104) — BEFORE the stream is opened, so the
    connection stays fully usable and a subsequent valid trailer still sends."""
    listener, host, port = await listen()
    seen = {}

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2UserError):
                await conn.request("POST", "/", body=b"data", trailers={"connection": "close"})
            with pytest.raises(H2UserError):  # `te` may only carry "trailers"
                await conn.request("POST", "/", body=b"data", trailers={"te": "gzip"})
            # The rejected calls never touched the wire — the SAME connection
            # still carries a valid-trailer request end to end.
            resp = await conn.request("POST", "/", body=b"data", trailers={"x-checksum": "abc"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["trailers"].get("x-checksum") == b"abc"


@pytest.mark.tonio
async def test_h2_connection_specific_request_headers_rejected():
    """Regular request HEADERS get the same RFC 9113 §8.2.2 rejection as
    trailers (h2 send.rs `send_headers` -> `check_headers`) — before any
    stream/slot state is touched, so the connection stays fully usable."""
    listener, host, port = await listen()

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                await req.read()
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            with pytest.raises(H2UserError):
                await conn.request("GET", "/", headers={"connection": "keep-alive"})
            with pytest.raises(H2UserError):  # `te` may only carry "trailers"
                await conn.request("GET", "/", headers={"te": "gzip"})
            resp = await conn.request("GET", "/", headers={"te": "trailers"})  # the one legal `te`
            assert await resp.read() == b"ok"
        s.cancel()


@pytest.mark.tonio
async def test_h2_connection_specific_response_headers_rejected():
    """The server's response HEADERS get the same §8.2.2 rejection — before the
    state transition, so the handler can still send a valid response on the
    same stream after a rejected attempt."""
    listener, host, port = await listen()

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                await req.read()
                with pytest.raises(H2UserError):
                    await req.respond(200, headers={"connection": "close"}, body=b"nope")
                await req.respond(200, body=b"ok")  # stream untouched -> still respondable

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("GET", "/")
            assert resp.status == 200
            assert await resp.read() == b"ok"
        s.cancel()


@pytest.mark.tonio
async def test_h2_bodyless_request_with_trailers():
    """A request with trailers but NO body still ends on the trailing HEADERS frame
    (HEADERS + trailers, no DATA) rather than END_STREAM on the request HEADERS."""
    listener, host, port = await listen()
    seen = {}

    async def server():
        transport = await listener.accept()
        async with H2Server(transport) as srv:
            async for req in srv:
                seen["body"] = await req.read()
                seen["trailers"] = req.trailers
                await req.respond(200, body=b"ok")

    async with scope() as s:
        s.spawn(server())
        async with open_h2(host, port) as conn:
            resp = await conn.request("POST", "/", trailers={"x-done": "1"})
            assert await resp.read() == b"ok"
        s.cancel()

    assert seen["body"] == b""
    assert seen["trailers"].get("x-done") == b"1"


# ===== peer GOAWAY → acknowledging GOAWAY + connection done (server role) =====
#
# The raw client acks the server's SETTINGS BEFORE it sends GOAWAY: once the server has
# replied GOAWAY it stops reading (h2: `should_close_now` → no more `poll_next`), so a
# SETTINGS ack still in flight would sit unread in its socket buffer and turn its close
# into a TCP RST instead of a FIN.


async def _serve_and_report(transport, done):
    """Serve one connection; set `done` once the accept loop has exited and the
    server's `__aexit__` has run (i.e. the transport is closed)."""
    with contextlib.suppress(Exception):
        async with H2Server(transport) as server, scope() as handlers:
            async for req in server:

                async def handle(r):
                    await r.read()
                    await r.respond(200, body=b"ok")

                handlers.spawn(handle(req))
    done.set()


async def _assert_goaway_then_eof(peer, done, *, seen=()):
    """The one contract under test: the server replies GOAWAY(NO_ERROR), then CLOSES
    (the client reads EOF — without the fix it hangs here), and its accept loop ends."""
    seen = [*seen, *await peer.frames()]
    goaways = [f for f in seen if isinstance(f, GoAway)]
    assert len(goaways) == 1 and goaways[0].error_code == H2Reason.NO_ERROR
    await done.wait()
    peer.close()
    return seen


@pytest.mark.tonio
@pytest.mark.parametrize("last_id", [0, 1])
async def test_server_closes_after_goaway_reply_when_idle(last_id):
    """Peer GOAWAY on an idle connection (one completed request): the server sends its
    acknowledging GOAWAY(NO_ERROR) AND closes the connection / ends the accept loop —
    h2 `go_away_now` → `should_close_now` → the connection future resolves.
    Previously only the GOAWAY went out and the socket leaked forever."""
    done = Event()
    async with h2_peer_against(lambda transport: _serve_and_report(transport, done)) as peer:
        await peer.send_request_headers(1, "GET", "http://x/", end_stream=True)
        seen = []
        while not any(isinstance(f, Headers) for f in seen):  # full response for stream 1
            seen.append(await peer.recv())
            assert seen[-1] is not None
        await peer.send_go_away(last_id, H2Reason.NO_ERROR)
        await _assert_goaway_then_eof(peer, done, seen=seen)


@pytest.mark.tonio
async def test_server_closes_after_goaway_reply_no_streams_ever():
    """Peer GOAWAY right after the handshake, no stream ever opened: same contract."""
    done = Event()
    async with h2_peer_against(lambda transport: _serve_and_report(transport, done)) as peer:
        await peer.handshake()
        await peer.send_go_away(0, H2Reason.NO_ERROR)
        await _assert_goaway_then_eof(peer, done)


@pytest.mark.tonio
async def test_server_closes_after_goaway_received_mid_request():
    """Peer GOAWAY(last=1) while stream 1 is still in flight, and the peer sends NOTHING
    more. Stream 1 finishing is a local event — h2's `drop_stream_ref` wakes the
    connection task so the idle check re-runs (streams.rs L1673-1675); httpunk must do the
    same from the stream-close path, not only after inbound bytes."""
    done = Event()
    async with h2_peer_against(lambda transport: _serve_and_report(transport, done)) as peer:
        await peer.handshake()
        # Request body still pending (no END_STREAM) when the GOAWAY lands...
        await peer.send_request_headers(1, "POST", "http://x/", end_stream=False)
        await peer.send_go_away(1, H2Reason.NO_ERROR)
        # ...then finish it; after this the client is silent.
        await peer.send_data(1, b"x", end_stream=True)
        seen = await _assert_goaway_then_eof(peer, done)
        # Stream 1 was <= last_stream_id, so it was served before the close.
        assert any(isinstance(f, Headers) and f.stream_id == 1 and f.status == 200 for f in seen)


@pytest.mark.tonio
async def test_server_closes_idle_after_phase1_goaway():
    """A received GOAWAY closes an idle connection whatever its last-stream-id, a
    phase-1 graceful one (2^31-1) included: h2 stores every received GOAWAY in
    `inner.error` (proto/connection.rs L596) and, once no stream remains, replies
    GOAWAY(NO_ERROR) and closes (L327-333). `should_close_on_idle`'s `StreamId::MAX`
    exclusion concerns only a GOAWAY the server sent itself (go_away.rs L108-114)."""
    done = Event()
    async with h2_peer_against(lambda transport: _serve_and_report(transport, done)) as peer:
        # Finish the SETTINGS exchange first: nothing of ours is still in flight
        # when the server closes, so its close is a FIN, not a reset.
        await peer.handshake()
        await peer.send_go_away(2**31 - 1, H2Reason.NO_ERROR)
        goaways = [f for f in await peer.frames() if isinstance(f, GoAway)]
        assert [(g.last_stream_id, g.error_code) for g in goaways] == [(0, H2Reason.NO_ERROR)]
        await done.wait()  # the accept loop ended and the server closed the transport
