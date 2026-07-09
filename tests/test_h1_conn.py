"""HTTP/1 client over a tonio loopback: the `H1Connection` driver wiring the Rust
`H1Codec` + `H1BodyDecoder` over a real transport — content-length / chunked /
close-delimited bodies, request bodies, keep-alive reuse, and connection-close."""

import pytest
from _client import open_h1
from tonio.colored import Event, scope
from tonio.colored.net import open_tcp_listeners

from httpunk import H2Error


async def _read_request(stream):
    """Read one HTTP/1 request from `stream` — head + any Content-Length body.
    Returns `(head_bytes, body_bytes)` or None on EOF before a full head."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = await stream.receive_some(65536)
        if not chunk:
            return None
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    clen = 0
    for line in head.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            clen = int(line.split(b":", 1)[1].strip())
    body = bytearray(rest)
    while len(body) < clen:
        chunk = await stream.receive_some(65536)
        if not chunk:
            break
        body += chunk
    return head, bytes(body)


async def _serve(listener, responses, requests, done):
    """Accept one connection; for each queued response, read a request and reply.
    Records requests seen. Then drains until the client closes."""
    try:
        stream = await listener.accept()
        for resp in responses:
            req = await _read_request(stream)
            if req is None:
                return
            requests.append(req)
            await stream.send_all(resp)
        while await stream.receive_some(65536):
            pass
    finally:
        done.set()


async def _listener():
    listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
    host, port = listener.socket.getsockname()[:2]
    return listener, host, port


@pytest.mark.tonio
async def test_get_content_length():
    listener, host, port = await _listener()
    requests, done = [], Event()
    resp = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 5\r\n\r\nhello"

    async with scope() as s:
        s.spawn(_serve(listener, [resp], requests, done))
        async with open_h1(host, port) as conn:
            # Low-level h1 conn: the caller supplies Host (like hyper's
            # client::conn::http1; it is not auto-added).
            r = await conn.request("GET", "/thing", headers={"host": f"{host}:{port}"})
            assert r.status == 200
            assert r.headers["content-type"] == b"text/plain"
            assert await r.read() == b"hello"
        await done.wait()
        s.cancel()

    head = requests[0][0]
    assert head.startswith(b"GET /thing HTTP/1.1\r\n")
    assert f"host: {host}:{port}".encode() in head.lower()


@pytest.mark.tonio
async def test_get_chunked():
    listener, host, port = await _listener()
    requests, done = [], Event()
    resp = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"

    async with scope() as s:
        s.spawn(_serve(listener, [resp], requests, done))
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/")
            assert await r.read() == b"hello world"
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_chunked_trailers_surfaced():
    listener, host, port = await _listener()
    requests, done = [], Event()
    # A chunked body followed by trailing headers (hyper delivers them as
    # Frame::trailers; we surface them on the response).
    resp = (
        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ntrailer: x-checksum\r\n\r\n"
        b"5\r\nhello\r\n0\r\nx-checksum: abc123\r\n\r\n"
    )

    async with scope() as s:
        s.spawn(_serve(listener, [resp], requests, done))
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/")
            assert await r.read() == b"hello"
            assert r.trailers is not None
            assert r.trailers["x-checksum"] == b"abc123"
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_streaming_body_iter():
    listener, host, port = await _listener()
    requests, done = [], Event()
    resp = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n"

    async with scope() as s:
        s.spawn(_serve(listener, [resp], requests, done))
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/")
            chunks = [c async for c in r.aiter_bytes()]
        await done.wait()
        s.cancel()

    assert b"".join(chunks) == b"abcdef"


@pytest.mark.tonio
async def test_bodyless_response_frees_slot_without_read():
    """A bodyless response (204/HEAD/CL:0) must free the in-flight slot as soon as
    it is returned — a caller that only inspects status/headers and never reads a
    (nonexistent) body must still be able to send the next request. Regression:
    the decoder was reporting is_complete=False at construction, so the slot leaked."""
    listener, host, port = await _listener()
    requests, done = [], Event()
    r1 = b"HTTP/1.1 204 No Content\r\n\r\n"  # bodyless, keep-alive (1.1 default)
    r2 = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi"

    async with scope() as s:
        s.spawn(_serve(listener, [r1, r2], requests, done))
        async with open_h1(host, port) as conn:
            resp1 = await conn.request("GET", "/a")
            assert resp1.status == 204
            # Deliberately do NOT read resp1 — there is no body. The connection
            # must be reusable immediately, so this second request must not hang.
            assert await (await conn.request("GET", "/b")).read() == b"hi"
        await done.wait()
        s.cancel()

    assert requests[0][0].startswith(b"GET /a ")
    assert requests[1][0].startswith(b"GET /b ")


@pytest.mark.tonio
async def test_keep_alive_two_requests():
    listener, host, port = await _listener()
    requests, done = [], Event()
    r1 = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"
    r2 = b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\nbye"

    async with scope() as s:
        s.spawn(_serve(listener, [r1, r2], requests, done))
        async with open_h1(host, port) as conn:
            assert await (await conn.request("GET", "/a")).read() == b"ok"
            assert await (await conn.request("GET", "/b")).read() == b"bye"  # same connection reused
        await done.wait()
        s.cancel()

    # both requests arrived on the one connection
    assert requests[0][0].startswith(b"GET /a ")
    assert requests[1][0].startswith(b"GET /b ")


@pytest.mark.tonio
async def test_http10_downgrade_reasserts_keep_alive_by_value_replacing_token():
    """After a peer answers HTTP/1.0, the client downgrades later requests to 1.0 and
    re-asserts keep-alive by VALUE (not mere header presence), REPLACING a non-keep-alive
    Connection token rather than leaving the 1.0 request to close — hyper fix_keep_alive (F58)."""
    listener, host, port = await _listener()
    requests, done = [], Event()
    r1 = b"HTTP/1.0 200 OK\r\nconnection: keep-alive\r\ncontent-length: 2\r\n\r\nok"
    r2 = b"HTTP/1.0 200 OK\r\nconnection: keep-alive\r\ncontent-length: 3\r\n\r\nbye"

    async with scope() as s:
        s.spawn(_serve(listener, [r1, r2], requests, done))
        async with open_h1(host, port) as conn:
            assert await (await conn.request("GET", "/a")).read() == b"ok"  # 1.0 keep-alive → reused + downgrade
            # r2 carries a custom Connection token; the downgrade must still re-assert
            # keep-alive (the old presence-check left it, so the server would close).
            assert await (await conn.request("GET", "/b", headers={"connection": "x-foo"})).read() == b"bye"
        await done.wait()
        s.cancel()

    r2_head = requests[1][0].lower()
    assert b"connection: keep-alive" in r2_head  # re-asserted by value
    assert b"x-foo" not in r2_head  # the custom token was replaced, not left / duplicated


@pytest.mark.tonio
async def test_connection_close_refuses_reuse():
    listener, host, port = await _listener()
    requests, done = [], Event()
    resp = b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi"

    async with scope() as s:
        s.spawn(_serve(listener, [resp], requests, done))
        async with open_h1(host, port) as conn:
            assert await (await conn.request("GET", "/")).read() == b"hi"
            with pytest.raises(H2Error):  # ConnectionClosedError: server said Connection: close
                await conn.request("GET", "/again")
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_close_delimited_body():
    listener, host, port = await _listener()
    done = Event()

    async def server():
        try:
            stream = await listener.accept()
            await _read_request(stream)
            # no content-length, no chunked, Connection: close -> body ends at EOF
            await stream.send_all(b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\nbody-until-eof")
            stream.close()
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h1(host, port) as conn:
            r = await conn.request("GET", "/")
            assert await r.read() == b"body-until-eof"
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_early_response_during_upload():
    """A server that answers before reading the full request body (413/redirect/
    auth) and stops reading must not deadlock the client. hyper interleaves reads
    and writes, so the early response is delivered even while a large body is
    still being written (dispatch.rs `poll_loop`)."""
    listener, host, port = await _listener()
    done = Event()

    async def server():
        try:
            stream = await listener.accept()
            # Read only the request head, then answer early and stop reading the
            # (large) body — the client's write would block on a full send buffer.
            buf = b""
            while b"\r\n\r\n" not in buf:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    return
                buf += chunk
            await stream.send_all(
                b"HTTP/1.1 413 Payload Too Large\r\nconnection: close\r\ncontent-length: 3\r\n\r\nbig"
            )
            stream.close()
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h1(host, port) as conn:
            big = b"x" * (5 * 1024 * 1024)  # larger than the socket send buffer
            r = await conn.request("POST", "/upload", headers={"host": f"{host}:{port}"}, body=big)
            assert r.status == 413
            assert await r.read() == b"big"
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_early_response_does_not_truncate_upload():
    """A keep-alive server answers before reading the body but keeps draining it: the
    client must finish writing the request body — hyper's poll_loop does NOT truncate
    the upload at head-arrival (dispatch.rs L172-211), it keeps writing (F11). The
    response body is withheld until the whole request has been drained, so the client
    cannot release the slot (and cancel the writer) until the upload has completed;
    the old behavior cancelled the writer at head-arrival, so the server's drain would
    hang forever waiting for bytes that were never sent."""
    listener, host, port = await _listener()
    done = Event()
    body = b"x" * (5 * 1024 * 1024)  # > the socket send buffer, so the writer is mid-send
    received = {}

    async def server():
        stream = await listener.accept()
        buf = b""
        while b"\r\n\r\n" not in buf:
            buf += await stream.receive_some(65536)
        await stream.send_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n")  # early HEAD only
        drained = len(buf.split(b"\r\n\r\n", 1)[1])
        while drained < len(body):
            drained += len(await stream.receive_some(65536))
        received["n"] = drained
        await stream.send_all(b"ok")  # only now can the client's r.read() complete
        stream.close()
        done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h1(host, port) as conn:
            r = await conn.request("POST", "/up", headers={"host": f"{host}:{port}"}, body=body)
            assert r.status == 200
            assert await r.read() == b"ok"  # completes only after the full body was drained
        await done.wait()
        s.cancel()
    assert received["n"] == len(body)  # the upload was NOT truncated at head-arrival


@pytest.mark.tonio
async def test_101_upgrade_tunnel():
    """A 101 Switching Protocols hands off the raw transport: the response carries
    an `H1Upgraded` the caller drives directly (hyper `Upgraded`). Bytes the server
    sent right after the 101 head are delivered first, then the tunnel echoes.
    The transport must survive the connection's `async with` exit."""
    listener, host, port = await _listener()
    done = Event()

    async def server():
        try:
            stream = await listener.accept()
            buf = b""
            while b"\r\n\r\n" not in buf:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    return
                buf += chunk
            # 101 + immediately some upgraded-protocol bytes (become `leftover`).
            await stream.send_all(
                b"HTTP/1.1 101 Switching Protocols\r\nupgrade: myproto\r\nconnection: upgrade\r\n\r\nHELLO"
            )
            while True:  # echo the upgraded protocol
                data = await stream.receive_some(65536)
                if not data:
                    break
                await stream.send_all(b"echo:" + data)
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h1(host, port) as conn:
            resp = await conn.request(
                "GET", "/ws", headers={"host": f"{host}:{port}", "upgrade": "myproto", "connection": "upgrade"}
            )
            assert resp.status == 101
            assert resp.is_upgrade
            up = resp.upgraded
        # The connection has exited its `async with`, but the tunnel is still live.
        assert await up.receive_some() == b"HELLO"  # bytes buffered past the 101 head
        await up.send_all(b"ping")
        assert await up.receive_some() == b"echo:ping"
        await up.aclose()
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_connect_tunnel():
    """A 2xx to a CONNECT request is a tunnel: the response is an upgrade and the
    transport is handed off as an `H1Upgraded`."""
    listener, host, port = await _listener()
    done = Event()

    async def server():
        try:
            stream = await listener.accept()
            buf = b""
            while b"\r\n\r\n" not in buf:
                chunk = await stream.receive_some(65536)
                if not chunk:
                    return
                buf += chunk
            assert buf.startswith(b"CONNECT example.com:443 HTTP/1.1\r\n")
            await stream.send_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            while True:
                data = await stream.receive_some(65536)
                if not data:
                    break
                await stream.send_all(data[::-1])  # echo reversed
        finally:
            done.set()

    async with scope() as s:
        s.spawn(server())
        async with open_h1(host, port) as conn:
            resp = await conn.request("CONNECT", "example.com:443", headers={"host": "example.com:443"})
            assert resp.status == 200
            assert resp.is_upgrade
            async with resp.upgraded as up:
                await up.send_all(b"abc")
                assert await up.receive_some() == b"cba"
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_post_request_body():
    listener, host, port = await _listener()
    requests, done = [], Event()
    resp = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"

    async with scope() as s:
        s.spawn(_serve(listener, [resp], requests, done))
        async with open_h1(host, port) as conn:
            r = await conn.request("POST", "/submit", body=b"payload!")
            assert r.status == 200
            assert await r.read() == b""
        await done.wait()
        s.cancel()

    head, body = requests[0]
    assert head.startswith(b"POST /submit HTTP/1.1\r\n")
    assert b"content-length: 8" in head.lower()
    assert body == b"payload!"


@pytest.mark.tonio
async def test_http10_keepalive_peer_downgrades_next_request():
    """After a peer answers in HTTP/1.0 (opting into keep-alive), the client
    downgrades subsequent requests on the reused connection to HTTP/1.0 and
    re-asserts Connection: keep-alive — hyper's enforce_version / fix_keep_alive
    (conn.rs L662-702). The first request (peer unknown) stays HTTP/1.1 (G33)."""
    listener, host, port = await _listener()
    requests, done = [], Event()
    r10 = b"HTTP/1.0 200 OK\r\ncontent-length: 2\r\nconnection: keep-alive\r\n\r\nok"

    async with scope() as s:
        s.spawn(_serve(listener, [r10, r10], requests, done))
        async with open_h1(host, port) as conn:
            assert await (await conn.request("GET", "/a", headers={"host": f"{host}:{port}"})).read() == b"ok"
            assert await (await conn.request("GET", "/b", headers={"host": f"{host}:{port}"})).read() == b"ok"
        await done.wait()
        s.cancel()

    assert requests[0][0].startswith(b"GET /a HTTP/1.1\r\n")  # peer unknown → 1.1
    assert requests[1][0].startswith(b"GET /b HTTP/1.0\r\n")  # downgraded after the 1.0 response
    assert b"connection: keep-alive" in requests[1][0].lower()  # re-asserted


@pytest.mark.tonio
async def test_unexpected_bytes_past_body_poison_connection():
    """A server that sends bytes past the response body violates HTTP/1 (it may
    not speak before the next request). The client must fail the connection —
    hyper `require_empty_read` -> `new_unexpected_message` (conn.rs L463-465) —
    rather than silently drop the extra bytes and reuse a corrupted stream (G35).
    The response itself is still delivered intact."""
    listener, host, port = await _listener()
    done = Event()

    async def serve():
        try:
            stream = await listener.accept()
            await _read_request(stream)
            # a valid 2-byte response, then UNSOLICITED junk past the body
            await stream.send_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhiSURPRISE-JUNK")
            while await stream.receive_some(65536):
                pass
        finally:
            done.set()

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            r1 = await conn.request("GET", "/a", headers={"host": f"{host}:{port}"})
            assert await r1.read() == b"hi"  # the response is intact
            with pytest.raises(ValueError, match="unexpected"):
                await conn.request("GET", "/b", headers={"host": f"{host}:{port}"})  # connection poisoned
        await done.wait()
        s.cancel()


@pytest.mark.tonio
async def test_reused_connection_poisoned_by_idle_window_bytes():
    """Bytes a server sends on an ALREADY-IDLE connection — after the client has fully
    consumed the previous response, so NOT coalesced into its body buffer — must still
    poison the connection. The pre-write `receive_nowait` check catches them so the next
    request fails rather than misparsing them as its response — hyper's require_empty_read
    the moment before sending (F31, problem b). Without the pre-write check these bytes
    live only on the socket, past the decoder, and go unnoticed until misparsed."""
    listener, host, port = await _listener()
    r1_read, junk_sent = Event(), Event()

    async def serve():
        stream = await listener.accept()
        await _read_request(stream)
        await stream.send_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
        await r1_read.wait()  # the client has fully consumed r1 — the connection is idle
        await stream.send_all(b"HTTP/1.1 500 unsolicited\r\n\r\n")  # junk into the idle socket
        junk_sent.set()
        while await stream.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            assert await (await conn.request("GET", "/a", headers={"host": f"{host}:{port}"})).read() == b"ok"
            r1_read.set()
            await junk_sent.wait()  # the junk is now sitting in the client's socket buffer
            with pytest.raises(ValueError, match="unexpected"):
                await conn.request("GET", "/b", headers={"host": f"{host}:{port}"})
        s.cancel()


@pytest.mark.tonio
async def test_body_iterable_error_fails_promptly():
    """A request body iterable that raises mid-upload fails send_request PROMPTLY with
    that error, instead of the error being swallowed / hanging on the response head
    until the server times out (F12)."""
    listener, host, port = await _listener()

    class _BoomError(Exception):
        pass

    def body():
        yield b"chunk-1"
        raise _BoomError("body generator failed mid-upload")

    async def serve():
        # Read whatever arrives; NEVER respond (the request is incomplete). Without the
        # fix the client would hang here forever; with it, the client closes on the body
        # error and this drains to EOF.
        transport = await listener.accept()
        while await transport.receive_some(65536):
            pass

    async with scope() as s:
        s.spawn(serve())
        async with open_h1(host, port) as conn:
            with pytest.raises(_BoomError):
                await conn.request("POST", "/", headers={"host": f"{host}:{port}"}, body=body())
        s.cancel()
