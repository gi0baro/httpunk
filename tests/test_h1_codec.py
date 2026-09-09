"""HTTP/1 codec: the vendored hyper h1 sans-IO core (head parse/encode + body
Encoder), driven via the `H1Codec` PyO3 glue with zero I/O."""

import pytest

from httpunk._httpunk import H1Codec, H1ResponseHead
from httpunk.exceptions import H1ParseError, H1UserError
from httpunk.http import HeaderMap


def test_serialize_request_content_length():
    codec = H1Codec()
    # The target is serialized verbatim; a path yields an origin-form request line.
    head = codec.serialize_request(
        "POST", "/x?q=1", HeaderMap([("host", "h"), ("content-type", "text/plain")]), content_length=5
    )
    assert head.startswith(b"POST /x?q=1 HTTP/1.1\r\n")
    assert b"host: h\r\n" in head
    assert b"content-length: 5\r\n" in head
    assert head.endswith(b"\r\n\r\n")
    # content-length body is written raw; end() adds nothing.
    assert codec.serialize_data(b"hello") == b"hello"
    assert codec.serialize_end() == b""


def test_serialize_request_no_body():
    codec = H1Codec()
    head = codec.serialize_request("GET", "/", HeaderMap([("host", "h")]))
    # A bodyless request carries no framing header (no content-length / chunked).
    assert head == b"GET / HTTP/1.1\r\nhost: h\r\n\r\n"
    assert codec.serialize_end() == b""


def test_serialize_request_absolute_form_for_proxy():
    # An absolute-URL target is sent in absolute-form (for an HTTP proxy), and an
    # authority target in authority-form (CONNECT) — hyper sends the target as-is.
    codec = H1Codec()
    head = codec.serialize_request("GET", "http://example.com/p?q=1", HeaderMap([("host", "example.com")]))
    assert head.startswith(b"GET http://example.com/p?q=1 HTTP/1.1\r\n")

    codec2 = H1Codec()
    connect = codec2.serialize_request("CONNECT", "example.com:443", HeaderMap([("host", "example.com:443")]))
    assert connect.startswith(b"CONNECT example.com:443 HTTP/1.1\r\n")


def test_serialize_request_chunked():
    codec = H1Codec()
    head = codec.serialize_request("POST", "/upload", HeaderMap([("host", "h")]), chunked=True)
    assert head.startswith(b"POST /upload HTTP/1.1\r\n")
    assert b"transfer-encoding: chunked\r\n" in head
    assert codec.serialize_data(b"hi") == b"2\r\nhi\r\n"
    assert codec.serialize_data(b"world!") == b"6\r\nworld!\r\n"
    assert codec.serialize_end() == b"0\r\n\r\n"


def test_receive_response_content_length():
    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    raw = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 5\r\n\r\nhello"
    ev = codec.receive_head(raw)
    assert isinstance(ev, H1ResponseHead)
    assert ev.status == 200
    assert ev.keep_alive is True
    assert ev.body_kind == "length"
    assert ev.content_length == 5
    assert ev.headers["content-type"] == b"text/plain"
    # the head is consumed; the body bytes already received are buffered.
    assert codec.take_body() == b"hello"
    assert codec.buffered() == 0


def test_receive_response_chunked():
    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    raw = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
    ev = codec.receive_head(raw)
    assert ev.status == 200
    assert ev.body_kind == "chunked"
    assert ev.content_length is None
    # body framing (decode) is Python's job; the codec just hands back the bytes.
    assert codec.take_body() == b"5\r\nhello\r\n0\r\n\r\n"


def test_receive_response_connection_close():
    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    raw = b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\nbody-until-eof"
    ev = codec.receive_head(raw)
    assert ev.keep_alive is False
    assert ev.body_kind == "close"  # no length, no chunked -> delimited by EOF
    assert codec.take_body() == b"body-until-eof"


def test_receive_head_needs_more_bytes():
    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    raw = b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n"
    assert codec.receive_head(raw[:20]) is None  # partial head -> need more
    ev = codec.receive_head(raw[20:])  # rest completes it
    assert ev is not None
    assert ev.status == 204
    assert ev.body_kind == "empty"


def test_receive_malformed_response_raises():
    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    with pytest.raises(H1ParseError) as ei:
        codec.receive_head(b"NOT-HTTP garbage\r\n\r\n")
    assert ei.value.args[0] == "version"  # hyper `Parse::Version` (httparse: no `HTTP/` after the status line start)


# ----- hyper `Kind::Parse` / `Kind::User` mirrored as `H1ParseError` / `H1UserError` -----


def test_parse_error_kinds_mirror_hyper_parse_variants():
    """`H1ParseError.args[0]` is the hyper `Parse` variant (`Parse::Header(h)` as
    `header_<h>`), with hyper's `Display` text as the message."""
    codec = H1Codec()
    with pytest.raises(H1ParseError) as ei:
        codec.receive_request_head(b"GET / HTTP/1.1\r\ncontent-length: abc\r\n\r\n")
    assert ei.value.args[0] == "header_content_length_invalid"
    assert ei.value.args[1] == "invalid content-length parsed"

    codec = H1Codec()
    with pytest.raises(H1ParseError) as ei:
        codec.receive_request_head(b"GET / HTTP/1.1\r\nBad Header Here\r\n\r\n")
    assert ei.value.args[0] == "header_token"
    assert ei.value.args[1] == "invalid HTTP header parsed"

    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    with pytest.raises(H1ParseError) as ei:
        codec.receive_head(b"HTTP/1.1 12 Nope\r\n\r\n")
    assert ei.value.args[0] == "status"


def test_response_1xx_status_is_user_error_unsupported_status_code():
    """hyper `Server::encode` refuses a 1xx (not 101) response (role.rs L400-405,
    `User::UnsupportedStatusCode`) and rewinds `dst`: nothing is encoded."""
    codec = H1Codec()
    codec.receive_request_head(b"GET / HTTP/1.1\r\n\r\n")
    with pytest.raises(H1UserError) as ei:
        codec.serialize_response(102)
    assert ei.value.args[0] == "unsupported_status_code"
    assert ei.value.args[1] == "response has 1xx status code, not supported by server"


def test_response_content_length_and_transfer_encoding_is_user_error_unexpected_header():
    """Both `content-length` and `transfer-encoding` on a response: hyper `Server::encode`
    cancels with `User::UnexpectedHeader` (role.rs L803-807) and rewinds `dst`."""
    codec = H1Codec()
    codec.receive_request_head(b"GET / HTTP/1.1\r\n\r\n")
    hdrs = HeaderMap([("content-length", "5"), ("transfer-encoding", "chunked")])
    with pytest.raises(H1UserError) as ei:
        codec.serialize_response(200, hdrs, content_length=5)
    assert ei.value.args[0] == "unexpected_header"
    assert ei.value.args[1] == "user sent unexpected header"


def test_body_short_of_content_length_is_user_error_body_write_aborted():
    """Ending a Content-Length body early: the encoder's `NotEof` -> hyper `end_body`
    -> `User::BodyWriteAborted` (conn.rs). Both roles, and with trailers."""
    codec = H1Codec()
    codec.serialize_request("POST", "http://h/", HeaderMap([("host", "h")]), content_length=5)
    codec.serialize_data(b"ab")
    with pytest.raises(H1UserError) as ei:
        codec.serialize_end()
    assert ei.value.args[0] == "body_write_aborted"
    assert ei.value.args[1] == "user body write aborted: early end, expected 3 more bytes"

    codec = H1Codec()
    codec.receive_request_head(b"GET / HTTP/1.1\r\n\r\n")
    codec.serialize_response(200, content_length=5)
    with pytest.raises(H1UserError) as ei:
        codec.serialize_trailers(HeaderMap([("x-t", "1")]))  # not chunked: falls back to `end()`
    assert ei.value.args[0] == "body_write_aborted"


def test_invalid_status_argument_stays_value_error():
    """Caller-argument validation (the `http` crate's `StatusCode::from_u16`) is not a
    hyper error kind: a plain `ValueError`, not an `H1Error`."""
    codec = H1Codec()
    with pytest.raises(ValueError, match="invalid status"):
        codec.serialize_response(1000)


def test_head_terminated_by_bare_lf_then_crlf_parses():
    """hyper 1.11.1 `is_complete_fast`: a bare-LF header line ending followed by a CRLF
    blank line ends the head (httparse accepts it), so the partial-read fast path must
    recognise it too instead of waiting for more bytes. Both roles."""
    codec = H1Codec()
    head = codec.receive_request_head(b"GET / HTTP/1.1\r\na: b\n\r\n")
    assert head is not None and head.headers["a"] == b"b"

    codec = H1Codec()
    codec.serialize_request("GET", "http://h/", HeaderMap([("host", "h")]))
    head = codec.receive_head(b"HTTP/1.1 204 No Content\r\na: b\n\r\n")
    assert head is not None and head.status == 204 and head.headers["a"] == b"b"


# ----- body framing crosses the boundary without copying (BOUNDARY_NOTES.md §2.2) -----


def test_serialize_data_content_length_returns_the_callers_object():
    # hyper `BufKind::Exact`: the chunk within the declared length is the frame itself.
    codec = H1Codec()
    codec.serialize_request("POST", "/", HeaderMap([("host", "h")]), content_length=10)
    first, second = b"hello", b"world"
    assert codec.serialize_data(first) is first
    assert codec.serialize_data(second) is second
    assert codec.serialize_end() == b""


def test_serialize_data_over_long_chunk_is_truncated_to_the_declared_length():
    # hyper `BufKind::Limited`: a chunk past the declared Content-Length is cut, not sent.
    codec = H1Codec()
    codec.serialize_request("POST", "/", HeaderMap([("host", "h")]), content_length=3)
    chunk = b"hello"
    out = codec.serialize_data(chunk)
    assert out == b"hel"
    assert out is not chunk
    assert codec.serialize_data(b"more") == b""  # nothing left to send
    assert codec.serialize_end() == b""


def test_serialize_data_close_delimited_returns_the_callers_object():
    # An HTTP/1.0 response with no length: close-delimited, the body goes out as is.
    codec = H1Codec()
    codec.receive_request_head(b"GET / HTTP/1.0\r\n\r\n")
    # An unknown-length body to a 1.0 peer cannot be chunked: hyper makes it
    # close-delimited (role.rs `Server::encode`, `set_length`).
    codec.serialize_response(200, keep_alive=False, http10=True, chunked=True)
    assert codec.response_close_delimited
    chunk = b"raw body"
    assert codec.serialize_data(chunk) is chunk
    assert codec.serialize_end() == b""


def test_serialize_data_chunked_is_a_new_exact_object_and_empty_chunks_pass_through():
    codec = H1Codec()
    codec.serialize_request("POST", "/", HeaderMap([("host", "h")]), chunked=True)
    chunk = b"abc"
    out = codec.serialize_data(chunk)
    assert out == b"3\r\nabc\r\n" and out is not chunk
    empty = b""
    assert codec.serialize_data(empty) is empty  # hyper never encodes an empty chunk
    assert codec.serialize_end() == b"0\r\n\r\n"


def test_serialize_end_hands_out_one_shared_terminator():
    ends = []
    for _ in range(2):
        codec = H1Codec()
        codec.serialize_request("POST", "/", HeaderMap([("host", "h")]), chunked=True)
        codec.serialize_data(b"x")
        ends.append(codec.serialize_end())
    assert ends[0] == b"0\r\n\r\n"
    assert ends[0] is ends[1]


def test_serialize_trailers_block_and_fallback():
    codec = H1Codec()
    codec.serialize_request("POST", "/", HeaderMap([("host", "h"), ("trailer", "x-t")]), chunked=True)
    codec.serialize_data(b"ab")
    out = codec.serialize_trailers(HeaderMap([("x-t", "1"), ("x-dropped", "2")]))
    assert out == b"0\r\nx-t: 1\r\n\r\n"  # only the declared field survives (hyper `encode_trailers`)
    # No declared trailer: the bare terminator, the shared object.
    codec = H1Codec()
    codec.serialize_request("POST", "/", HeaderMap([("host", "h")]), chunked=True)
    codec.serialize_data(b"ab")
    assert codec.serialize_trailers(HeaderMap([("x-t", "1")])) is _shared_chunked_end()


def _shared_chunked_end():
    codec = H1Codec()
    codec.serialize_request("POST", "/", HeaderMap([("host", "h")]), chunked=True)
    codec.serialize_data(b"x")
    return codec.serialize_end()


def test_serialize_head_and_body_is_the_pieces_in_one_object():
    for framing, body, expect_body in (
        ({"content_length": 5}, b"hello", b"hello"),
        ({"chunked": True}, b"hello", b"5\r\nhello\r\n0\r\n\r\n"),
        ({"content_length": 3}, b"hello", b"hel"),  # truncated, as `serialize_data` would
        ({}, b"", b""),  # no framing: the head alone
    ):
        codec = H1Codec()
        codec.receive_request_head(b"GET / HTTP/1.1\r\n\r\n")
        head = codec.serialize_response(200, **framing)
        out = codec.serialize_head_and_body(head, body)
        assert out == head + expect_body, framing
    # Trailers ride the coalesced write when the request allowed them.
    codec = H1Codec()
    codec.receive_request_head(b"GET / HTTP/1.1\r\nte: trailers\r\n\r\n")
    head = codec.serialize_response(200, HeaderMap([("trailer", "x-t")]), chunked=True)
    out = codec.serialize_head_and_body(head, b"hi", HeaderMap([("x-t", "1")]))
    assert out == head + b"2\r\nhi\r\n0\r\nx-t: 1\r\n\r\n"


def test_head_buffer_is_reused_across_messages():
    # hyper keeps one `WriteBuf.headers`; each head is still its own `bytes` object.
    codec = H1Codec()
    codec.receive_request_head(b"GET /a HTTP/1.1\r\n\r\n")
    h1 = codec.serialize_response(200, content_length=0)
    codec.receive_request_head(b"GET /b HTTP/1.1\r\n\r\n")
    h2 = codec.serialize_response(404, content_length=0)
    assert h1.startswith(b"HTTP/1.1 200 OK\r\n") and h2.startswith(b"HTTP/1.1 404 Not Found\r\n")
    assert h1 is not h2


def test_take_body_into_moves_head_adjacent_body_bytes_and_later_feeds_continue():
    # The client's shape (BOUNDARY_NOTES S9): body bytes that arrived with the head move
    # from the codec's read buffer straight into the decoder, then the decoder keeps
    # taking transport bytes; the body decodes whole and in order.
    from httpunk._httpunk import H1BodyDecoder

    codec = H1Codec()
    codec.serialize_request("GET", "/", HeaderMap([("host", "h")]))
    head = codec.receive_head(b"HTTP/1.1 200 OK\r\ncontent-length: 12\r\n\r\nhello, ")
    assert head is not None and head.body_kind == "length" and head.content_length == 12
    decoder = H1BodyDecoder(head.body_kind, head.content_length)
    codec.take_body_into(decoder)
    assert codec.buffered() == 0
    assert decoder.decode() == b"hello, "
    assert decoder.decode() is None and not decoder.is_complete
    decoder.feed(b"world")
    assert decoder.decode() == b"world"
    assert decoder.is_complete


def test_take_body_into_with_nothing_buffered_is_a_no_op():
    from httpunk._httpunk import H1BodyDecoder

    codec = H1Codec()
    codec.serialize_request("GET", "/", HeaderMap([("host", "h")]))
    head = codec.receive_head(b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\n")
    decoder = H1BodyDecoder(head.body_kind, head.content_length)
    codec.take_body_into(decoder)
    assert decoder.decode() is None and decoder.buffered == 0
    decoder.feed(b"abc")
    assert decoder.decode() == b"abc" and decoder.is_complete


# ----- head strings are built once and shared where the set is closed (rules 6/7) -----


def _request_head(codec, raw):
    head = codec.receive_request_head(raw)
    assert head is not None
    codec.take_body()
    return head


def test_request_head_strings_are_shared_and_built_once():
    a = _request_head(H1Codec(), b"GET /a?x=1 HTTP/1.1\r\nhost: h\r\n\r\n")
    b = _request_head(H1Codec(), b"GET /b HTTP/1.1\r\nhost: h\r\ncontent-length: 2\r\n\r\nhi")
    assert a.method == "GET" and a.method is b.method  # one interned object per standard method
    assert a.target == "/a?x=1" and a.target is a.target  # built at parse, handed out by reference
    assert b.target == "/b"
    assert a.body_kind == "empty" and b.body_kind == "length"
    assert b.body_kind is _request_head(H1Codec(), b"POST / HTTP/1.1\r\ncontent-length: 1\r\n\r\nx").body_kind
    ext = _request_head(H1Codec(), b"PURGE /c HTTP/1.1\r\nhost: h\r\n\r\n")
    assert ext.method == "PURGE"  # an extension method: a plain str


def test_request_target_forms_survive_the_parse():
    assert _request_head(H1Codec(), b"GET http://h/p?q=1 HTTP/1.1\r\nhost: h\r\n\r\n").target == "http://h/p?q=1"
    assert _request_head(H1Codec(), b"CONNECT h:443 HTTP/1.1\r\nhost: h:443\r\n\r\n").target == "h:443"
    assert _request_head(H1Codec(), b"OPTIONS * HTTP/1.1\r\nhost: h\r\n\r\n").target == "*"


def test_response_head_body_kind_is_shared():
    heads = []
    for _ in range(2):
        codec = H1Codec()
        codec.serialize_request("GET", "/", HeaderMap([("host", "h")]))
        heads.append(codec.receive_head(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"))
    assert heads[0].body_kind == "chunked" and heads[0].body_kind is heads[1].body_kind
