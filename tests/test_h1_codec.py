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
