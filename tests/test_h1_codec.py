"""HTTP/1 codec: the vendored hyper h1 sans-IO core (head parse/encode + body
Encoder), driven via the `H1Codec` PyO3 glue with zero I/O."""

import pytest

from httpunk._httpunk import H1Codec, H1ResponseHead
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
    with pytest.raises(ValueError):
        codec.receive_head(b"NOT-HTTP garbage\r\n\r\n")
