"""HTTP/1 body decoder — the Rust `H1BodyDecoder`, which drives the vendored
hyper `proto/h1/decode.rs` synchronously (content-length / chunked / close).

`decode()` returns a `bytes` chunk or `None`; `None` is "no chunk right now" —
end vs. need-more is told by `is_complete`."""

import pytest

from httpunk._httpunk import H1BodyDecoder


def _pull(dec):
    """Pull all chunks currently available; returns (joined_bytes, is_complete)."""
    chunks = []
    while (c := dec.decode()) is not None:
        chunks.append(c)
    return b"".join(chunks), dec.is_complete


def test_empty():
    body, complete = _pull(H1BodyDecoder("empty"))
    assert body == b"" and complete


def test_length_whole():
    dec = H1BodyDecoder("length", 5)
    dec.feed(b"hello")
    body, complete = _pull(dec)
    assert body == b"hello" and complete


def test_length_split_across_feeds():
    dec = H1BodyDecoder("length", 5)
    dec.feed(b"hel")
    body, complete = _pull(dec)
    assert body == b"hel" and not complete  # 2 bytes still expected
    dec.feed(b"lo")
    body, complete = _pull(dec)
    assert body == b"lo" and complete


def test_length_stops_at_declared_length():
    # extra bytes (e.g. a pipelined next response) are not consumed as body.
    dec = H1BodyDecoder("length", 3)
    dec.feed(b"abcHTTP/1.1 200 OK")
    body, complete = _pull(dec)
    assert body == b"abc" and complete


def test_chunked_simple():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5\r\nhello\r\n0\r\n\r\n")
    body, complete = _pull(dec)
    assert body == b"hello" and complete


def test_chunked_multiple_and_hex_size():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"1a\r\n" + b"x" * 26 + b"\r\n3\r\nabc\r\n0\r\n\r\n")
    body, complete = _pull(dec)
    assert body == b"x" * 26 + b"abc" and complete


def test_chunked_extension_ignored():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5;name=value\r\nhello\r\n0\r\n\r\n")
    body, complete = _pull(dec)
    assert body == b"hello" and complete


def test_chunked_trailers_consumed():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5\r\nhello\r\n0\r\nExpires: 0\r\nX-Trace: abc\r\n\r\n")
    body, complete = _pull(dec)
    assert body == b"hello" and complete  # trailers consumed, terminates cleanly


def test_chunked_split_mid_size_and_mid_body():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"a")  # partial hex size (0xa = 10)
    body, complete = _pull(dec)
    assert body == b"" and not complete
    dec.feed(b"\r\nhelloworld")  # size CRLF + the 10 body bytes (no trailing CRLF yet)
    body, complete = _pull(dec)
    assert body == b"helloworld" and not complete
    dec.feed(b"\r\n0\r\n\r\n")  # close the chunk + terminating zero-chunk
    body, complete = _pull(dec)
    assert body == b"" and complete


def test_chunked_bad_size_raises():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"zz\r\n")
    with pytest.raises(ValueError):
        dec.decode()


def test_chunked_extension_newline_rejected():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5;bad\nvalue\r\nhello\r\n0\r\n\r\n")
    with pytest.raises(ValueError):
        _pull(dec)


def test_close_delimited_reads_until_eof():
    dec = H1BodyDecoder("close")
    dec.feed(b"partial")
    body, complete = _pull(dec)
    assert body == b"partial" and not complete  # no EOF yet
    dec.mark_eof()
    body, complete = _pull(dec)
    assert body == b"" and complete
