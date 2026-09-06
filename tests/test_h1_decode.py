"""HTTP/1 body decoder — the Rust `H1BodyDecoder`, which drives the vendored
hyper `proto/h1/decode.rs` synchronously (content-length / chunked / close).

`decode()` returns a `bytes` chunk or `None`; `None` is "no chunk right now" —
end vs. need-more is told by `is_complete`."""

import pytest

from httpunk._httpunk import H1BodyDecoder
from httpunk.exceptions import H1BodyError


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


def test_length_complete_after_single_decode_of_last_bytes():
    # is_complete flips on the very decode() that consumes the last body byte —
    # mirroring hyper's poll_read_body checking decoder.is_eof() right after the data
    # frame, NOT one decode() later. A single-poll consumer (the server's cheap body
    # drain, `_drain_unread_body`) relies on this to reuse a fully-buffered length body.
    dec = H1BodyDecoder("length", 5)
    dec.feed(b"hello")
    assert dec.decode() == b"hello"
    assert dec.is_complete  # complete immediately, without a follow-up decode()


def test_chunked_data_frame_alone_is_not_complete():
    # One decode() of a chunked DATA frame does NOT complete the body — the
    # terminating zero-chunk is still unseen. So a single-poll drain of a chunked body
    # correctly can't cheaply drain it (it closes), matching hyper's one poll_read_body.
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5\r\nhello\r\n0\r\n\r\n")  # data chunk + terminator both buffered
    assert dec.decode() == b"hello"
    assert not dec.is_complete


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
    with pytest.raises(H1BodyError) as ei:
        dec.decode()
    assert ei.value.args[0] == "invalid_input"  # hyper: `Kind::Body` over an `InvalidInput` io error


def test_chunked_extension_newline_rejected():
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5;bad\nvalue\r\nhello\r\n0\r\n\r\n")
    with pytest.raises(H1BodyError) as ei:
        _pull(dec)
    assert ei.value.args[0] == "invalid_data"


def test_close_delimited_reads_until_eof():
    dec = H1BodyDecoder("close")
    dec.feed(b"partial")
    body, complete = _pull(dec)
    assert body == b"partial" and not complete  # no EOF yet
    dec.mark_eof()
    body, complete = _pull(dec)
    assert body == b"" and complete


# ----- hyper `Kind::Body` over an `UnexpectedEof` io error: a truncated body -----


def test_length_body_truncated_at_eof_is_body_error_unexpected_eof():
    """The transport closes before a Content-Length body is complete: hyper's decoder
    returns `io::Error(UnexpectedEof, IncompleteBody)` (decode.rs L159-165) and the
    dispatcher surfaces it as `Error::new_body` — the same `Kind::Body` as a framing
    error, told apart by the io kind of the cause. Mirrored as `H1BodyError` with
    `args[0] == "unexpected_eof"`."""
    dec = H1BodyDecoder("length", 10)
    dec.feed(b"hello")
    body, complete = _pull(dec)
    assert body == b"hello" and not complete
    dec.mark_eof()
    with pytest.raises(H1BodyError) as ei:
        dec.decode()
    assert ei.value.args[0] == "unexpected_eof"
    assert ei.value.args[1] == "error reading a body from connection: end of file before message length reached"


def test_chunked_body_truncated_at_eof_is_body_error_unexpected_eof():
    """A chunked body cut before its terminator: EOF inside a chunk (decode.rs L484-491,
    `IncompleteBody`) or inside the size line (L254, "unexpected EOF during chunk size
    line") are both `UnexpectedEof`."""
    dec = H1BodyDecoder("chunked")
    dec.feed(b"5\r\nhel")
    dec.mark_eof()
    with pytest.raises(H1BodyError) as ei:
        _pull(dec)
    assert ei.value.args[0] == "unexpected_eof"

    dec = H1BodyDecoder("chunked")
    dec.feed(b"5\r\nhello\r\n")  # a complete chunk, then EOF where the next size line should be
    dec.mark_eof()
    with pytest.raises(H1BodyError) as ei:
        _pull(dec)
    assert ei.value.args[0] == "unexpected_eof"


def test_body_error_is_httpunk_error_and_copies_with_io_kind():
    """`H1BodyError` sits under `H1Error` / `HTTPunkError`, and `fresh_exc`'s `copy.copy`
    (used to store/re-raise connection errors) keeps `io_kind`."""
    import copy

    from httpunk.exceptions import H1Error, HTTPunkError

    exc = H1BodyError("unexpected_eof", "msg")
    assert isinstance(exc, H1Error) and isinstance(exc, HTTPunkError)
    dup = copy.copy(exc)
    assert type(dup) is H1BodyError and dup.args == ("unexpected_eof", "msg")
