"""Phase 0 proof: the vendored h2 frame+hpack core parses real HTTP/2 wire
bytes into Python frame objects, with zero I/O.

The HEADERS block below is a hand-encoded, spec-valid HPACK header block:
  - 0x88            -> static table index 8  => ":status: 200"
  - 0x40 ...        -> literal w/ incremental indexing, new name "x-test": "hi"
"""

from httpunk import _httpunk
from httpunk.http import HeaderMap


# A SETTINGS frame (stream 0) advertising MAX_CONCURRENT_STREAMS = 100.
SETTINGS = (
    b"\x00\x00\x06"  # length = 6
    b"\x04"  # type = SETTINGS
    b"\x00"  # flags = 0
    b"\x00\x00\x00\x00"  # stream id = 0
    b"\x00\x03\x00\x00\x00\x64"  # SETTINGS_MAX_CONCURRENT_STREAMS = 100
)

# A HEADERS frame (stream 1), END_HEADERS | END_STREAM, HPACK-encoded.
HEADERS = (
    b"\x00\x00\x0c"  # length = 12
    b"\x01"  # type = HEADERS
    b"\x05"  # flags = END_HEADERS | END_STREAM
    b"\x00\x00\x00\x01"  # stream id = 1
    b"\x88"  # indexed field 8 -> :status: 200
    b"\x40\x06x-test\x02hi"  # literal new name "x-test": "hi"
)


def test_parses_settings_then_headers():
    codec = _httpunk.H2Codec("client")
    frames = codec.receive(SETTINGS + HEADERS)

    assert len(frames) == 2
    settings, headers = frames

    assert isinstance(settings, _httpunk.H2FrameSettings)
    assert settings.ack is False
    assert settings.max_concurrent_streams == 100

    assert isinstance(headers, _httpunk.H2FrameHeaders)
    assert headers.stream_id == 1
    assert headers.status == 200
    assert headers.end_stream is True
    assert headers.end_headers is True
    # Pseudo-header (:status) is broken out; regular fields come through as
    # (name: str, value: bytes) tuples.
    assert headers.headers.items() == [("x-test", b"hi")]


def test_buffers_partial_frames_across_calls():
    codec = _httpunk.H2Codec()  # defaults to role="client"
    stream = SETTINGS + HEADERS

    # Feed a partial first chunk (mid-SETTINGS): nothing decodable yet.
    assert codec.receive(stream[:5]) == []
    assert codec.buffered() == 5

    # Feed the rest: both frames now complete.
    frames = codec.receive(stream[5:])
    assert len(frames) == 2
    assert codec.buffered() == 0
    assert isinstance(frames[0], _httpunk.H2FrameSettings)
    assert isinstance(frames[1], _httpunk.H2FrameHeaders)


def test_serialize_request_roundtrips_through_server_codec():
    """Client serializes a request; a server-role codec decodes it back —
    proving the encode side (HPACK-encode included) against our own decoder."""
    client = _httpunk.H2Codec("client")
    server = _httpunk.H2Codec("server")

    wire = b""
    wire += client.serialize_settings(max_concurrent_streams=100, initial_window_size=65535)
    wire += client.serialize_request_headers(
        1, "GET", "http://localhost/foo?x=1", headers=HeaderMap([("user-agent", b"httpunk")])
    )
    wire += client.serialize_data(1, b"", end_stream=True)

    frames = server.receive(wire)
    assert len(frames) == 3
    settings, headers, data = frames

    assert isinstance(settings, _httpunk.H2FrameSettings)
    assert settings.max_concurrent_streams == 100
    assert settings.initial_window_size == 65535

    assert isinstance(headers, _httpunk.H2FrameHeaders)
    assert headers.stream_id == 1
    assert headers.method == "GET"
    assert headers.scheme == "http"
    assert headers.authority == "localhost"
    assert headers.path == "/foo?x=1"
    assert headers.end_stream is False
    assert ("user-agent", b"httpunk") in headers.headers.items()

    assert isinstance(data, _httpunk.H2FrameData)
    assert data.stream_id == 1
    assert data.data == b""
    assert data.end_stream is True


def test_hpack_dynamic_table_persists_across_frames():
    """The literal 'x-test' was inserted into the dynamic table by the first
    HEADERS frame; a second HEADERS frame can reference it by index, proving
    the HPACK decoder state lives in the codec and survives across frames."""
    codec = _httpunk.H2Codec("client")
    first = codec.receive(HEADERS)
    assert first[0].headers.items() == [("x-test", b"hi")]

    # Dynamic table entries start at index 62; the just-added "x-test: hi" is 62.
    # 0xbe = 0x80 | 62 -> indexed field referencing the dynamic entry.
    second_headers = (
        b"\x00\x00\x02"  # length = 2
        b"\x01"  # type = HEADERS
        b"\x05"  # END_HEADERS | END_STREAM
        b"\x00\x00\x00\x03"  # stream id = 3
        b"\x88"  # :status: 200
        b"\xbe"  # dynamic index 62 -> x-test: hi
    )
    frames = codec.receive(second_headers)
    assert len(frames) == 1
    assert frames[0].stream_id == 3
    assert frames[0].status == 200
    assert frames[0].headers.items() == [("x-test", b"hi")]


def _frame(ftype, flags, stream_id, payload):
    return len(payload).to_bytes(3, "big") + bytes([ftype, flags]) + stream_id.to_bytes(4, "big") + payload


def test_continuation_reassembly():
    """A header block split across HEADERS (no END_HEADERS) + CONTINUATION
    (END_HEADERS) reassembles into a single Headers event."""
    client = _httpunk.H2Codec("client")
    server = _httpunk.H2Codec("server")

    wire = client.serialize_request_headers(
        1, "GET", "http://h/x", headers=HeaderMap([("a", b"1"), ("bb", b"22"), ("ccc", b"333")])
    )
    block = wire[9:]  # drop the 9-byte HEADERS frame header, keep the HPACK block
    mid = len(block) // 2
    h1 = _frame(0x01, 0x00, 1, block[:mid])  # HEADERS, END_HEADERS clear
    cont = _frame(0x09, 0x04, 1, block[mid:])  # CONTINUATION, END_HEADERS set

    assert server.receive(h1) == []  # buffered, no event yet
    frames = server.receive(cont)
    assert len(frames) == 1
    hdrs = frames[0]
    assert isinstance(hdrs, _httpunk.H2FrameHeaders)
    assert hdrs.stream_id == 1
    assert hdrs.method == "GET"
    assert hdrs.path == "/x"
    assert ("a", b"1") in hdrs.headers.items()
    assert ("bb", b"22") in hdrs.headers.items()
    assert ("ccc", b"333") in hdrs.headers.items()


def test_continuation_flood_is_capped():
    """An unbounded stream of CONTINUATION frames (never END_HEADERS) is rejected
    (ENHANCE_YOUR_CALM) — the CONTINUATION-flood DoS guard."""
    import pytest

    server = _httpunk.H2Codec("server")
    assert server.receive(_frame(0x01, 0x00, 1, b"\x88")) == []  # HEADERS, no END_HEADERS
    with pytest.raises(_httpunk.H2ProtocolError):
        for _ in range(5000):
            server.receive(_frame(0x09, 0x00, 1, b"\x00"))  # CONTINUATION, no END_HEADERS


def _frame_types(wire):
    types, i = [], 0
    while i < len(wire):
        length = int.from_bytes(wire[i : i + 3], "big")
        types.append(wire[i + 3])
        i += 9 + length
    return types


def test_large_headers_split_across_continuation():
    """A header block exceeding one frame is sent as HEADERS + CONTINUATION(s)
    on the client, and reassembled on the server — full round-trip."""
    client = _httpunk.H2Codec("client")
    server = _httpunk.H2Codec("server")
    big = bytes(65 + (i % 26) for i in range(40000))  # ~40 KB, HPACK block > one frame

    wire = client.serialize_request_headers(1, "GET", "http://h/x", headers=HeaderMap([("big", big)]))
    types = _frame_types(wire)
    assert types[0] == 0x01  # HEADERS
    assert 0x09 in types  # at least one CONTINUATION

    frames = server.receive(wire)
    assert len(frames) == 1
    assert isinstance(frames[0], _httpunk.H2FrameHeaders)
    assert frames[0].stream_id == 1
    assert ("big", big) in frames[0].headers.items()
