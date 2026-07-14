"""HeaderMap surface tests (Rust `http` crate wrapper) — currently just `raw_items`,
the ASGI-shaped `(bytes, bytes)` view; the rest of the surface is exercised throughout
the h1/h2 suites."""

from httpunk import HeaderMap


def test_raw_items_matches_items_with_bytes_names():
    h = HeaderMap({"Content-Type": "text/plain"})
    h.add("set-cookie", "a=1")
    h.add("Set-Cookie", "b=2")  # names normalize to lowercase; duplicates kept in order

    raw = h.raw_items()
    assert raw == [(b"content-type", b"text/plain"), (b"set-cookie", b"a=1"), (b"set-cookie", b"b=2")]
    # exact parity with items(), modulo the name type
    assert raw == [(name.encode("latin-1"), value) for name, value in h.items()]
    assert all(isinstance(n, bytes) and isinstance(v, bytes) for n, v in raw)


def test_raw_items_empty():
    assert HeaderMap().raw_items() == []
