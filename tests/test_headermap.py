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


def test_equality_with_itself_and_across_maps_does_not_deadlock():
    """`HeaderMap.__eq__` must never hold both maps' locks at once: `m == m` on the
    non-reentrant Rust mutex used to self-deadlock, and `a == b` racing `b == a` on two
    threads could acquire the pair in opposite orders."""
    m = HeaderMap([("a", "1")])
    assert m == m
    assert m != HeaderMap([("a", "2")])
    assert HeaderMap([("a", "1")]) == m
