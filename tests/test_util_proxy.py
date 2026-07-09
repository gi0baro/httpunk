"""`httpunk.util.proxy` — the vendored hyper-util proxy matcher exposed via PyO3.

The selection logic is byte-for-byte vendored Rust (with upstream's own tests), so
these check the Python binding + the behaviors a caller relies on: scheme routing,
ALL_PROXY fallback, NO_PROXY (domain-suffix / IP / CIDR / wildcard) bypass, auth
parsing, and env/CGI handling. No async runtime — pure logic, plain tests.
"""

from httpunk.util import proxy
from httpunk.util.proxy import Intercept, Matcher


def test_http_scheme_routes_to_http_proxy():
    m = Matcher.from_parts(http="http://proxy.local:8080")
    i = m.intercept("http://example.com/path")
    assert isinstance(i, Intercept)
    assert i.uri == "http://proxy.local:8080/"
    assert m.intercept("https://example.com/") is None  # https proxy unset


def test_all_proxy_is_fallback_for_both_schemes():
    m = Matcher.from_parts(all="http://all.local:3128")
    assert m.intercept("http://a.com/").uri == "http://all.local:3128/"
    assert m.intercept("https://a.com/").uri == "http://all.local:3128/"


def test_scheme_specific_overrides_all():
    m = Matcher.from_parts(all="http://all.local:3128", https="http://sec.local:8443")
    assert m.intercept("https://a.com/").uri == "http://sec.local:8443/"  # https wins over all
    assert m.intercept("http://a.com/").uri == "http://all.local:3128/"  # http falls back to all


def test_basic_auth_from_userinfo():
    m = Matcher.from_parts(http="http://user:pass@proxy.local:8080")
    i = m.intercept("http://a.com/")
    assert i.basic_auth() == "Basic dXNlcjpwYXNz"  # base64("user:pass")
    assert i.raw_auth() is None


def test_socks_proxy_uses_raw_auth_not_basic():
    m = Matcher.from_parts(all="socks5://u:p@socks.local:1080")
    i = m.intercept("https://a.com/")
    assert i.uri == "socks5://socks.local:1080/"
    assert i.raw_auth() == ("u", "p")
    assert i.basic_auth() is None


def test_no_proxy_domain_suffix_bypass():
    m = Matcher.from_parts(http="http://proxy.local:8080", no=".internal")
    assert m.intercept("http://api.internal/") is None  # suffix match -> bypass
    assert m.intercept("http://example.com/").uri == "http://proxy.local:8080/"


def test_no_proxy_exact_and_subdomain():
    m = Matcher.from_parts(http="http://proxy.local:8080", no="example.com")
    assert m.intercept("http://example.com/") is None  # exact
    assert m.intercept("http://www.example.com/") is None  # dot-anchored subdomain
    assert m.intercept("http://notexample.com/").uri == "http://proxy.local:8080/"  # not a suffix boundary


def test_no_proxy_cidr_and_ip():
    m = Matcher.from_parts(http="http://proxy.local:8080", no="10.0.0.0/8,192.168.1.5")
    assert m.intercept("http://10.1.2.3/") is None  # inside the CIDR
    assert m.intercept("http://192.168.1.5/") is None  # exact IP
    assert m.intercept("http://11.0.0.1/").uri == "http://proxy.local:8080/"  # outside


def test_no_proxy_wildcard_matches_everything():
    m = Matcher.from_parts(all="http://proxy.local:8080", no="*")
    assert m.intercept("http://anything.example/") is None
    assert m.intercept("https://other.test/") is None


def test_non_http_scheme_and_unparseable_return_none():
    m = Matcher.from_parts(all="http://proxy.local:8080")
    assert m.intercept("ftp://example.com/") is None  # only http/https destinations
    assert m.intercept("not a url") is None  # unparseable -> no proxy


def test_from_env_reads_proxy_vars(monkeypatch):
    monkeypatch.delenv("REQUEST_METHOD", raising=False)
    monkeypatch.setenv("HTTP_PROXY", "http://env.local:8080")
    monkeypatch.setenv("NO_PROXY", "skip.me")
    m = Matcher.from_env()
    assert m.intercept("http://example.com/").uri == "http://env.local:8080/"
    assert m.intercept("http://skip.me/") is None


def test_from_env_cgi_disables_proxying(monkeypatch):
    # httpoxy guard: in a CGI context HTTP_PROXY is attacker-controlled -> no proxy.
    monkeypatch.setenv("REQUEST_METHOD", "GET")
    monkeypatch.setenv("HTTP_PROXY", "http://attacker.local:8080")
    assert Matcher.from_env().intercept("http://example.com/") is None


def test_intercept_hides_credentials_in_repr():
    m = Matcher.from_parts(http="http://user:secret@proxy.local:8080")
    assert "secret" not in repr(m.intercept("http://a.com/"))


def test_module_reexports_rust_types():
    from httpunk._httpunk import ProxyIntercept, ProxyMatcher

    assert proxy.Matcher is ProxyMatcher
    assert proxy.Intercept is ProxyIntercept
