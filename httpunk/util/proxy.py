"""Proxy selection — `httpunk.util`'s analogue of hyper-util's
`client::proxy::matcher` (itself ported from reqwest).

This is pure, IO-free proxy *selection* logic, so — like the h1/h2 codecs, and
unlike `connect`/`auto`/graceful, which need the async runtime — it is **vendored
in Rust** (`crates/vendor-hyper-util`, byte-for-byte) and exposed via PyO3, not
reimplemented in Python. `Matcher`/`Intercept` are those PyO3 types.

- `Matcher.from_env()` — build from `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`NO_PROXY`
  (and lowercase variants); a CGI context (`REQUEST_METHOD` set) disables proxying.
- `Matcher.from_parts(*, all=, http=, https=, no=)` — build explicitly.
- `matcher.intercept(url)` — the `Intercept` (`.uri`, `.basic_auth()`, `.raw_auth()`)
  for a destination URL, or `None` (NO_PROXY bypass / non-http(s) / no proxy set).

Dialing the chosen proxy is a connector's job (a later phase), not this module's.
"""

from .._httpunk import ProxyIntercept as Intercept, ProxyMatcher as Matcher


__all__ = ["Intercept", "Matcher"]
