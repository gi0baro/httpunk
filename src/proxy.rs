//! PyO3 surface for the vendored hyper-util proxy matcher
//! (`vendor_hyper_util::client::proxy::matcher`). `client::proxy::matcher` is pure,
//! IO-free proxy *selection* logic, so — like the h1/h2 codecs — it is vendored
//! byte-for-byte and exposed here, rather than reimplemented in Python. Dialing the
//! chosen proxy stays a connector's job (Python, over the runtime).
//!
//! `Matcher`/`Intercept` are immutable after construction, so these pyclasses are
//! `frozen` with no `Mutex` (the vendored values are `Sync`).

use pyo3::prelude::*;

use vendor_hyper_util::client::proxy::matcher::{Intercept, Matcher};

/// Selects the proxy for a destination URL (≈ hyper-util `matcher::Matcher`).
#[pyclass(module = "httpunk._httpunk", name = "ProxyMatcher", frozen)]
pub struct ProxyMatcher {
    inner: Matcher,
}

#[pymethods]
impl ProxyMatcher {
    /// Build from the environment: `HTTP_PROXY`/`http_proxy`,
    /// `HTTPS_PROXY`/`https_proxy`, `ALL_PROXY`/`all_proxy`, `NO_PROXY`/`no_proxy`
    /// (uppercase preferred). httpoxy guard: a CGI context (`REQUEST_METHOD` set)
    /// disables all proxying.
    #[staticmethod]
    fn from_env() -> Self {
        ProxyMatcher {
            inner: Matcher::from_env(),
        }
    }

    /// Build explicitly from proxy-URL strings (≈ `matcher::Builder`): `all` is the
    /// fallback for both schemes; `no` is the NO_PROXY list. Empty/omitted → unset.
    #[staticmethod]
    #[pyo3(signature = (*, all=None, http=None, https=None, no=None))]
    fn from_parts(
        all: Option<String>,
        http: Option<String>,
        https: Option<String>,
        no: Option<String>,
    ) -> Self {
        let mut b = Matcher::builder();
        if let Some(v) = all {
            b = b.all(v);
        }
        if let Some(v) = http {
            b = b.http(v);
        }
        if let Some(v) = https {
            b = b.https(v);
        }
        if let Some(v) = no {
            b = b.no(v);
        }
        ProxyMatcher { inner: b.build() }
    }

    /// The proxy `ProxyIntercept` for destination `url`, or None (NO_PROXY bypass,
    /// non-http(s) scheme, no host, or an unparseable URL).
    fn intercept(&self, url: &str) -> Option<ProxyIntercept> {
        let uri: http::Uri = url.parse().ok()?;
        self.inner
            .intercept(&uri)
            .map(|inner| ProxyIntercept { inner })
    }
}

/// A selected proxy: its URL plus any auth parsed from the proxy URL's userinfo
/// (≈ hyper-util `matcher::Intercept`).
#[pyclass(module = "httpunk._httpunk", name = "ProxyIntercept", frozen)]
pub struct ProxyIntercept {
    inner: Intercept,
}

#[pymethods]
impl ProxyIntercept {
    /// The proxy URL (`scheme://host:port/`, userinfo stripped).
    #[getter]
    fn uri(&self) -> String {
        self.inner.uri().to_string()
    }

    /// The `Basic <base64>` `Proxy-Authorization` value for an http/https proxy,
    /// else None.
    fn basic_auth(&self) -> Option<String> {
        self.inner
            .basic_auth()
            .and_then(|hv| hv.to_str().ok().map(String::from))
    }

    /// The `(user, password)` for a SOCKS proxy, else None.
    fn raw_auth(&self) -> Option<(String, String)> {
        self.inner
            .raw_auth()
            .map(|(u, p)| (u.to_string(), p.to_string()))
    }

    fn __repr__(&self) -> String {
        // Never render credentials.
        format!("ProxyIntercept(uri={:?})", self.inner.uri().to_string())
    }
}

/// Register the proxy pyclasses on the extension module.
pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<ProxyMatcher>()?;
    m.add_class::<ProxyIntercept>()?;
    Ok(())
}
