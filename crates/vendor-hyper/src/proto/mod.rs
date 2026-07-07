//! Pieces pertaining to the HTTP message protocol.

cfg_feature! {
    #![feature = "http1"]

    pub mod h1;

    // SHIM: hyper re-exports `self::h1::Conn` and (client) `self::h1::dispatch`
    // here. Both live in unvendored modules (`conn`, `dispatch`), so these
    // re-exports are dropped. `ServerTransaction` is gated to the unbuilt
    // `server` feature and left in place (compiles out).
    #[cfg(feature = "server")]
    pub use self::h1::ServerTransaction;
}

// SHIM: hyper's HTTP/2 proto module (`#[cfg(feature="http2")] pub mod h2;`)
// is not vendored — httpunk uses the vendored `h2` *crate* for HTTP/2, not
// hyper's. Dropped (it was already compiled out with `http2` off; removing it
// also lets rustfmt resolve the module tree).

/// An Incoming Message head. Includes request/status line, and headers.
#[cfg(feature = "http1")]
#[derive(Debug, Default)]
pub struct MessageHead<S> {
    /// HTTP version of the message.
    pub version: http::Version,
    /// Subject (request line or status line) of Incoming message.
    pub subject: S,
    /// Headers of the Incoming message.
    pub headers: http::HeaderMap,
    /// Extensions.
    extensions: http::Extensions,
}

/// An incoming request message.
#[cfg(feature = "http1")]
pub type RequestHead = MessageHead<RequestLine>;

#[derive(Debug, Default, PartialEq)]
#[cfg(feature = "http1")]
pub struct RequestLine(pub http::Method, pub http::Uri);

/// An incoming response message.
#[cfg(all(feature = "http1", feature = "client"))]
pub type ResponseHead = MessageHead<http::StatusCode>;

#[derive(Debug)]
#[cfg(feature = "http1")]
pub enum BodyLength {
    /// `Content-Length`.
    Known(u64),
    /// `Transfer-Encoding: chunked` (if h1).
    Unknown,
}

// SHIM: hyper's `Dispatched` enum is dropped — its `Upgrade` variant
// references `crate::upgrade::Pending`, and neither `upgrade` nor the
// dispatcher that returns `Dispatched` is vendored.

#[cfg(all(feature = "client", feature = "http1"))]
impl MessageHead<http::StatusCode> {
    fn into_response<B>(self, body: B) -> http::Response<B> {
        let mut res = http::Response::new(body);
        *res.status_mut() = self.subject;
        *res.headers_mut() = self.headers;
        *res.version_mut() = self.version;
        *res.extensions_mut() = self.extensions;
        res
    }
}
