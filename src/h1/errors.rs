//! HTTP/1 error taxonomy — one Python exception class per public hyper `Error`
//! kind the h1 codec can produce (defined here like the H2 family in
//! `h2::errors`), and the mapping from a hyper `Error` to it.
//!
//! `args` carry the discriminant then hyper's `Display` text (the H2 family's
//! `(kind, message)` convention):
//!
//! - `Kind::Parse(p)` -> `H1ParseError(kind, message)`
//! - `Kind::Body` -> `H1BodyError(io_kind, message)`: the io kind of the cause, which
//!   is how a hyper user tells a truncated body (`unexpected_eof`) from a framing error
//! - `Kind::IncompleteMessage` -> `H1IncompleteMessageError(message)`
//! - `Kind::UnexpectedMessage` -> `H1UnexpectedMessageError(message)`
//! - `Kind::User(u)` -> `H1UserError(kind, message)`
//! - `Kind::Io` -> `TransportError` (protocol-neutral; h2's `Io` too)
//!
//! `H1BodyError` is a `BodyError` and `H1UserError` a `UserError` (their HTTP/2
//! counterparts sit beside them); `H1IncompleteMessageError` is both an
//! `HTTPConnectionError` and an `H1Error`.
//!
//! Anything else (`Canceled`, `ChannelClosed`, ...) is a hyper-internal plumbing
//! kind the sans-IO codec never yields; it falls back to the `H1Error` base.
//! Messages are hyper's `Display` text, with the cause appended (`{kind}: {cause}`)
//! the way `hyper::Error`'s `Debug`/`source()` chain reads.

use std::error::Error as _;

use pyo3::create_exception;
use pyo3::prelude::*;

use crate::errors::{
    BodyError, HTTPConnectionError, HTTPunkError, TransportError, UserError, create_exception_multi,
};
use vendor_hyper::{error_kind, error_parse_kind, error_user_kind};

create_exception!(
    _httpunk,
    H1Error,
    HTTPunkError,
    "Base class for the HTTP/1 errors HTTP/2 has no counterpart for — one subclass per \
     such hyper `Error` kind (parse / incomplete message / unexpected message). \
     Messages are hyper's `Display` text."
);
create_exception!(
    _httpunk,
    H1ParseError,
    H1Error,
    "A malformed HTTP/1 message head (hyper `Kind::Parse`). Raised to the client caller \
     for a malformed response; on the server hyper answers automatically (400 / 414 / \
     431, `Server::on_error`) and closes, so the app never sees it. args = (kind: str, \
     message: str); `kind` is the hyper `Parse` variant: `method`, `version`, \
     `version_h2` (an HTTP/2 preface on an HTTP/1 connection), `uri`, `uri_too_long`, \
     `header_token`, `header_content_length_invalid`, `header_transfer_encoding_invalid`, \
     `header_transfer_encoding_unexpected`, `too_large` (a head limit was reached: \
     `max_headers`, `max_header_size`, or `max_buf_size`; hyper role.rs:183-187, \
     io.rs:204-215), `status`, `internal`."
);
create_exception!(
    _httpunk,
    H1BodyError,
    BodyError,
    "An HTTP/1 body that could not be read (hyper `Kind::Body`): a framing error, the \
     transport closing before the body was complete, or the transport failing \
     mid-body. hyper reports all three as one kind whose cause is an `io::Error`; \
     args = (io_kind: str, message: str), and `io_kind` is that io kind, so a caller \
     tells them apart the way a hyper user does: `unexpected_eof` = truncated (the \
     peer hung up mid-body, \"end of file before message length reached\"); \
     `invalid_input` / `invalid_data` = a malformed chunk size line, extension, CRLF, \
     or trailer block; for a transport failure the kind Rust derives from its errno \
     (`connection_reset`, `timed_out`, ...), `other` without one."
);
create_exception_multi!(
    _httpunk,
    H1IncompleteMessageError,
    (HTTPConnectionError, H1Error),
    "The connection closed while a message was still expected (hyper \
     `Kind::IncompleteMessage`, `Error::is_incomplete_message`): the client got EOF \
     before the response head, or the server's client closed its side while the \
     response was still being written (`mid_message_detect_eof`), failing the in-flight \
     `respond()` / `send_data`. The HTTP state saying the message was cut short, not an \
     IO error on the transport (`TransportError`). args = (message: str,)."
);
create_exception!(
    _httpunk,
    H1UnexpectedMessageError,
    H1Error,
    "The peer sent bytes when none were expected (hyper `Kind::UnexpectedMessage`, \
     `require_empty_read`): a server wrote on an idle client connection — outside any \
     response — which poisons the connection; the next `request()` raises \
     `RequestNotSentError` from it. args = (message: str,)."
);
create_exception!(
    _httpunk,
    H1UserError,
    UserError,
    "Local misuse hyper detects on the wire path and reports as `Kind::User`, which \
     then closes the connection. args = (kind: str, message: str); `kind` is the hyper \
     `User` variant: `body_write_aborted` (a body ended short of its declared \
     Content-Length), `unexpected_header` (a response carrying both `content-length` \
     and `transfer-encoding`), `unsupported_status_code` (a server 1xx status other \
     than 101)."
);

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("H1Error", py.get_type::<H1Error>())?;
    m.add("H1ParseError", py.get_type::<H1ParseError>())?;
    m.add("H1BodyError", py.get_type::<H1BodyError>())?;
    m.add(
        "H1IncompleteMessageError",
        py.get_type::<H1IncompleteMessageError>(),
    )?;
    m.add(
        "H1UnexpectedMessageError",
        py.get_type::<H1UnexpectedMessageError>(),
    )?;
    m.add("H1UserError", py.get_type::<H1UserError>())?;
    m.add_function(wrap_pyfunction!(h1_io_kind, m)?)?;
    Ok(())
}

/// The `H1BodyError.io_kind` of a transport failure: the `std::io::ErrorKind` Rust
/// derives from `errno` (what upstream's `io::Error` would report), `other` without one.
#[pyfunction]
#[pyo3(signature = (errno=None))]
fn h1_io_kind(errno: Option<i32>) -> String {
    errno.map_or_else(
        || "other".to_string(),
        |n| io_kind_tag(std::io::Error::from_raw_os_error(n).kind()),
    )
}

/// hyper's `Display` plus the cause, if any: `"error reading a body from
/// connection: end of file before message length reached"`.
fn message(e: &vendor_hyper::Error) -> String {
    match e.source() {
        Some(cause) => format!("{e}: {cause}"),
        None => e.to_string(),
    }
}

/// `std::io::ErrorKind` as a stable snake_case tag (the `Debug` name, lowered).
fn io_kind_tag(kind: std::io::ErrorKind) -> String {
    let mut out = String::new();
    for (i, ch) in format!("{kind:?}").chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Map a hyper error to the httpunk exception mirroring its kind.
pub fn map_hyper_err(e: vendor_hyper::Error) -> PyErr {
    let msg = message(&e);
    match error_kind(&e) {
        "parse" => H1ParseError::new_err((error_parse_kind(&e).unwrap_or("internal"), msg)),
        "body" => {
            let io_kind = e
                .find_source::<std::io::Error>()
                .map_or_else(|| "other".to_string(), |io| io_kind_tag(io.kind()));
            H1BodyError::new_err((io_kind, msg))
        }
        "incomplete_message" => H1IncompleteMessageError::new_err((msg,)),
        "unexpected_message" => H1UnexpectedMessageError::new_err((msg,)),
        "user" => H1UserError::new_err((error_user_kind(&e).unwrap_or("other"), msg)),
        "io" | "shutdown" => TransportError::new_err((msg,)),
        _ => H1Error::new_err((msg,)),
    }
}
