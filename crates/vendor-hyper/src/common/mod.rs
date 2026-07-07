// Vendored hyper `src/common/` — only the pieces the server role needs. hyper's
// `common` also has buf/future/io/lock/task/time/watch (async orchestration
// glue), which httpunk reimplements in Python, so they are not vendored.
#[cfg(all(feature = "server", feature = "http1"))]
pub mod date;
