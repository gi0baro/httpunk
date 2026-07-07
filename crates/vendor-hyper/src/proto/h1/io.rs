//! Minimal extract of hyper's `proto/h1/io.rs`: only the `MemRead` trait that the
//! vendored `decode.rs` reads through. The rest of hyper's io.rs is the async
//! buffered-transport layer, which is not vendored (that is the Python driver's
//! job). httpunk supplies its own synchronous `MemRead` in the bridge facade.

use std::io;
use std::task::{Context, Poll};

use bytes::Bytes;

pub trait MemRead {
    fn read_mem(&mut self, cx: &mut Context<'_>, len: usize) -> Poll<io::Result<Bytes>>;
}
