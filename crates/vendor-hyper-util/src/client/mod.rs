//! Hand-written module glue (not from upstream): exposes only `proxy`, dropping
//! hyper-util's unvendored `client` submodules (legacy, pool, connect, …).

pub mod proxy;
