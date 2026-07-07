"""Reused `http`-crate value types, exposed via the Rust extension.

Mirrors the Rust `http` crate: `HeaderMap` today; `Uri` / `Method` / `StatusCode`
/ `Version` may follow when the connector / retries need them. These are
Rust-backed PyO3 wrappers over the `http` crate — not Python re-implementations.
"""

from ._httpunk import HeaderMap as HeaderMap
