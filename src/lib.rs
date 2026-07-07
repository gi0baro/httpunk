use pyo3::prelude::*;
use std::sync::OnceLock;

// The vendored third-party code lives in separate workspace member crates —
// `vendor_h2` (hyperium/h2 frame+hpack) and `vendor_hyper` (hyper h1 role+encode)
// — so each owns its crate root (upstream `crate::` paths resolve with no
// rewrite → byte-identical) and is excluded from this crate's fmt/clippy. This
// crate is just the PyO3 adapter layer over them.
mod py;

pub fn get_lib_version() -> &'static str {
    static LIB_VERSION: OnceLock<String> = OnceLock::new();

    LIB_VERSION.get_or_init(|| {
        let version = env!("CARGO_PKG_VERSION");
        version.replace("-alpha", "a").replace("-beta", "b")
    })
}

#[pymodule(gil_used = false)]
fn _httpunk(_py: Python, module: &Bound<PyModule>) -> PyResult<()> {
    module.add("__version__", get_lib_version())?;

    py::register(module)?;

    Ok(())
}
