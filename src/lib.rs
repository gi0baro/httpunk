use pyo3::prelude::*;
use std::sync::OnceLock;

mod errors;
mod h1;
mod h2;
mod http;
mod proxy;

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

    errors::register(module)?;
    http::register(module)?;
    h2::register(module)?;
    h1::register(module)?;
    proxy::register(module)?;

    Ok(())
}
