//! PyO3 wrappers that reuse the `http` crate's value types, exposed under the
//! Python `httpunk.http` module. `HeaderMap` today; `Uri`/`Method`/`StatusCode`/
//! `Version` may follow when the connector/retries need them.
//!
//! Like the rest of the extension, `HeaderMap` is `frozen` with a
//! `std::sync::Mutex` guarding the inner `http::HeaderMap`, so instances are
//! `Sync` and safe to share across the runtime's worker threads. (Locks are
//! `.unwrap()`ed: release builds are `panic = "abort"`, so a poisoned lock can
//! never be observed.)

use http::HeaderMap as HttpHeaderMap;
use http::header::{HeaderName, HeaderValue};
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use std::sync::Mutex;

fn value_err<E: std::fmt::Display>(what: &str, e: E) -> PyErr {
    PyValueError::new_err(format!("{what}: {e}"))
}

/// `http::HeaderMap` caps at `MAX_SIZE` (32768) distinct entries; the panicking
/// `insert`/`append` would abort the process under `panic = "abort"`, so we use
/// the fallible `try_*` variants and surface `MaxSizeReached` as a Python error.
fn max_size_err() -> PyErr {
    PyValueError::new_err("header map is full (http::HeaderMap MAX_SIZE, 32768 entries)")
}

/// Parse a Python `str`/`bytes` into an `http::HeaderName`. `HeaderName` lower-
/// cases the name (via the crate's char table), matching HTTP/2's requirement
/// that field names be lowercase on the wire.
fn parse_name(name: &Bound<'_, PyAny>) -> PyResult<HeaderName> {
    if let Ok(s) = name.cast::<PyString>() {
        HeaderName::from_bytes(s.to_str()?.as_bytes())
            .map_err(|e| value_err("invalid header name", e))
    } else if let Ok(b) = name.cast::<PyBytes>() {
        HeaderName::from_bytes(b.as_bytes()).map_err(|e| value_err("invalid header name", e))
    } else {
        Err(PyTypeError::new_err("header name must be str or bytes"))
    }
}

/// Parse a Python `str`/`bytes` into an `http::HeaderValue` (validated).
fn parse_value(value: &Bound<'_, PyAny>) -> PyResult<HeaderValue> {
    if let Ok(s) = value.cast::<PyString>() {
        HeaderValue::from_str(s.to_str()?).map_err(|e| value_err("invalid header value", e))
    } else if let Ok(b) = value.cast::<PyBytes>() {
        HeaderValue::from_bytes(b.as_bytes()).map_err(|e| value_err("invalid header value", e))
    } else {
        Err(PyTypeError::new_err("header value must be str or bytes"))
    }
}

/// An ordered, case-insensitive, multi-valued header collection — the `http`
/// crate's `HeaderMap`, reused directly. Names normalize to lowercase `str`;
/// values are `bytes` (`HeaderValue`). `str`/`bytes` are accepted on input and
/// validated by the `http` crate.
#[pyclass(module = "httpunk._httpunk", name = "HeaderMap", frozen)]
pub struct HeaderMap {
    inner: Mutex<HttpHeaderMap>,
}

impl HeaderMap {
    /// Wrap an existing `http::HeaderMap` (used by the codec's decode path — a
    /// decoded HEADERS frame already owns one, so this is zero-copy).
    pub(crate) fn from_inner(map: HttpHeaderMap) -> Self {
        HeaderMap {
            inner: Mutex::new(map),
        }
    }

    /// Clone out the inner map (used by the codec's serialize path).
    pub(crate) fn snapshot(&self) -> HttpHeaderMap {
        self.inner.lock().unwrap().clone()
    }

    /// Number of values (crate-visible; `__len__` itself is private to pymethods).
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn items_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let guard = self.inner.lock().unwrap();
        let list = PyList::empty(py);
        for (name, value) in guard.iter() {
            list.append((name.as_str(), PyBytes::new(py, value.as_bytes())))?;
        }
        Ok(list)
    }
}

#[pymethods]
impl HeaderMap {
    #[new]
    #[pyo3(signature = (init=None))]
    fn new(init: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut map = HttpHeaderMap::new();
        if let Some(obj) = init {
            if let Ok(other) = obj.cast::<HeaderMap>() {
                map = other.get().snapshot();
            } else if let Ok(dict) = obj.cast::<PyDict>() {
                for (name, value) in dict.iter() {
                    map.try_append(parse_name(&name)?, parse_value(&value)?)
                        .map_err(|_| max_size_err())?;
                }
            } else {
                for item in obj.try_iter()? {
                    let item = item?;
                    let name = item.get_item(0)?;
                    let value = item.get_item(1)?;
                    map.try_append(parse_name(&name)?, parse_value(&value)?)
                        .map_err(|_| max_size_err())?;
                }
            }
        }
        Ok(HeaderMap {
            inner: Mutex::new(map),
        })
    }

    /// First value for `name`, or raise `KeyError`.
    fn __getitem__(&self, py: Python<'_>, name: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
        let name = parse_name(name)?;
        let guard = self.inner.lock().unwrap();
        match guard.get(&name) {
            Some(v) => Ok(PyBytes::new(py, v.as_bytes()).unbind()),
            None => Err(PyKeyError::new_err(name.as_str().to_string())),
        }
    }

    /// First value for `name`, or `default`.
    #[pyo3(signature = (name, default=None))]
    fn get(
        &self,
        py: Python<'_>,
        name: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let name = parse_name(name)?;
        let guard = self.inner.lock().unwrap();
        match guard.get(&name) {
            Some(v) => Ok(PyBytes::new(py, v.as_bytes()).into_any().unbind()),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    /// All values for `name`, in order.
    fn get_all(&self, py: Python<'_>, name: &Bound<'_, PyAny>) -> PyResult<Py<PyList>> {
        let name = parse_name(name)?;
        let guard = self.inner.lock().unwrap();
        let list = PyList::empty(py);
        for v in guard.get_all(&name) {
            list.append(PyBytes::new(py, v.as_bytes()))?;
        }
        Ok(list.unbind())
    }

    /// Append a value for `name`, keeping any existing ones (multi-valued).
    fn add(&self, name: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let name = parse_name(name)?;
        let value = parse_value(value)?;
        self.inner
            .lock()
            .unwrap()
            .try_append(name, value)
            .map_err(|_| max_size_err())?;
        Ok(())
    }

    /// Set `name` to a single value, dropping any existing values for it.
    fn __setitem__(&self, name: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let name = parse_name(name)?;
        let value = parse_value(value)?;
        self.inner
            .lock()
            .unwrap()
            .try_insert(name, value)
            .map_err(|_| max_size_err())?;
        Ok(())
    }

    fn __delitem__(&self, name: &Bound<'_, PyAny>) -> PyResult<()> {
        let name = parse_name(name)?;
        let mut guard = self.inner.lock().unwrap();
        if guard.remove(&name).is_none() {
            return Err(PyKeyError::new_err(name.as_str().to_string()));
        }
        Ok(())
    }

    #[pyo3(signature = (name, value))]
    fn setdefault(
        &self,
        py: Python<'_>,
        name: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyBytes>> {
        let name = parse_name(name)?;
        let mut guard = self.inner.lock().unwrap();
        if let Some(v) = guard.get(&name) {
            return Ok(PyBytes::new(py, v.as_bytes()).unbind());
        }
        let value = parse_value(value)?;
        let out = PyBytes::new(py, value.as_bytes()).unbind();
        guard.try_append(name, value).map_err(|_| max_size_err())?;
        Ok(out)
    }

    fn __contains__(&self, name: &Bound<'_, PyAny>) -> bool {
        match parse_name(name) {
            Ok(name) => self.inner.lock().unwrap().contains_key(&name),
            Err(_) => false,
        }
    }

    /// Distinct names, in the map's iteration order.
    fn keys(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .keys()
            .map(|k| k.as_str().to_string())
            .collect()
    }

    /// Every value, in order (duplicates included).
    fn values(&self, py: Python<'_>) -> Vec<Py<PyBytes>> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(_, v)| PyBytes::new(py, v.as_bytes()).unbind())
            .collect()
    }

    /// Every `(name, value)` pair, in order (duplicates included).
    fn items(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        Ok(self.items_list(py)?.unbind())
    }

    /// Every `(name, value)` pair with the name as raw `bytes` (already lowercase
    /// ASCII in the `http` crate), in order, duplicates included — the exact shape
    /// ASGI servers want for a scope's `headers`, in one boundary crossing and with
    /// no per-name re-encoding.
    fn raw_items(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let guard = self.inner.lock().unwrap();
        let list = PyList::empty(py);
        for (name, value) in guard.iter() {
            list.append((
                PyBytes::new(py, name.as_str().as_bytes()),
                PyBytes::new(py, value.as_bytes()),
            ))?;
        }
        Ok(list.unbind())
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.keys())?;
        Ok(list.try_iter()?.into_any().unbind())
    }

    /// Number of values (like the `http` crate's `HeaderMap::len`).
    fn __len__(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.cast::<HeaderMap>() {
            Ok(other) => *self.inner.lock().unwrap() == other.get().snapshot(),
            Err(_) => false,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "HeaderMap({})",
            self.items_list(py)?.repr()?.to_str()?
        ))
    }
}

/// Register the `http`-crate wrappers on the extension module.
pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<HeaderMap>()?;
    Ok(())
}
