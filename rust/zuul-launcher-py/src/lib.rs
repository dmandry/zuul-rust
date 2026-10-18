// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// PyO3 test harness. zuul_launcher_py.Launcher is a stand-in for
// zuul.launcher.server.Launcher when ZUUL_LAUNCHER_IMPL=rust. The Python
// server module is not modified. Each _run calls the Rust launcher against
// a real TLS ZooKeeper session.

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use std::sync::{Arc, Mutex};
use zuul_launcher::launcher::Launcher as RustLauncher;
use zuul_launcher::tlszk::TlsZk;
use zuul_launcher::zk::ZkOps;

const BOOTSTRAP: &str = include_str!("bootstrap.py");

#[pyclass(subclass, dict, module = "zuul_launcher_py", name = "Launcher")]
struct PyLauncher {
    rust: Arc<Mutex<Option<RustLauncher>>>,
}

#[allow(non_snake_case)]
#[pymethods]
impl PyLauncher {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        Self {
            rust: Arc::new(Mutex::new(None)),
        }
    }

    /// Accepts the same arguments TestLauncher passes to super().__init__.
    #[pyo3(signature = (config, connections, _hostname=None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        config: Bound<'_, PyAny>,
        connections: Bound<'_, PyAny>,
        _hostname: Option<String>,
    ) -> PyResult<()> {
        call_py(
            py,
            "bootstrap",
            (slf, &config, &connections, _hostname.clone()),
        )?;
        connect_rust(slf, py)?;
        Ok(())
    }

    #[classattr]
    const MAX_SLEEP: f64 = 1.0;
    #[classattr]
    const MIN_INTERVAL: f64 = 60.0;

    fn start(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        call_py(py, "start", (slf,))
    }

    fn stop(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        call_py(py, "stop", (slf,))
    }

    fn join(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        call_py(py, "join", (slf,))
    }

    fn run(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        call_py(py, "run", (slf,))
    }

    fn _close_rust_session(&self) {
        if let Ok(mut guard) = self.rust.lock() {
            *guard = None;
        }
    }

    /// One Rust launcher iteration. The test subclass holds `_test_lock`
    /// around this call; the GIL is released for the ZooKeeper work.
    /// Layout state is published before request processing so the
    /// component is running, matching Launcher._run.
    fn _run(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let rust = slf.borrow().rust.clone();
        let reloaded = py.allow_threads(|| {
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                launcher.api.sync();
                match launcher.reload_providers() {
                    Ok(_) => Ok(launcher.layout_snapshot_json()),
                    Err(err) => Err(err),
                }
            } else {
                Ok(String::new())
            }
        });
        match reloaded {
            Ok(snapshot) if !snapshot.is_empty() => {
                call_py(py, "publish_layout", (slf, snapshot))?;
            }
            Ok(_) => {}
            Err(err) => {
                let log = slf.getattr("log")?;
                log.call_method1(
                    "error",
                    (format!("Error in layout reload: {err}"),),
                )?;
                return Ok(());
            }
        }
        let rust = slf.borrow().rust.clone();
        py.allow_threads(|| {
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                launcher.api.sync();
                launcher.process_once();
            }
        });
        Ok(())
    }

    fn _getProvider<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        tenant_name: String,
        provider_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = call_py_value(py, "get_provider", (slf, tenant_name, provider_name))?;
        Ok(result.into_bound(py))
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn checkMissingUploads(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("checkMissingUploads")
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn checkOldImages(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("checkOldImages")
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn _runStats(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("_runStats")
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn _acceptRequest(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("_acceptRequest")
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn getImageExternalId(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("getImageExternalId")
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn _getUnassignedNodes(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("_getUnassignedNodes")
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn updateTenantProviders(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("updateTenantProviders")
    }
}

fn not_ported(name: &str) -> PyResult<()> {
    Err(PyNotImplementedError::new_err(format!(
        "rust launcher: {name} is not ported"
    )))
}

fn connect_rust(slf: &Bound<'_, PyLauncher>, py: Python<'_>) -> PyResult<()> {
    let hosts: String = slf.getattr("_zk_hosts")?.extract()?;
    let cert: String = slf.getattr("_zk_cert")?.extract()?;
    let key: String = slf.getattr("_zk_key")?.extract()?;
    let ca: String = slf.getattr("_zk_ca")?.extract()?;
    let timeout_ms: i32 = slf.getattr("_zk_timeout_ms")?.extract()?;
    let hostname: String = slf.getattr("hostname")?.extract()?;
    let filter: Option<Vec<String>> = slf.getattr("connection_filter")?.extract()?;
    let zk = TlsZk::connect(&hosts, &cert, &key, &ca, timeout_ms).map_err(|e| {
        PyRuntimeError::new_err(format!("rust launcher zooKeeper: {e}"))
    })?;
    let session = zk.session_id();
    let ops: Arc<dyn ZkOps> = zk;
    let mut launcher = RustLauncher::new(&hostname, ops);
    launcher.connection_filter = filter.clone();
    launcher.api.connection_filter = filter;
    let log = slf.getattr("log")?;
    log.call_method1(
        "info",
        (format!(
            "Rust launcher connected to ZooKeeper session {session:#x}"
        ),),
    )?;
    *slf.borrow().rust.lock().unwrap() = Some(launcher);
    let _ = py;
    Ok(())
}

fn call_py(py: Python<'_>, name: &str, args: impl IntoPy<Py<PyTuple>>) -> PyResult<()> {
    call_py_value(py, name, args)?;
    Ok(())
}

fn call_py_value(
    py: Python<'_>,
    name: &str,
    args: impl IntoPy<Py<PyTuple>>,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    // Same dict for globals and locals so imports are visible inside
    // the helper functions (Python binds a function's globals at def time).
    py.run_bound(BOOTSTRAP, Some(&dict), Some(&dict))?;
    let func = dict
        .get_item(name)?
        .ok_or_else(|| PyRuntimeError::new_err(format!("missing helper {name}")))?;
    Ok(func.call1(args)?.into())
}

#[pymodule]
fn zuul_launcher_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLauncher>()?;
    Ok(())
}
