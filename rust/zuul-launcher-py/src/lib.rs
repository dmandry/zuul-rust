// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// PyO3 test harness. zuul_launcher_py.Launcher is a stand-in for
// zuul.launcher.server.Launcher when ZUUL_LAUNCHER_IMPL=rust. The Python
// server module is not modified. Each _run calls the Rust launcher against
// a real TLS ZooKeeper session.

use pyo3::exceptions::{PyAttributeError, PyNotImplementedError, PyRuntimeError};
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

    /// Start Rust cleanup / stats / upload pools (called from bootstrap.start).
    #[pyo3(signature = (start_cleanup = true))]
    fn start_rust_workers(slf: &Bound<'_, Self>, start_cleanup: bool) -> PyResult<()> {
        let rust = slf.borrow().rust.clone();
        let mut guard = rust.lock().unwrap();
        if let Some(launcher) = guard.as_mut() {
            launcher.start_workers(start_cleanup);
        }
        Ok(())
    }

    /// Inject getProviderQuota payloads into the Rust cache (accept_one).
    fn seed_quota_cache(slf: &Bound<'_, Self>, payload: String) -> PyResult<()> {
        let rust = slf.borrow().rust.clone();
        if let Some(ptr) = zuul_launcher::cloud::active_launcher() {
            let launcher =
                unsafe { &mut *(ptr as *mut zuul_launcher::launcher::Launcher) };
            launcher.seed_quota_cache_from_json(&payload);
            return Ok(());
        }
        let mut guard = rust.lock().unwrap();
        if let Some(launcher) = guard.as_mut() {
            launcher.seed_quota_cache_from_json(&payload);
        }
        Ok(())
    }

    /// One CleanupWorker pass. cleanup_worker._run() calls this so tests
    /// that invoke the Python method still run the Rust matcher.
    fn run_cleanup_now(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let hostname: String = slf.getattr("hostname")?.extract()?;
        let rust = slf.borrow().rust.clone();
        py.allow_threads(|| {
            let _cloud = zuul_launcher::cloud::enter(&hostname);
            let leaks = {
                let guard = rust.lock().unwrap();
                guard.as_ref().map(|launcher| launcher.leaks.clone())
            };
            if let Some(leaks) = leaks {
                if let Ok(mut book) = leaks.lock() {
                    zuul_launcher::workers::cleanup_pass(&mut book);
                }
            }
        });
        Ok(())
    }

    /// Stop Rust cleanup / stats / upload pools (called from bootstrap.stop).
    ///
    /// Must release the GIL while joining: cleanup/upload ticks call back
    /// into Python via cloud::invoke. Holding the GIL here deadlocks those
    /// worker threads (and can wedge the launcher run loop).
    fn stop_rust_workers(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let rust = slf.borrow().rust.clone();
        py.allow_threads(|| {
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                launcher.stop_workers();
            }
        });
        Ok(())
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

    fn _close_rust_session(slf: &Bound<'_, Self>) {
        if let Ok(hostname) = slf.getattr("hostname").and_then(|h| h.extract::<String>()) {
            zuul_launcher::cloud::unregister_handler(&hostname);
        }
        if let Ok(mut guard) = slf.borrow().rust.lock() {
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
        // Feed layout/image/delete events into Rust maintain_images_and_uploads.
        let flags_json = call_py_value(py, "maintenance_flags", (slf,))?;
        let flags_text: String = flags_json.extract(py)?;
        let hostname: String = slf.getattr("hostname")?.extract()?;
        let rust = slf.borrow().rust.clone();
        let (events, stats) = py.allow_threads(|| {
            let _cloud = zuul_launcher::cloud::enter(&hostname);
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                if let Ok(flags) = zuul_launcher::jsonutil::loads(&flags_text) {
                    if flags
                        .get("layout_updated")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        launcher.layout_updated = true;
                    }
                    if flags
                        .get("image_updated")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        launcher.image_updated = true;
                    }
                    if flags
                        .get("upload_deleted")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        launcher.upload_deleted = true;
                    }
                }
                let _active = zuul_launcher::cloud::enter_launcher(
                    launcher as *mut _ as *mut (),
                );
                launcher.api.sync();
                launcher.process_once();
                (
                    launcher.take_pending_events_json(),
                    launcher.take_stats_json(),
                )
            } else {
                ("[]".to_string(), "[]".to_string())
            }
        });
        if events != "[]" {
            call_py(py, "emit_events", (slf, events))?;
        }
        if stats != "[]" {
            call_py(py, "emit_stats", (slf, stats))?;
        }
        // maintain_images_and_uploads runs inside process_once (no Python duplicate).
        // No ChildrenWatch in this loop, so poll again after MAX_SLEEP
        // instead of waiting out MIN_INTERVAL.
        if slf.getattr("_running")?.is_truthy()? {
            slf.getattr("wake_event")?.call_method0("set")?;
        }
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
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let hostname: String = slf.getattr("hostname")?.extract()?;
        let rust = slf.borrow().rust.clone();
        py.allow_threads(|| {
            let _cloud = zuul_launcher::cloud::enter(&hostname);
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                launcher.api.sync();
                launcher.check_missing_uploads();
            }
        });
        Ok(())
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn checkOldImages(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let hostname: String = slf.getattr("hostname")?.extract()?;
        let rust = slf.borrow().rust.clone();
        py.allow_threads(|| {
            let _cloud = zuul_launcher::cloud::enter(&hostname);
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                launcher.api.sync();
                launcher.check_old_images();
            }
        });
        Ok(())
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn _runStats(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        call_py(py, "call_launcher", (slf, "_runStats"))
    }

    #[pyo3(signature = (*args, **_kwargs))]
    fn _acceptRequest(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        // Tests patch this method and then call the saved original.
        // The original is this method: accept runs in Rust. When called
        // from accept_one during process_once, the mutex is already held
        // — use the thread-local active launcher pointer instead.
        let request = args.get_item(0)?;
        let uuid: String = request.getattr("uuid")?.extract()?;
        let hostname: String = slf.getattr("hostname")?.extract()?;
        let rust = slf.borrow().rust.clone();
        py.allow_threads(|| {
            let _cloud = zuul_launcher::cloud::enter(&hostname);
            if let Some(ptr) = zuul_launcher::cloud::active_launcher() {
                let launcher =
                    unsafe { &mut *(ptr as *mut zuul_launcher::launcher::Launcher) };
                launcher.api.sync();
                return launcher
                    .accept_request_by_uuid(&uuid)
                    .map_err(PyRuntimeError::new_err);
            }
            let mut guard = rust.lock().unwrap();
            if let Some(launcher) = guard.as_mut() {
                launcher.api.sync();
                launcher
                    .accept_request_by_uuid(&uuid)
                    .map_err(PyRuntimeError::new_err)?;
            }
            Ok(())
        })
    }

    #[pyo3(signature = (*args, **_kwargs))]
    fn getImageExternalId<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = args.get_item(0)?;
        let provider = args.get_item(1)?;
        let result = call_py_value(
            py,
            "call_launcher",
            (slf, "getImageExternalId", node, provider),
        )?;
        Ok(result.into_bound(py))
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn _getUnassignedNodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = call_py_value(py, "call_launcher", (slf, "_getUnassignedNodes"))?;
        Ok(result.into_bound(py))
    }

    #[pyo3(signature = (*_args, **_kwargs))]
    fn updateTenantProviders(
        &self,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        not_ported("updateTenantProviders")
    }

    /// Class attributes tests patch on `zuul.launcher.server.Launcher`
    /// (`tracer`, `DELETE_TIMEOUT`, `SUBNODE_SYNC_TIME`) live on that
    /// class. The PyO3 subclass does not inherit them. Forward data
    /// attributes so `self.tracer` and those patches resolve. Methods
    /// stay on the instance via `_bind_missing_methods`.
    fn __getattr__(&self, py: Python<'_>, name: PyObject) -> PyResult<PyObject> {
        let name_str: String = name.extract(py)?;
        if name_str.starts_with("__") {
            return Err(PyAttributeError::new_err(name_str));
        }
        let cls = py.import_bound("zuul.launcher.server")?.getattr("Launcher")?;
        let attr = cls.getattr(name_str.as_str()).map_err(|_| {
            PyAttributeError::new_err(name_str.clone())
        })?;
        if attr.is_callable() {
            return Err(PyAttributeError::new_err(name_str));
        }
        Ok(attr.into())
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
    install_cloud(slf);
    let _ = py;
    Ok(())
}

fn install_cloud(slf: &Bound<'_, PyLauncher>) {
    // In-process fakes (moto, responses, OpenStack/Kubernetes clients, and
    // patches of endpoint or state-machine methods) are only visible to
    // Python. Each launcher registers its own handler. _run selects the
    // one for this thread so a second launcher does not steal the first.
    let hostname: String = match slf.getattr("hostname").and_then(|h| h.extract()) {
        Ok(name) => name,
        Err(_) => return,
    };
    let handle = slf.clone().unbind();
    zuul_launcher::cloud::register_handler(&hostname, Arc::new(move |op, args| {
        Python::with_gil(|py| {
            let slf = handle.bind(py);
            let payload = zuul_launcher::jsonutil::dumps(args);
            let result = call_py_value(py, "cloud_call", (slf, op, payload))
                .map_err(|err| err.to_string())?;
            let text: String = result.extract(py).map_err(|err| err.to_string())?;
            zuul_launcher::jsonutil::loads(&text).map_err(|err| err.to_string())
        })
    }));
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
