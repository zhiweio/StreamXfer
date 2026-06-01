use pyo3::prelude::*;
use streamxfer_core::planner::catalog::StaticCatalog;
use streamxfer_core::{ExecutionEngine, ExportConfig};

#[pyfunction]
fn plan(config_json: &str) -> PyResult<String> {
    let config: ExportConfig = serde_json::from_str(config_json)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    runtime.block_on(async move {
        let engine = ExecutionEngine::new();
        let catalog = StaticCatalog::default();
        let tasks = engine
            .plan(&config, &catalog)
            .await
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        serde_json::to_string(&tasks)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))
    })
}

#[pyfunction]
fn export(config_json: &str) -> PyResult<String> {
    let config: ExportConfig = serde_json::from_str(config_json)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
    runtime.block_on(async move {
        let engine = ExecutionEngine::new();
        let catalog = StaticCatalog::default();
        let summary = engine
            .dry_run(&config, &catalog)
            .await
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(err.to_string()))?;
        serde_json::to_string(&summary)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))
    })
}

#[pymodule]
fn streamxfer_rust(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(plan, module)?)?;
    module.add_function(wrap_pyfunction!(export, module)?)?;
    Ok(())
}
