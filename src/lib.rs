// PyO3 FFI boundary — exposes Rust functions to the Python intelligence layer.
// See architecture.md §5 (IPC Contracts) and §4 (Execution Layer).

use pyo3::prelude::*;
use std::sync::Arc;

pub mod admin_api;
pub mod disruptor;
pub mod ipc;
pub mod llm_equity;
pub mod market_context;
pub mod oms;
pub mod risk;
pub mod runtime;
pub mod telemetry;

use ipc::arrow_ffi::{expose_order_book_arrow, ArrowOrderBookHandle, OrderBookMatrix};

/// Return the Rust execution layer version compiled into the extension.
#[pyfunction]
pub fn rust_engine_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyclass]
struct OrderBookSnapshotTestHandle {
    matrix: Arc<OrderBookMatrix>,
    handle: ArrowOrderBookHandle,
}

#[pymethods]
impl OrderBookSnapshotTestHandle {
    fn array_ptr(&self) -> usize {
        self.handle.array_ptr()
    }

    fn schema_ptr(&self) -> usize {
        self.handle.schema_ptr()
    }

    fn bids_ptr(&self) -> usize {
        self.matrix.bids_ptr() as usize
    }

    fn write_bid(&self, slot: usize, price: f64, qty: f64) {
        self.matrix.write_bid(slot, price, qty);
    }

    fn write_ask(&self, slot: usize, price: f64, qty: f64) {
        self.matrix.write_ask(slot, price, qty);
    }
}

/// Build a Rust-owned order-book snapshot handle for Python integration tests.
///
/// The normal pytest job does not build the PyO3 extension, so this is used by
/// the dedicated maturin import check when `TRAIDAN_EXPECT_RUST_EXTENSION=1`.
#[pyfunction]
fn _test_order_book_snapshot_handle() -> OrderBookSnapshotTestHandle {
    let matrix = Arc::new(OrderBookMatrix::new());
    matrix.write_bid(0, 50_000.0, 1.5);
    matrix.write_ask(0, 50_001.0, 0.75);
    let handle = expose_order_book_arrow(matrix.clone()).expect("Arrow FFI build failed");
    OrderBookSnapshotTestHandle { matrix, handle }
}

/// Python extension module exposed as `traidan._rust`.
#[pymodule]
fn _rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_engine_version, m)?)?;
    m.add_function(wrap_pyfunction!(_test_order_book_snapshot_handle, m)?)?;
    m.add_class::<OrderBookSnapshotTestHandle>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_stub_matches_package_version() {
        assert_eq!(rust_engine_version(), env!("CARGO_PKG_VERSION"));
    }
}
