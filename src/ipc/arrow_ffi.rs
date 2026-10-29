// Apache Arrow C Data Interface — zero-copy shared memory transfer.
// Rust populates FFI_ArrowArray + FFI_ArrowSchema structs and exposes the
// 64-bit pointer to an in-process Python consumer via PyCapsule.
// See architecture.md section 5: Apache Arrow C Data Interface.
//
// Address-space contract:
//   The pointer is only meaningful to a Python consumer that shares this
//   process's address space, i.e. one
//   that loads `traidan._rust` as a PyO3 extension module and reads the
//   pointer in the same interpreter that received it. The actual byte-sharing
//   is intra-process.
//   Cross-process consumers are out of scope for this bridge and would need
//   `shm_open`/`mmap` shared memory instead.
//
// Pointer-trust contract:
//   `consume_order_book_arrow` on the Python side performs only a non-zero
//   check before handing the pointer to PyArrow's `_import_from_c`. A
//   stale or spoofed pointer can crash Python or release attacker-controlled
//   memory. The bridge therefore relies on the surrounding deployment
//   trust boundary: only the Rust executor that owns the matrix may publish
//   `(array_ptr, schema_ptr)` on the control channel. Hardening (capability
//   tokens, per-handle nonces, or a registry of live handles) is deferred
//   until that boundary is broken.
//
// FFI_ArrowArray release-callback lifecycle (issue #18 AC6):
//
//   The `arrow` crate's `to_ffi(&data)` initialises the `FFI_ArrowArray`'s
//   `release` callback to a function that, when invoked, drops the
//   `Arc<ArrayData>` carried inside `private_data`. Each child `ArrayData`
//   in turn owns the buffer-side `Arc` clones produced by
//   `Buffer::from_custom_allocation`. Together this chain is what keeps the
//   `Arc<OrderBookMatrix>` alive while the FFI buffers can still be read.
//
//   PyArrow's `pyarrow.Array._import_from_c(array_ptr, schema_ptr)` performs
//   a `memcpy` of both FFI structs into PyArrow-owned storage and **moves
//   the `release` callback** by setting the *source* struct's `release`
//   field to `NULL`. From that moment on:
//     • The Rust side's `Box<FFI_ArrowArray>` holds an empty struct whose
//       `Drop` is a no-op; dropping it does not free anything.
//     • The PyArrow array carries the live `release` callback. When the
//       PyArrow array is collected, `release` runs once, dropping the
//       `Arc<ArrayData>` and (transitively) every `Arc<OrderBookMatrix>`
//       clone the buffers held — including the publisher-side clone if the
//       publisher was already dropped.
//
//   This means **single-shot import**: importing the same
//   `(array_ptr, schema_ptr)` pair twice returns a PyArrow array with a
//   `NULL` release callback. The duplicate array still aliases the live
//   buffers but contributes no cleanup, so when *both* arrays drop the
//   buffers are freed exactly once (by the first arr's release). The
//   second array's PyArrow buffer references would dangle if the original
//   array dropped first — and that ordering is non-deterministic. The
//   matching subscriber (`OrderBookSnapshotSubscriber.receive_handshake`)
//   therefore refuses to consume the handshake more than once. The publisher
//   may re-send the same pointer payload for startup recovery, but duplicate
//   frames must not trigger duplicate imports.
//
//   The Rust publisher may safely outlive the Python import or be dropped
//   first; in either ordering the Arrow buffers' Arc count reaches zero
//   exactly once, which drops the `OrderBookMatrix` storage exactly once.

use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, FixedSizeListArray, Float64Array, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Fields};
use arrow::error::ArrowError;
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

/// Number of top-of-book levels exposed per side (matches equity depth top-20).
pub const DEPTH: usize = 20;

/// Width of each level in `f64` slots: `[price, qty]`.
pub const LEVEL_WIDTH: i32 = 2;

/// Shared L2 order book matrix used as the zero-copy backing store for the
/// Arrow C Data Interface bridge. Slots are stored as raw IEEE-754 bits in
/// atomics so Rust can update through a shared `Arc` without `UnsafeCell`
/// data races while the Arrow buffers alias the same bytes.
#[repr(C)]
pub struct OrderBookMatrix {
    bids: [[AtomicU64; 2]; DEPTH],
    asks: [[AtomicU64; 2]; DEPTH],
}

// NOTE (synchronisation): each f64 slot is written atomically, but there is no
// seqlock / double-buffer around the pair. Readers may still observe a price
// from one update and a quantity from another, which is the documented §5
// snapshot consistency tradeoff.
// `RefUnwindSafe` is required by `arrow_buffer::alloc::Allocation`. The
// matrix carries no panic-sensitive invariants: slots are independent atomics.
impl RefUnwindSafe for OrderBookMatrix {}

impl OrderBookMatrix {
    pub fn new() -> Self {
        Self {
            bids: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0.0_f64.to_bits()))
            }),
            asks: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0.0_f64.to_bits()))
            }),
        }
    }

    /// Overwrite the bid level at `idx`. Panics if `idx >= DEPTH`.
    pub fn write_bid(&self, idx: usize, price: f64, qty: f64) {
        assert!(
            idx < DEPTH,
            "bid level index {idx} out of bounds (DEPTH={DEPTH})"
        );
        self.bids[idx][0].store(price.to_bits(), Ordering::Release);
        self.bids[idx][1].store(qty.to_bits(), Ordering::Release);
    }

    /// Overwrite the ask level at `idx`. Panics if `idx >= DEPTH`.
    pub fn write_ask(&self, idx: usize, price: f64, qty: f64) {
        assert!(
            idx < DEPTH,
            "ask level index {idx} out of bounds (DEPTH={DEPTH})"
        );
        self.asks[idx][0].store(price.to_bits(), Ordering::Release);
        self.asks[idx][1].store(qty.to_bits(), Ordering::Release);
    }

    /// Raw pointer to the contiguous bid `f64` buffer (length `DEPTH * LEVEL_WIDTH`).
    pub fn bids_ptr(&self) -> *const f64 {
        self.bids.as_ptr() as *const f64
    }

    /// Raw pointer to the contiguous ask `f64` buffer (length `DEPTH * LEVEL_WIDTH`).
    pub fn asks_ptr(&self) -> *const f64 {
        self.asks.as_ptr() as *const f64
    }
}

impl Default for OrderBookMatrix {
    fn default() -> Self {
        Self::new()
    }
}

/// Owned handle wrapping the FFI_ArrowArray + FFI_ArrowSchema pair.
///
/// The `usize` pointers returned by `array_ptr()` and `schema_ptr()` are
/// stable for the lifetime of the handle; Python imports them via
/// `pyarrow.Array._import_from_c(array_ptr, schema_ptr)`.
///
/// The handle keeps an `Arc<OrderBookMatrix>` alive so the Arrow buffers
/// always alias valid memory; the buffer's custom-allocation owner also
/// holds an `Arc`, so the matrix survives until both the handle and any
/// imported PyArrow array have been dropped.
pub struct ArrowOrderBookHandle {
    matrix: Arc<OrderBookMatrix>,
    array: Box<FFI_ArrowArray>,
    schema: Box<FFI_ArrowSchema>,
}

impl ArrowOrderBookHandle {
    /// Address of the FFI_ArrowArray struct.
    pub fn array_ptr(&self) -> usize {
        &*self.array as *const FFI_ArrowArray as usize
    }

    /// Address of the FFI_ArrowSchema struct.
    pub fn schema_ptr(&self) -> usize {
        &*self.schema as *const FFI_ArrowSchema as usize
    }

    /// Reference to the underlying matrix; the executor uses this to push
    /// fresh top-of-book levels into the same memory the Arrow buffers alias.
    pub fn matrix(&self) -> &Arc<OrderBookMatrix> {
        &self.matrix
    }

    /// Consume the handle and yield the underlying FFI structs. Test-only:
    /// the production path keeps the handle alive and lets Python read
    /// through the pointers, so exposing this in the public API would let
    /// callers move out the FFI structs while another thread observes
    /// `array_ptr()`.
    #[cfg(test)]
    pub(crate) fn into_ffi(self) -> (FFI_ArrowArray, FFI_ArrowSchema, Arc<OrderBookMatrix>) {
        (*self.array, *self.schema, self.matrix)
    }
}

/// Build an Arrow `StructArray { bids, asks }` whose buffers alias the
/// matrix's `[[f64; 2]; DEPTH]` arrays directly — no copy, no allocation
/// of price/qty data.
pub fn expose_order_book_arrow(
    matrix: Arc<OrderBookMatrix>,
) -> Result<ArrowOrderBookHandle, ArrowError> {
    let bytes_per_side: usize = std::mem::size_of::<[[f64; 2]; DEPTH]>();
    let elements_per_side: usize = DEPTH * (LEVEL_WIDTH as usize);

    // SAFETY: the buffer's owner is an `Arc<OrderBookMatrix>`, so the atomic
    // slot arrays stay at stable addresses for as long as the buffers live.
    // The pointer is non-null because the matrix is initialised with concrete
    // storage. AtomicU64 has the same size/alignment as the f64 payload bits.
    let bids_buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new_unchecked(matrix.bids.as_ptr() as *mut u8),
            bytes_per_side,
            matrix.clone(),
        )
    };
    let asks_buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new_unchecked(matrix.asks.as_ptr() as *mut u8),
            bytes_per_side,
            matrix.clone(),
        )
    };

    let bids_inner_data = ArrayData::builder(DataType::Float64)
        .len(elements_per_side)
        .add_buffer(bids_buffer)
        .build()?;
    let asks_inner_data = ArrayData::builder(DataType::Float64)
        .len(elements_per_side)
        .add_buffer(asks_buffer)
        .build()?;

    let level_field = Arc::new(Field::new("item", DataType::Float64, false));
    let bids_inner: ArrayRef = Arc::new(Float64Array::from(bids_inner_data));
    let asks_inner: ArrayRef = Arc::new(Float64Array::from(asks_inner_data));

    let bids_array =
        FixedSizeListArray::try_new(level_field.clone(), LEVEL_WIDTH, bids_inner, None)?;
    let asks_array = FixedSizeListArray::try_new(level_field, LEVEL_WIDTH, asks_inner, None)?;

    let struct_fields = Fields::from(vec![
        Field::new("bids", bids_array.data_type().clone(), false),
        Field::new("asks", asks_array.data_type().clone(), false),
    ]);
    let struct_array = StructArray::try_new(
        struct_fields,
        vec![Arc::new(bids_array), Arc::new(asks_array)],
        None,
    )?;

    let (array, schema) = to_ffi(&struct_array.to_data())?;

    Ok(ArrowOrderBookHandle {
        matrix,
        array: Box::new(array),
        schema: Box::new(schema),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::ffi::from_ffi;

    #[test]
    fn matrix_default_is_zeros() {
        let m = OrderBookMatrix::new();
        unsafe {
            for i in 0..DEPTH {
                assert_eq!(*m.bids_ptr().add(2 * i), 0.0_f64);
                assert_eq!(*m.bids_ptr().add(2 * i + 1), 0.0_f64);
                assert_eq!(*m.asks_ptr().add(2 * i), 0.0_f64);
                assert_eq!(*m.asks_ptr().add(2 * i + 1), 0.0_f64);
            }
        }
    }

    #[test]
    fn matrix_write_round_trips() {
        let m = OrderBookMatrix::new();
        m.write_bid(0, 50_000.0, 1.5);
        m.write_ask(DEPTH - 1, 50_001.0, 0.25);
        unsafe {
            assert_eq!(*m.bids_ptr(), 50_000.0);
            assert_eq!(*m.bids_ptr().add(1), 1.5);
            assert_eq!(*m.asks_ptr().add(2 * (DEPTH - 1)), 50_001.0);
            assert_eq!(*m.asks_ptr().add(2 * (DEPTH - 1) + 1), 0.25);
        }
    }

    #[test]
    #[should_panic(expected = "bid level index 20 out of bounds")]
    fn matrix_write_bid_out_of_bounds_panics() {
        OrderBookMatrix::new().write_bid(DEPTH, 0.0, 0.0);
    }

    #[test]
    #[should_panic(expected = "ask level index 20 out of bounds")]
    fn matrix_write_ask_out_of_bounds_panics() {
        OrderBookMatrix::new().write_ask(DEPTH, 0.0, 0.0);
    }

    #[test]
    fn handle_exposes_non_zero_pointers_and_keeps_matrix_alive() {
        let matrix = Arc::new(OrderBookMatrix::new());
        let strong_before = Arc::strong_count(&matrix);
        let handle = expose_order_book_arrow(matrix.clone()).unwrap();
        assert_ne!(handle.array_ptr(), 0);
        assert_ne!(handle.schema_ptr(), 0);
        // The handle holds one Arc; each Arrow buffer also holds one Arc as
        // its custom-allocation owner — so strong count grows by at least 3.
        assert!(Arc::strong_count(&matrix) >= strong_before + 3);
    }

    #[test]
    fn round_trip_through_ffi_yields_struct_with_two_fixed_size_lists() {
        let matrix = Arc::new(OrderBookMatrix::new());
        matrix.write_bid(0, 50_000.0, 1.5);
        matrix.write_ask(0, 50_001.0, 0.75);

        let (array_ffi, schema_ffi, _matrix) = expose_order_book_arrow(matrix).unwrap().into_ffi();

        // SAFETY: the FFI structs were just produced by `to_ffi` in this
        // process, so they are well-formed.
        let data = unsafe { from_ffi(array_ffi, &schema_ffi) }.unwrap();
        assert_eq!(data.len(), DEPTH);

        let struct_array = StructArray::from(data);
        assert_eq!(struct_array.num_columns(), 2);

        let bids = struct_array
            .column_by_name("bids")
            .expect("bids field must be present")
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("bids must be a FixedSizeListArray");
        assert_eq!(bids.value_length(), LEVEL_WIDTH);
        assert_eq!(bids.len(), DEPTH);
        let bids_values = bids
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("bids values must be Float64Array");
        assert_eq!(bids_values.value(0), 50_000.0);
        assert_eq!(bids_values.value(1), 1.5);

        let asks = struct_array
            .column_by_name("asks")
            .expect("asks field must be present")
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("asks must be a FixedSizeListArray");
        let asks_values = asks
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("asks values must be Float64Array");
        assert_eq!(asks_values.value(0), 50_001.0);
        assert_eq!(asks_values.value(1), 0.75);
    }

    #[test]
    fn buffer_aliases_matrix_memory_zero_copy() {
        let matrix = Arc::new(OrderBookMatrix::new());
        let bids_addr = matrix.bids_ptr() as usize;
        let asks_addr = matrix.asks_ptr() as usize;

        let (array_ffi, schema_ffi, _matrix) = expose_order_book_arrow(matrix).unwrap().into_ffi();
        // SAFETY: in-process round-trip of FFI structs we just produced.
        let data = unsafe { from_ffi(array_ffi, &schema_ffi) }.unwrap();

        // StructArray child[0] = bids FixedSizeList; its child[0] = inner Float64.
        let bids_inner = &data.child_data()[0].child_data()[0];
        let asks_inner = &data.child_data()[1].child_data()[0];

        assert_eq!(bids_inner.buffers()[0].as_ptr() as usize, bids_addr);
        assert_eq!(asks_inner.buffers()[0].as_ptr() as usize, asks_addr);
    }

    #[test]
    fn release_callback_drops_owner_arc_after_handle_drop() {
        use std::sync::Weak;

        let matrix = Arc::new(OrderBookMatrix::new());
        let weak: Weak<OrderBookMatrix> = Arc::downgrade(&matrix);
        // Move the only strong reference into expose; after the call, all
        // strong refs are owned by the handle (one) and the two buffer
        // custom-allocation owners.
        let handle = expose_order_book_arrow(matrix).unwrap();
        assert!(weak.upgrade().is_some(), "handle keeps matrix alive");

        drop(handle);

        assert!(
            weak.upgrade().is_none(),
            "release callback must drop the buffer-owner Arcs so the matrix is freed",
        );
    }

    #[test]
    fn mutation_after_expose_is_visible_through_buffer() {
        let matrix = Arc::new(OrderBookMatrix::new());
        let bids_addr = matrix.bids_ptr() as usize;

        let handle = expose_order_book_arrow(matrix.clone()).unwrap();

        // SAFETY: we just allocated the matrix; reading/writing through the
        // raw pointer is sound for this single-threaded test.
        unsafe {
            assert_eq!(*(bids_addr as *const f64), 0.0_f64);
        }

        matrix.write_bid(0, 50_000.0, 1.5);

        unsafe {
            let price = *(bids_addr as *const f64);
            let qty = *((bids_addr + std::mem::size_of::<f64>()) as *const f64);
            assert_eq!(price, 50_000.0);
            assert_eq!(qty, 1.5);
        }

        // Drop the handle last so the buffer outlives the read.
        drop(handle);
    }
}
