// Cache-line alignment structs.
// Pads data to 64 bytes to prevent false sharing between independent threads
// writing to adjacent memory addresses in the same CPU cache line.
// See architecture.md §4 — LMAX Disruptor (mechanical sympathy).

#[repr(align(64))]
pub struct CacheLinePadded<T> {
    pub inner: T,
    _pad: [u8; 0], // zero-size — compiler inserts trailing pad to reach 64-byte boundary
}

impl<T> CacheLinePadded<T> {
    pub fn new(val: T) -> Self {
        Self { inner: val, _pad: [] }
    }
}
