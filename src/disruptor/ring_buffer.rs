// LMAX Disruptor lock-free ring buffer.
// Pre-allocated, bounded; event processors claim sequences via atomic CAS.
// See architecture.md §4 — LMAX Disruptor.

use std::sync::atomic::{AtomicU64, Ordering};
use super::padding::CacheLinePadded;

pub const BUFFER_SIZE: usize = 1024; // must be power-of-two

pub struct RingBuffer {
    cursor: CacheLinePadded<AtomicU64>,
    slots: Box<[CacheLinePadded<Option<MarketEvent>>]>,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    BookUpdate { symbol: String, seq: u64 },
    Fill { cl_ord_id: String, qty: f64, price: f64 },
    VarBreach,
}

impl RingBuffer {
    pub fn new() -> Self {
        let slots = (0..BUFFER_SIZE)
            .map(|_| CacheLinePadded::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            cursor: CacheLinePadded::new(AtomicU64::new(0)),
            slots,
        }
    }

    pub fn publish(&self, event: MarketEvent) -> u64 {
        let seq = self.cursor.inner.fetch_add(1, Ordering::AcqRel);
        let idx = (seq as usize) & (BUFFER_SIZE - 1);
        // SAFETY: single producer assumed; full disruptor gating required for MPSC
        unsafe {
            let slot = &self.slots[idx] as *const _ as *mut CacheLinePadded<Option<MarketEvent>>;
            (*slot).inner = Some(event);
        }
        seq
    }
}
