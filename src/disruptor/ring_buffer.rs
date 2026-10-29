// LMAX Disruptor lock-free ring buffer.
// Pre-allocated, bounded; producer claims sequences via AtomicU64::fetch_add.
// See architecture.md §4 — LMAX Disruptor.

use super::padding::CacheLinePadded;
use crate::market_context::MarketContext;
use std::cell::{Cell, UnsafeCell};
use std::error::Error;
use std::fmt;
use std::hint::{black_box, spin_loop};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const BUFFER_SIZE: usize = 1024; // must be power-of-two

pub struct RingBuffer {
    inner: Arc<RingBufferInner>,
    /// The raw ring exposes convenience methods for same-thread tests and
    /// setup only. It is intentionally not `Sync`; use `split()` to create the
    /// single producer and single consumer handles for inter-thread use.
    _not_sync: PhantomData<Cell<()>>,
}

struct RingBufferInner {
    /// Next producer sequence to claim.
    cursor: CacheLinePadded<AtomicU64>,
    /// Next consumer sequence to read.
    consumer_cursor: CacheLinePadded<AtomicU64>,
    /// Last sequence made visible to consumers.
    published: CacheLinePadded<AtomicU64>,
    slots: Box<[CacheLinePadded<Slot>]>,
}

struct Slot {
    /// Producer may write when this equals its claimed sequence.
    /// Consumer may read when this equals its sequence + 1.
    sequence: AtomicU64,
    event: UnsafeCell<MaybeUninit<MarketEvent>>,
}

// SAFETY: RingBuffer is single-producer/single-consumer. The per-slot sequence
// protocol ensures the producer and consumer never access the same slot's event
// storage at the same time.
unsafe impl Sync for Slot {}

/// Symbol interned to a u64 — zero allocation on the hot path.
/// The intern table lives outside the ring buffer (one-time setup per symbol).
pub type SymbolId = u64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlgoStrategy {
    Market,
    Ioc,
    PassiveLimit,
    Twap,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AlgoParams {
    pub strategy: AlgoStrategy,
    pub urgency: f64,
    pub max_slippage_bps: Option<f64>,
    pub post_only: bool,
    pub reduce_only: bool,
}

impl Default for AlgoParams {
    fn default() -> Self {
        Self {
            strategy: AlgoStrategy::Market,
            urgency: 0.0,
            max_slippage_bps: None,
            post_only: false,
            reduce_only: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketEvent {
    /// Market data depth update for an interned symbol.
    MarketDepthUpdate {
        symbol: SymbolId,
        seq: u64,
    },
    /// Execution fill. `cl_ord_id` is a UUID stored as raw bytes (no heap).
    Fill {
        cl_ord_id: [u8; 36],
        qty: f64,
        price: f64,
        market_context: MarketContext,
    },
    TradeCommand {
        symbol: SymbolId,
        side: TradeSide,
        qty: f64,
        limit_price: Option<f64>,
        confidence: f64,
        algo_params: AlgoParams,
        market_context: MarketContext,
    },
    VarBreach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishError {
    Full,
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "ring buffer is full"),
        }
    }
}

impl Error for PublishError {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SequencedMarketEvent {
    pub sequence: u64,
    pub event: MarketEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventProcessorRole {
    MarketDataReceiver,
    OrderDispatcher,
}

pub struct MarketDataReceiver {
    ring: Arc<RingBufferInner>,
    /// Producers are movable to one thread but not shareable across threads.
    _not_sync: PhantomData<Cell<()>>,
}

pub struct OrderDispatcher {
    ring: Arc<RingBufferInner>,
    /// Consumers are movable to one thread but not shareable across threads.
    _not_sync: PhantomData<Cell<()>>,
}

impl Slot {
    fn new(sequence: u64) -> Self {
        Self {
            sequence: AtomicU64::new(sequence),
            event: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl Default for RingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl RingBuffer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RingBufferInner::new()),
            _not_sync: PhantomData,
        }
    }

    /// Split the ring into the only producer and consumer handles.
    ///
    /// The handles are `Send` but intentionally not `Sync`, so safe code cannot
    /// put either side behind an `Arc` and use it concurrently from multiple
    /// threads.
    ///
    /// ```compile_fail
    /// use _rust::disruptor::ring_buffer::RingBuffer;
    /// use std::sync::Arc;
    ///
    /// let (receiver, _) = RingBuffer::new().split();
    /// let shared = Arc::new(receiver);
    /// let producer = Arc::clone(&shared);
    ///
    /// std::thread::spawn(move || {
    ///     let _ = producer.publish_market_depth_update(1, 1);
    /// });
    /// ```
    pub fn split(self) -> (MarketDataReceiver, OrderDispatcher) {
        let consumer_ring = Arc::clone(&self.inner);
        (
            MarketDataReceiver {
                ring: self.inner,
                _not_sync: PhantomData,
            },
            OrderDispatcher {
                ring: consumer_ring,
                _not_sync: PhantomData,
            },
        )
    }

    /// Reserved extension point for future multi-consumer gate registration.
    /// Current implementation is single-consumer and uses the inline
    /// `consumer_cursor` plus per-slot sequence gates for backpressure.
    pub fn register_consumer_gate(&self, gate: &AtomicU64) {
        // A future MPSC implementation should retain consumer gates here.
        let _ = gate;
    }

    /// Publish one event. Returns the sequence number it was assigned.
    /// Panics if the bounded ring is full; use `try_publish` to handle
    /// backpressure without panicking.
    pub fn publish(&self, event: MarketEvent) -> u64 {
        self.inner.publish(event)
    }

    pub fn try_publish(&self, event: MarketEvent) -> Result<u64, PublishError> {
        self.inner.try_publish(event)
    }

    pub fn try_consume(&self) -> Option<SequencedMarketEvent> {
        self.inner.try_consume()
    }

    /// Returns the sequence number of the last published event, or `None` if empty.
    pub fn last_published(&self) -> Option<u64> {
        self.inner.last_published()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    const fn index(sequence: u64) -> usize {
        (sequence as usize) & (BUFFER_SIZE - 1)
    }
}

impl RingBufferInner {
    fn new() -> Self {
        assert!(BUFFER_SIZE.is_power_of_two());
        let slots = (0..BUFFER_SIZE)
            .map(|sequence| CacheLinePadded::new(Slot::new(sequence as u64)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            cursor: CacheLinePadded::new(AtomicU64::new(0)),
            consumer_cursor: CacheLinePadded::new(AtomicU64::new(0)),
            published: CacheLinePadded::new(AtomicU64::new(u64::MAX)),
            slots,
        }
    }

    fn publish(&self, event: MarketEvent) -> u64 {
        self.try_publish(event)
            .expect("ring buffer is full; use try_publish to handle backpressure")
    }

    fn try_publish(&self, event: MarketEvent) -> Result<u64, PublishError> {
        // SPSC invariant: safe code exposes only one producer handle, so this
        // load-then-claim sequence has no competing producer. A future MPSC
        // variant must replace this with a compare-exchange claim loop.
        let seq = self.cursor.inner.load(Ordering::Acquire);
        let slot = &self.slots[RingBuffer::index(seq)].inner;
        if slot.sequence.load(Ordering::Acquire) != seq {
            return Err(PublishError::Full);
        }

        let claimed = self.cursor.inner.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(claimed, seq);

        unsafe {
            (*slot.event.get()).write(event);
        }
        slot.sequence.store(seq + 1, Ordering::Release);
        self.published.inner.store(seq, Ordering::Release);

        Ok(seq)
    }

    fn try_consume(&self) -> Option<SequencedMarketEvent> {
        let seq = self.consumer_cursor.inner.load(Ordering::Acquire);
        let slot = &self.slots[RingBuffer::index(seq)].inner;
        if slot.sequence.load(Ordering::Acquire) != seq + 1 {
            return None;
        }

        let event = unsafe { (*slot.event.get()).assume_init_read() };
        self.consumer_cursor.inner.store(seq + 1, Ordering::Release);
        slot.sequence
            .store(seq + BUFFER_SIZE as u64, Ordering::Release);

        Some(SequencedMarketEvent {
            sequence: seq,
            event,
        })
    }

    fn last_published(&self) -> Option<u64> {
        let seq = self.published.inner.load(Ordering::Acquire);
        if seq == u64::MAX {
            None
        } else {
            Some(seq)
        }
    }

    fn capacity(&self) -> usize {
        BUFFER_SIZE
    }

    fn len(&self) -> usize {
        let produced = self.cursor.inner.load(Ordering::Acquire);
        let consumed = self.consumer_cursor.inner.load(Ordering::Acquire);
        (produced - consumed) as usize
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn is_full(&self) -> bool {
        self.len() == BUFFER_SIZE
    }
}

impl MarketDataReceiver {
    pub fn role(&self) -> EventProcessorRole {
        EventProcessorRole::MarketDataReceiver
    }

    pub fn publish_market_depth_update(
        &self,
        symbol: SymbolId,
        seq: u64,
    ) -> Result<u64, PublishError> {
        self.ring
            .try_publish(MarketEvent::MarketDepthUpdate { symbol, seq })
    }

    pub fn publish_fill(
        &self,
        cl_ord_id: [u8; 36],
        qty: f64,
        price: f64,
    ) -> Result<u64, PublishError> {
        self.publish_fill_with_context(cl_ord_id, qty, price, MarketContext::default())
    }

    pub fn publish_fill_with_context(
        &self,
        cl_ord_id: [u8; 36],
        qty: f64,
        price: f64,
        market_context: MarketContext,
    ) -> Result<u64, PublishError> {
        self.ring.try_publish(MarketEvent::Fill {
            cl_ord_id,
            qty,
            price,
            market_context,
        })
    }

    pub fn publish_var_breach(&self) -> Result<u64, PublishError> {
        self.ring.try_publish(MarketEvent::VarBreach)
    }

    pub fn publish_trade_command(
        &self,
        symbol: SymbolId,
        side: TradeSide,
        qty: f64,
        limit_price: Option<f64>,
        confidence: f64,
        algo_params: AlgoParams,
    ) -> Result<u64, PublishError> {
        self.ring.try_publish(MarketEvent::TradeCommand {
            symbol,
            side,
            qty,
            limit_price,
            confidence,
            algo_params,
            market_context: MarketContext::default(),
        })
    }

    pub fn publish_market_event(&self, event: MarketEvent) -> Result<u64, PublishError> {
        self.ring.try_publish(event)
    }
}

impl OrderDispatcher {
    pub fn role(&self) -> EventProcessorRole {
        EventProcessorRole::OrderDispatcher
    }

    pub fn try_dispatch(&self) -> Option<SequencedMarketEvent> {
        self.ring.try_consume()
    }
}

pub fn benchmark_single_producer_single_consumer(iterations: u64) -> f64 {
    let ring = RingBuffer::new();
    let start = Instant::now();

    for seq in 0..iterations {
        ring.publish(MarketEvent::MarketDepthUpdate { symbol: 1, seq });
        let event = loop {
            if let Some(event) = ring.try_consume() {
                break event;
            }
            spin_loop();
        };
        black_box(event);
    }

    iterations as f64 / start.elapsed().as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_returns_monotonic_sequences() {
        let rb = RingBuffer::new();
        let s0 = rb.publish(MarketEvent::VarBreach);
        let s1 = rb.publish(MarketEvent::MarketDepthUpdate { symbol: 1, seq: 42 });
        assert_eq!(s0 + 1, s1);
    }

    #[test]
    fn capacity_is_power_of_two_and_wraps_with_bitmask() {
        let rb = RingBuffer::new();

        assert!(BUFFER_SIZE.is_power_of_two());
        assert_eq!(rb.capacity(), BUFFER_SIZE);
        assert_eq!(RingBuffer::index(BUFFER_SIZE as u64), 0);
        assert!(rb.is_empty());
    }

    #[test]
    fn fill_event_is_copy_no_alloc() {
        let cl_ord_id = [b'a'; 36];
        let ev = MarketEvent::Fill {
            cl_ord_id,
            qty: 1.0,
            price: 50_000.0,
            market_context: MarketContext::default(),
        };
        // Copy trait means no heap involvement — this is a compile-time proof.
        let _ev2 = ev;
        let _ev3 = ev;
    }

    #[test]
    fn trade_command_event_is_copy_no_alloc() {
        let ev = MarketEvent::TradeCommand {
            symbol: 42,
            side: TradeSide::Buy,
            qty: 0.5,
            limit_price: Some(50_000.0),
            confidence: 0.9,
            algo_params: AlgoParams {
                strategy: AlgoStrategy::PassiveLimit,
                urgency: 0.2,
                max_slippage_bps: Some(3.0),
                post_only: true,
                reduce_only: false,
            },
            market_context: MarketContext::default(),
        };
        let _ev2 = ev;
        let _ev3 = ev;
    }

    #[test]
    fn last_published_none_before_first_publish() {
        let rb = RingBuffer::new();
        assert!(rb.last_published().is_none());
    }

    #[test]
    fn last_published_reflects_latest_seq() {
        let rb = RingBuffer::new();
        let seq = rb.publish(MarketEvent::VarBreach);
        assert_eq!(rb.last_published(), Some(seq));
    }

    #[test]
    fn consumer_reads_events_in_publish_order() {
        let rb = RingBuffer::new();

        for seq in 0..16 {
            rb.try_publish(MarketEvent::MarketDepthUpdate { symbol: 7, seq })
                .expect("publish succeeds below capacity");
        }

        for expected_seq in 0..16 {
            let consumed = rb.try_consume().expect("event available");
            assert_eq!(consumed.sequence, expected_seq);
            assert_eq!(
                consumed.event,
                MarketEvent::MarketDepthUpdate {
                    symbol: 7,
                    seq: expected_seq
                }
            );
        }
        assert!(rb.try_consume().is_none());
    }

    #[test]
    fn full_buffer_does_not_drop_or_overwrite_events() {
        let rb = RingBuffer::new();

        for seq in 0..BUFFER_SIZE as u64 {
            rb.try_publish(MarketEvent::MarketDepthUpdate { symbol: 9, seq })
                .expect("capacity slot is available");
        }
        assert!(rb.is_full());
        assert_eq!(rb.len(), BUFFER_SIZE);
        assert_eq!(
            rb.try_publish(MarketEvent::VarBreach),
            Err(PublishError::Full)
        );

        for expected_seq in 0..BUFFER_SIZE as u64 {
            let consumed = rb.try_consume().expect("capacity event available");
            assert_eq!(
                consumed.event,
                MarketEvent::MarketDepthUpdate {
                    symbol: 9,
                    seq: expected_seq
                }
            );
        }
        assert!(rb.try_consume().is_none());
        assert!(rb.is_empty());
    }

    #[test]
    fn market_data_receiver_and_order_dispatcher_process_events() {
        let (receiver, dispatcher) = RingBuffer::new().split();

        assert_eq!(receiver.role(), EventProcessorRole::MarketDataReceiver);
        assert_eq!(dispatcher.role(), EventProcessorRole::OrderDispatcher);

        receiver
            .publish_market_depth_update(42, 100)
            .expect("market data event published");
        let dispatched = dispatcher.try_dispatch().expect("event dispatched");

        assert_eq!(dispatched.sequence, 0);
        assert_eq!(
            dispatched.event,
            MarketEvent::MarketDepthUpdate {
                symbol: 42,
                seq: 100
            }
        );
    }

    #[test]
    fn split_processor_handles_are_send_and_work_across_threads() {
        fn assert_send<T: Send>() {}
        assert_send::<MarketDataReceiver>();
        assert_send::<OrderDispatcher>();

        let (receiver, dispatcher) = RingBuffer::new().split();
        let producer = std::thread::spawn(move || {
            for seq in 0..128 {
                loop {
                    if receiver.publish_market_depth_update(99, seq).is_ok() {
                        break;
                    }
                    spin_loop();
                }
            }
        });
        let consumer = std::thread::spawn(move || {
            for expected_seq in 0..128 {
                let consumed = loop {
                    if let Some(event) = dispatcher.try_dispatch() {
                        break event;
                    }
                    spin_loop();
                };
                assert_eq!(
                    consumed.event,
                    MarketEvent::MarketDepthUpdate {
                        symbol: 99,
                        seq: expected_seq
                    }
                );
            }
        });

        producer.join().expect("producer thread joins");
        consumer.join().expect("consumer thread joins");
    }

    #[test]
    #[ignore = "release-mode benchmark; run with cargo test --release benchmark_single_producer_single_consumer_exceeds_10m_events_per_sec -- --ignored"]
    fn benchmark_single_producer_single_consumer_exceeds_10m_events_per_sec() {
        let throughput = benchmark_single_producer_single_consumer(1_000_000);

        assert!(
            throughput >= 10_000_000.0,
            "throughput {throughput:.0} events/sec below 10M events/sec target"
        );
    }
}
