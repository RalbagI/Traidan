-- QuestDB market_data schema for L2 order book snapshots.
--
-- SYMBOL dictionary encoding keeps pair lookup integer-backed instead of
-- string-comparison based.
-- PARTITION BY HOUR keeps partition files bounded under sub-millisecond update rates.
-- DEDUP UPSERT handles duplicate ticks caused by out-of-order WebSocket delivery
-- by replacing rows with the same timestamp+symbol key.
-- Ingestion code writes exactly 20 levels per side; shallower books must be padded
-- with zero volume levels before insert.
CREATE TABLE IF NOT EXISTS market_data (
    timestamp TIMESTAMP,
    symbol    SYMBOL,
    bids      DOUBLE[20][2],
    asks      DOUBLE[20][2]
) TIMESTAMP(timestamp)
  PARTITION BY HOUR
  DEDUP UPSERT KEYS(timestamp, symbol);
