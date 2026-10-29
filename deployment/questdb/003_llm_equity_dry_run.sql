-- LLM-only equity dry-run schema.
--
-- These tables store simulated decisions and fills only. They must not imply
-- broker-paper or live order submission; Alpaca is a market-data source in v1.
CREATE TABLE IF NOT EXISTS llm_equity_market_research_snapshot (
    timestamp              TIMESTAMP,
    snapshot_id            SYMBOL,
    market_session         SYMBOL,
    symbols_json           STRING,
    market_calendar_json   STRING,
    source_freshness_json  STRING,
    trend_summaries_json   STRING,
    macro_news_json        STRING,
    unavailable_flags_json STRING,
    provider               SYMBOL
) TIMESTAMP(timestamp)
  PARTITION BY DAY
  DEDUP UPSERT KEYS(timestamp, snapshot_id);

CREATE TABLE IF NOT EXISTS llm_equity_decision (
    timestamp              TIMESTAMP,
    cycle_id               SYMBOL,
    market_session         SYMBOL,
    portfolio_nav_usd      DOUBLE,
    commands_json          STRING,
    evidence_json          STRING,
    risk_assessment_json   STRING,
    memory_updates_json    STRING,
    validation_status      SYMBOL
) TIMESTAMP(timestamp)
  PARTITION BY DAY
  DEDUP UPSERT KEYS(timestamp, cycle_id);

CREATE TABLE IF NOT EXISTS llm_equity_simulated_fill (
    timestamp              TIMESTAMP,
    cycle_id               SYMBOL,
    command_id             SYMBOL,
    symbol                 SYMBOL,
    side                   SYMBOL,
    qty                    DOUBLE,
    avg_price              DOUBLE,
    notional_usd           DOUBLE,
    fee_usd                DOUBLE,
    slippage_usd           DOUBLE,
    cash_after_usd         DOUBLE,
    nav_after_usd          DOUBLE,
    status                 SYMBOL,
    rejection_reason       SYMBOL
) TIMESTAMP(timestamp)
  PARTITION BY DAY
  DEDUP UPSERT KEYS(timestamp, cycle_id, command_id);

CREATE TABLE IF NOT EXISTS llm_equity_memory_event (
    timestamp              TIMESTAMP,
    event_id               SYMBOL,
    cycle_id               SYMBOL,
    thesis                 STRING,
    outcome                STRING,
    conclusion             STRING,
    prompt_adjustment      STRING,
    next_watch_items_json  STRING
) TIMESTAMP(timestamp)
  PARTITION BY DAY
  DEDUP UPSERT KEYS(timestamp, event_id);
