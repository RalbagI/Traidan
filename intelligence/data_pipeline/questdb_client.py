"""Async QuestDB client.
Uses LATEST ON queries (hash-map backed, ~27× faster than GROUP BY) to retrieve
the current L2 order book state. Exports cold data as Parquet for ML training.
See architecture.md §3 — Data Layer.
"""

from __future__ import annotations

import polars as pl


QUESTDB_HTTP = "http://localhost:9000"
QUESTDB_ILP  = "localhost:9009"


async def latest_book(symbol: str) -> pl.DataFrame:
    """Return the most recent L2 order book snapshot for `symbol`."""
    query = f"""
        SELECT timestamp, bids, asks
        FROM market_data
        LATEST ON timestamp PARTITION BY symbol
        WHERE symbol = '{symbol}'
    """
    # TODO: execute via QuestDB HTTP API, return as Polars DataFrame
    raise NotImplementedError


async def export_parquet(symbol: str, start: str, end: str, dest_s3_path: str) -> None:
    """Export a time range of market_data for `symbol` to Parquet on S3."""
    # TODO: QuestDB REST export → Arrow → write_parquet to S3
    raise NotImplementedError
