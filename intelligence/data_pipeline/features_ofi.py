"""Order Flow Imbalance (OFI) feature computation.

OFI quantifies net changes in volume at the best bid/ask over microsecond
windows. Rising OFI is a leading indicator for short-term directional price
momentum because institutional traders interact with limit books before prices
move. See architecture.md §6 — Engineered Microstructure Features.
"""

from __future__ import annotations

import numpy as np
import polars as pl


def compute_ofi(book_snapshots: pl.DataFrame) -> pl.Series:
    """Compute OFI from a sequence of L2 order book snapshots.

    Args:
        book_snapshots: DataFrame with columns [timestamp, best_bid_price,
            best_bid_qty, best_ask_price, best_ask_qty].

    Returns:
        OFI series aligned to book_snapshots timestamps.
    """
    # TODO: implement delta-based OFI calculation
    #   OFI_t = ΔBidQty_t * sign(ΔBidPrice_t) - ΔAskQty_t * sign(ΔAskPrice_t)
    raise NotImplementedError
