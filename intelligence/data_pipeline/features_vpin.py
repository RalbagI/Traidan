"""Volume-Synchronized Probability of Informed Trading (VPIN).

Measures market toxicity by classifying incoming tick volume as buyer- or
seller-initiated. VPIN spikes indicate toxic institutional flow; the RL agent
widens quoting spreads to avoid adverse selection.
See architecture.md §6 — Engineered Microstructure Features.
"""

from __future__ import annotations

import numpy as np
import polars as pl


def compute_vpin(
    ticks: pl.DataFrame,
    bucket_volume: float,
    n_buckets: int = 50,
) -> float:
    """Compute VPIN over the last `n_buckets` volume buckets.

    Args:
        ticks: DataFrame with columns [timestamp, price, volume, side].
               side: 1 = buyer-initiated, -1 = seller-initiated.
        bucket_volume: target volume per bucket (calibrate to avg daily volume / n_buckets).
        n_buckets: rolling window length in buckets.

    Returns:
        VPIN scalar in [0, 1]. Values > 0.5 indicate high toxicity.
    """
    # TODO: implement volume-bucket classification and VPIN calculation
    raise NotImplementedError
