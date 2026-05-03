"""Market Impact Adjusted Reward functions.

Composite reward combining:
  1. Slippage-adjusted PnL — raw return minus simulated execution cost.
  2. CARA utility — U = -exp(-λ * r), exponentially penalizes high-variance
     strategies. λ calibrated to historical PnL volatility.
  3. Temporal coherence bonus — rewards consecutive position consistency,
     penalizes oscillation that bleeds capital to maker/taker fees.

See architecture.md §6 — Market Impact Adjusted Reward Functions.
"""

from __future__ import annotations

import math


def cara_utility(pnl: float, risk_aversion: float) -> float:
    """Constant Absolute Risk Aversion utility: U = -exp(-λ * r).

    Marginal utility of additional profit decreases exponentially, naturally
    penalizing excessive risk exposure.
    """
    return -math.exp(-risk_aversion * pnl)


def temporal_coherence_bonus(
    prev_position: float,
    curr_position: float,
    fee_rate: float = 0.001,
) -> float:
    """Penalize position flip magnitude proportional to fee cost.

    Positive when positions are consistent; negative on large reversals.
    """
    turnover = abs(curr_position - prev_position)
    return -turnover * fee_rate


def compute_reward(
    raw_pnl: float,
    execution_cost: float,
    prev_position: float,
    curr_position: float,
    risk_aversion: float = 0.1,
    fee_rate: float = 0.001,
) -> float:
    """Composite Market Impact Adjusted Reward."""
    adjusted_pnl = raw_pnl - execution_cost
    utility = cara_utility(adjusted_pnl, risk_aversion)
    coherence = temporal_coherence_bonus(prev_position, curr_position, fee_rate)
    return utility + coherence
