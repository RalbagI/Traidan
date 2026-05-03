"""Offline strategy augmentation: reviews RL policy logic and suggests
rule refinements. Runs during training, never at inference time.
See architecture.md §6 — LLM Orchestrator.
"""

from __future__ import annotations


async def review_reward_function(reward_source: str) -> str:
    """Send the reward function source to the LLM for critique and improvement
    suggestions. Returns a markdown-formatted analysis."""
    # TODO: call Anthropic SDK with reward_source, parse suggestions
    raise NotImplementedError


async def generate_feature_hypothesis(market_regime: str) -> list[str]:
    """Ask the LLM to hypothesize new microstructure features relevant to
    the current market regime (e.g., 'high_volatility', 'low_liquidity')."""
    # TODO: implement
    raise NotImplementedError
