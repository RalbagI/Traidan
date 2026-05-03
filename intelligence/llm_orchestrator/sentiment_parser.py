"""Parse news and social streams into sentiment tensors for RL input features.
Uses Claude via the Anthropic SDK in async batch mode — never in the hot path.
See architecture.md §6 — LLM Orchestrator.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import numpy as np


@dataclass
class SentimentVector:
    symbol: str
    score: float          # -1.0 (bearish) to +1.0 (bullish)
    confidence: float     # 0.0 to 1.0
    regime: str           # 'risk_on' | 'risk_off' | 'neutral'


async def parse_news_batch(headlines: list[str], symbol: str) -> SentimentVector:
    """Send a batch of headlines to the LLM and return a sentiment vector.

    This runs asynchronously and feeds into RL state — never blocks execution.
    """
    # TODO: call Anthropic SDK, extract structured sentiment fields
    raise NotImplementedError
