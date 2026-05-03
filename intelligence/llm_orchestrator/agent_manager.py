"""Multi-agent coordination: routes sentiment signals to RL agents,
proposes hyperparameter tuning, manages strategy lifecycle.
See architecture.md §6 — LLM Orchestrator.
"""

from __future__ import annotations


class AgentManager:
    """Coordinates multiple RL agents and LLM-proposed strategy updates."""

    def __init__(self) -> None:
        self._agents: dict[str, object] = {}

    def register_agent(self, name: str, agent: object) -> None:
        self._agents[name] = agent

    async def dispatch_sentiment(self, symbol: str, sentiment_score: float) -> None:
        """Route a sentiment vector to all agents tracking the given symbol."""
        # TODO: implement routing logic
        raise NotImplementedError

    async def propose_hyperparameter_update(self, agent_name: str) -> dict:
        """Ask the LLM orchestrator to suggest new hyperparameters based on
        recent training metrics. Returns a dict of param overrides."""
        # TODO: pull W&B metrics, call LLM, parse structured response
        raise NotImplementedError
