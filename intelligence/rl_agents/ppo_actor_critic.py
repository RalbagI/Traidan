"""PPO Actor-Critic RL agent.

State space: order book depth (20 levels × 2 sides), OFI, VPIN, portfolio
             exposure, sentiment vector.
Action space: continuous position target ∈ [-1.0, 1.0] (fraction of max notional).
See architecture.md §6 — Reinforcement Learning Workflow.
"""

from __future__ import annotations

import torch
import torch.nn as nn


class PolicyNetwork(nn.Module):
    """Actor: maps state → Gaussian distribution over continuous action."""

    def __init__(self, state_dim: int, hidden_dim: int = 256) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
        )
        self.mean_head = nn.Linear(hidden_dim, 1)
        self.log_std = nn.Parameter(torch.zeros(1))

    def forward(self, state: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        features = self.net(state)
        mean = torch.tanh(self.mean_head(features))
        std = self.log_std.exp().expand_as(mean)
        return mean, std


class ValueNetwork(nn.Module):
    """Critic: estimates long-term expected return from current state."""

    def __init__(self, state_dim: int, hidden_dim: int = 256) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, state: torch.Tensor) -> torch.Tensor:
        return self.net(state)


class PPOAgent:
    """Proximal Policy Optimization agent wrapping actor and critic networks."""

    def __init__(self, state_dim: int, lr: float = 3e-4, clip_eps: float = 0.2) -> None:
        self.actor = PolicyNetwork(state_dim)
        self.critic = ValueNetwork(state_dim)
        self.actor_optimizer = torch.optim.Adam(self.actor.parameters(), lr=lr)
        self.critic_optimizer = torch.optim.Adam(self.critic.parameters(), lr=lr)
        self.clip_eps = clip_eps

    def select_action(self, state: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Sample an action and return (action, log_prob)."""
        mean, std = self.actor(state)
        dist = torch.distributions.Normal(mean, std)
        action = dist.sample()
        return torch.clamp(action, -1.0, 1.0), dist.log_prob(action)

    def update(self, states, actions, old_log_probs, returns, advantages) -> dict[str, float]:
        """Single PPO update step. Returns loss metrics for logging."""
        # TODO: implement clipped surrogate objective and value loss
        raise NotImplementedError
