"""MLflow / Weights & Biases experiment tracking.

Logs hyperparameters, feature sets, and episodic rewards across training runs.
Enforces sim-to-reality parity: models must match paper-trading performance
within configurable thresholds before live capital is allocated.
See architecture.md §6 — MLOps.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class RunConfig:
    symbol: str
    algo: str              # 'ppo' | 'sac'
    risk_aversion: float
    lr: float
    clip_eps: float
    reward_version: str


def log_run(config: RunConfig, metrics: dict[str, float], backend: str = "mlflow") -> str:
    """Log a training run to the specified tracking backend.

    Returns the run ID.
    """
    if backend == "mlflow":
        # TODO: mlflow.start_run(), mlflow.log_params(), mlflow.log_metrics()
        raise NotImplementedError
    elif backend == "wandb":
        # TODO: wandb.init(), wandb.log()
        raise NotImplementedError
    else:
        raise ValueError(f"Unknown backend: {backend}")


def check_parity(
    sim_sharpe: float,
    paper_sharpe: float,
    threshold: float = 0.15,
) -> bool:
    """Return True if sim and paper Sharpe ratios are within `threshold`."""
    return abs(sim_sharpe - paper_sharpe) <= threshold
