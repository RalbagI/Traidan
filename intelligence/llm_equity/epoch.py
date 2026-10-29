"""Explicit, stopped-writer policy migration that preserves the authority account."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


def _write(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def verify_account_transition(state_dir: Path, target_id: str, *, source_id: str | None = None) -> None:
    """An opening seed alone is never evidence that a migration completed."""
    target = state_dir / "epochs" / target_id
    receipt = json.loads((target / "policy-transition.json").read_text())
    origin = str(receipt["from_epoch"])
    if (re.fullmatch(r"epoch-[a-f0-9]{16}", origin) is None
            or receipt["to_epoch"] != target_id or source_id is not None and source_id != origin):
        raise ValueError("policy transition identity mismatch")
    original = (state_dir / "epochs" / origin / "rust-authority-state.json").read_bytes()
    source = json.loads(original)
    seed = json.loads((target / "account-opening-state.json").read_text())
    authority = json.loads((target / "rust-authority-state.json").read_text())
    if (receipt["source_sha256"] != hashlib.sha256(original).hexdigest()
            or {k: v for k, v in seed.items() if k != "saved_at"}
            != {k: v for k, v in source.items() if k != "saved_at"}
            or authority.get("schema_version") != 1 or "portfolio" not in authority
            or int(authority["highest_sequence"]) < int(seed["highest_sequence"])
            or source_id is not None and authority != source):
        raise ValueError("policy transition account mismatch")


def migrate_account_epoch(state_dir: Path, descriptor: Mapping[str, Any], *, at: datetime) -> Path:
    target_id = str(descriptor["epoch_id"])
    if re.fullmatch(r"epoch-[a-f0-9]{16}", target_id) is None:
        raise ValueError("invalid target epoch")
    runtime_dirs = {state_dir.parent / "local-dry-run",
                    Path(__file__).resolve().parents[2] / ".agent/state/local-dry-run"}
    if configured := os.environ.get("TRAIDAN_LOCAL_DRY_RUN_STATE_DIR"):
        runtime_dirs.add(Path(configured).expanduser().resolve())
    for pid_file in (root / "pids" / f"{name}.pid" for root in runtime_dirs
                     for name in ("rust", "llm-equity-service")):
        if pid_file.exists():
            pid = int(pid_file.read_text().strip())
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                pass
            else:
                raise RuntimeError("stop paper services before migrating the policy epoch")
    current_path = state_dir / "strategy-epoch.json"
    previous = json.loads(current_path.read_text())
    source_id = str(previous["epoch_id"])
    if re.fullmatch(r"epoch-[a-f0-9]{16}", source_id) is None:
        raise ValueError("invalid source epoch")
    target = state_dir / "epochs" / target_id
    if source_id == target_id:
        if not (target / "policy-transition.json").exists():
            raise ValueError("target lacks a verified transition")
        verify_account_transition(state_dir, target_id)
        return target
    source = state_dir / "epochs" / source_id
    source_bytes = (source / "rust-authority-state.json").read_bytes()
    authority = json.loads(source_bytes)
    if authority.get("schema_version") != 1 or "portfolio" not in authority:
        raise ValueError("invalid source authority")
    target.mkdir(parents=True, exist_ok=True)
    with (target / ".migration.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (target / "rust-authority-state.json").exists():
            raise ValueError("target account already exists; reconcile before retrying")
        seed = {**authority, "saved_at": at.isoformat()}
        _write(target / "account-opening-state.json", seed)
        _write(target / "rust-authority-state.json", authority)
        (target / "rust-authority-state.jsonl").write_text(json.dumps(seed) + "\n")
        for filename in ("economic-cost-policy.json", "position-exit-plans.json"):
            path = source / filename
            if path.exists():
                _write(target / filename, json.loads(path.read_text()))
        plans_path = target / "position-exit-plans.json"
        plans = json.loads(plans_path.read_text()) if plans_path.exists() else {}
        for symbol, position in authority["portfolio"]["positions"].items():
            quantity = float(position["quantity"])
            if (quantity and symbol not in plans
                    and descriptor.get("components", {}).get("risk_policy_version") != "fully_invested_v1"):
                sign = 1 if quantity > 0 else -1
                plans[symbol] = {"policy": "profit_driven_v1", "side": sign,
                    "origin_command_id": f"migration:{source_id}:{symbol}",
                    "stop_price": float(authority["last_prices"][symbol]) * (1 - sign * .05),
                    "exit_at": (at + timedelta(days=1)).isoformat(),
                    "thesis": "Inherited paper holding; bounded transition review window."}
        _write(plans_path, plans)
        _write(target / "account-opening-exit-plans.json", plans)
        if (source / "rust-authority-state.json").read_bytes() != source_bytes:
            raise RuntimeError("source account changed during migration; reconcile target")
        transition = {"schema_version": 1, "from_epoch": source_id, "to_epoch": target_id,
            "transition_at": at.isoformat(), "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "preserved": ["cash", "positions", "sequence", "seen_cycle_ids", "peak_nav", "risk_counters"]}
        _write(target / "policy-transition.json", transition)
        _write(target / "strategy-epoch.json", descriptor)
        _write(current_path, descriptor)
    return target
