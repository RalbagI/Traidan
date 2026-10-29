#!/usr/bin/env python3
"""Validate the supervised LLM equity resident without mutating runtime state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

RUNTIME_CRITICAL_PREFIXES = (
    "config/",
    "intelligence/",
    "prompts/",
    "schemas/",
    "src/",
)
RUNTIME_CRITICAL_FILES = frozenset(
    {
        "Cargo.lock",
        "Cargo.toml",
        "pyproject.toml",
        "scripts/local-dry-run.sh",
    }
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _git(project_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=project_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "git command failed"
        raise ValueError(detail)
    return completed.stdout.strip()


def _changed_paths(project_root: Path, resident_sha: str, expected_sha: str) -> list[str]:
    if resident_sha == expected_sha:
        return []
    output = _git(
        project_root,
        "diff",
        "--name-only",
        "--no-renames",
        f"{resident_sha}..{expected_sha}",
        "--",
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def _dirty_paths(project_root: Path) -> list[str]:
    paths: set[str] = set()
    for args in (
        ("diff", "--name-only", "--no-renames", "--"),
        ("diff", "--cached", "--name-only", "--no-renames", "--"),
        ("ls-files", "--others", "--exclude-standard"),
    ):
        output = _git(project_root, *args)
        paths.update(line.strip() for line in output.splitlines() if line.strip())
    return sorted(paths)


def _runtime_critical(path: str) -> bool:
    return path in RUNTIME_CRITICAL_FILES or path.startswith(RUNTIME_CRITICAL_PREFIXES)


def _runtime_file_hash(project_root: Path, relative_path: str) -> str:
    try:
        payload = (project_root / relative_path).read_bytes()
    except OSError as exc:
        raise ValueError(f"{relative_path} is unreadable: {exc}") from exc
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _runtime_drift_errors(
    project_root: Path,
    *,
    runtime_ref: str,
    expected_sha: str,
    label: str,
) -> list[str]:
    if not runtime_ref:
        return [f"{label} build ref is required"]
    try:
        runtime_commit = _git(
            project_root,
            "rev-parse",
            "--verify",
            f"{runtime_ref}^{{commit}}",
        )
        changed = _changed_paths(project_root, runtime_commit, expected_sha)
    except ValueError as exc:
        return [f"{label} drift comparison failed: {exc}"]
    critical = [path for path in changed if _runtime_critical(path)]
    return [f"{label} runtime-critical drift: " + ", ".join(critical)] if critical else []


def _scheduler_errors(service: Mapping[str, Any], runtime_project_root: Path) -> list[str]:
    owner = str(service.get("scheduler_owner", ""))
    matched = re.fullmatch(r"resident:([1-9][0-9]*)", owner)
    if matched is None:
        return ["scheduler_owner must start with resident: and contain a valid PID"]
    resident_pid = int(matched.group(1))
    pid_file = (
        runtime_project_root
        / ".agent"
        / "state"
        / "local-dry-run"
        / "pids"
        / "llm-equity-service.pid"
    )
    try:
        supervised_pid = int(pid_file.read_text(encoding="utf-8").strip())
    except (OSError, ValueError) as exc:
        return [f"resident scheduler PID metadata is invalid: {exc}"]
    if supervised_pid != resident_pid:
        return [
            f"resident scheduler PID {resident_pid} does not match supervised PID {supervised_pid}"
        ]
    try:
        os.kill(resident_pid, 0)
    except (OSError, ValueError):
        return [f"resident scheduler process is not alive: {resident_pid}"]
    return []


def validate_resident_health(
    status: Mapping[str, Any],
    *,
    project_root: Path,
    runtime_project_root: Path | None = None,
    expected_ref: str,
) -> list[str]:
    runtime_root = runtime_project_root or project_root
    errors: list[str] = []
    if status.get("run_mode") != "dry_run":
        errors.append("run_mode must be dry_run")
    if status.get("live_order_capability_disabled") is not True:
        errors.append("live_order_capability_disabled must be true")

    llm_equity = _mapping(status.get("llm_equity"))
    if llm_equity.get("fill_mode") != "paper":
        errors.append("resident fill_mode must be paper")
    runtime_identity = _mapping(llm_equity.get("runtime_identity"))
    service = _mapping(runtime_identity.get("service"))
    if service.get("fill_mode") != "paper":
        errors.append("service identity fill_mode must be paper")
    if service.get("risk_mode") not in {"profit_driven", "fully_invested"}:
        errors.append("resident risk_mode must be profit_driven or fully_invested")
    errors.extend(_scheduler_errors(service, runtime_root))

    resident_sha = str(service.get("git_sha", "")).strip()
    rust_build_ref = str(runtime_identity.get("rust_build_ref", "")).strip()
    try:
        expected_sha = _git(project_root, "rev-parse", "--verify", f"{expected_ref}^{{commit}}")
    except ValueError as exc:
        errors.append(f"expected ref comparison failed: {exc}")
        return errors

    errors.extend(
        _runtime_drift_errors(
            project_root,
            runtime_ref=resident_sha,
            expected_sha=expected_sha,
            label="service",
        )
    )
    errors.extend(
        _runtime_drift_errors(
            project_root,
            runtime_ref=rust_build_ref,
            expected_sha=expected_sha,
            label="rust",
        )
    )
    try:
        dirty_critical = [path for path in _dirty_paths(runtime_root) if _runtime_critical(path)]
    except ValueError as exc:
        errors.append(f"runtime worktree comparison failed: {exc}")
    else:
        if dirty_critical:
            errors.append("dirty runtime-critical paths: " + ", ".join(dirty_critical))

    for identity_key, relative_path in (
        ("python_source_hash", "intelligence/llm_equity/hourly_service.py"),
        ("review_schema_hash", "schemas/llm_portfolio_review_v1.schema.json" if service.get("risk_mode") == "fully_invested" else "schemas/llm_review_result_v3.schema.json"),
    ):
        reported_hash = str(service.get(identity_key, "")).strip()
        try:
            current_hash = _runtime_file_hash(runtime_root, relative_path)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if reported_hash != current_hash:
            errors.append(f"{identity_key} does not match resident runtime file")
    if service.get("risk_mode") == "fully_invested":
        hashes = _mapping(service.get("allocation_source_hashes"))
        for name in ("fully_invested.py", "fully_invested_service.py"):
            try:
                actual = _runtime_file_hash(runtime_root, f"intelligence/llm_equity/{name}")
                if hashes.get(name) != actual:
                    errors.append(f"allocation source hash mismatch: {name}")
            except ValueError as exc:
                errors.append(str(exc))
    return errors


def _drift_summary(
    status: Mapping[str, Any],
    *,
    project_root: Path,
    expected_ref: str,
) -> str:
    service = _mapping(
        _mapping(_mapping(status.get("llm_equity")).get("runtime_identity")).get("service")
    )
    runtime_identity = _mapping(_mapping(status.get("llm_equity")).get("runtime_identity"))
    resident_sha = str(service.get("git_sha", "")).strip()
    rust_build_ref = str(runtime_identity.get("rust_build_ref", "")).strip()
    expected_sha = _git(project_root, "rev-parse", "--verify", f"{expected_ref}^{{commit}}")
    resident_commit = _git(project_root, "rev-parse", "--verify", f"{resident_sha}^{{commit}}")
    rust_commit = _git(project_root, "rev-parse", "--verify", f"{rust_build_ref}^{{commit}}")
    changed = sorted(
        set(_changed_paths(project_root, resident_commit, expected_sha))
        | set(_changed_paths(project_root, rust_commit, expected_sha))
    )
    if not changed:
        return "resident service and Rust authority match expected main"
    return "non-runtime drift accepted: " + ", ".join(changed)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status-file", required=True, type=Path)
    parser.add_argument("--project-root", required=True, type=Path)
    parser.add_argument("--runtime-project-root", required=True, type=Path)
    parser.add_argument("--expected-ref", default="HEAD")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        parsed = json.loads(args.status_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"resident health status is unreadable: {exc}", file=sys.stderr)
        return 1
    if not isinstance(parsed, Mapping):
        print("resident health status must be a JSON object", file=sys.stderr)
        return 1

    errors = validate_resident_health(
        parsed,
        project_root=args.project_root,
        runtime_project_root=args.runtime_project_root,
        expected_ref=args.expected_ref,
    )
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    try:
        summary = _drift_summary(
            parsed,
            project_root=args.project_root,
            expected_ref=args.expected_ref,
        )
    except ValueError as exc:
        print(f"resident drift comparison failed: {exc}", file=sys.stderr)
        return 1
    print(f"resident health valid: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
