from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate-llm-resident-health.py"


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        text=True,
        capture_output=True,
        check=True,
    )
    return completed.stdout.strip()


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _status(
    root: Path,
    git_sha: str,
    *,
    rust_build_ref: str | None = None,
    scheduler_pid: int | None = None,
    **overrides: object,
) -> dict[str, object]:
    pid = os.getpid() if scheduler_pid is None else scheduler_pid
    status: dict[str, object] = {
        "run_mode": "dry_run",
        "live_order_capability_disabled": True,
        "llm_equity": {
            "fill_mode": "paper",
            "runtime_identity": {
                "rust_build_ref": rust_build_ref or git_sha,
                "service": {
                    "git_sha": git_sha,
                    "python_source_hash": _sha256(
                        root / "intelligence" / "llm_equity" / "hourly_service.py"
                    ),
                    "review_schema_hash": _sha256(
                        root / "schemas" / "llm_review_result_v3.schema.json"
                    ),
                    "scheduler_owner": f"resident:{pid}",
                    "fill_mode": "paper",
                    "risk_mode": "profit_driven",
                },
            },
        },
    }
    status.update(overrides)
    return status


def _repo_with_resident_commit(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "repo"
    (root / "intelligence" / "llm_equity").mkdir(parents=True)
    (root / "schemas").mkdir()
    (root / "config" / "market_universe").mkdir(parents=True)
    (root / "intelligence" / "llm_equity" / "hourly_service.py").write_text(
        "VERSION = 1\n", encoding="utf-8"
    )
    (root / "schemas" / "llm_review_result_v3.schema.json").write_text(
        '{"type":"object"}\n', encoding="utf-8"
    )
    (root / "config" / "market_universe" / "llm-equity-dry-run.json").write_text(
        '{"symbols":["SPY"]}\n', encoding="utf-8"
    )
    (root / "README.md").write_text("initial\n", encoding="utf-8")
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "Test User")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "initial")
    pid_dir = root / ".agent" / "state" / "local-dry-run" / "pids"
    pid_dir.mkdir(parents=True)
    (pid_dir / "llm-equity-service.pid").write_text(str(os.getpid()), encoding="utf-8")
    return root, _git(root, "rev-parse", "HEAD")


def _validate(root: Path, status: dict[str, object]) -> subprocess.CompletedProcess[str]:
    status_file = root / "runtime-health.json"
    status_file.write_text(json.dumps(status), encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--status-file",
            str(status_file),
            "--project-root",
            str(root),
            "--runtime-project-root",
            str(root),
            "--expected-ref",
            "HEAD",
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_resident_health_accepts_documentation_only_drift(tmp_path: Path) -> None:
    root, resident_sha = _repo_with_resident_commit(tmp_path)
    (root / "README.md").write_text("documentation update\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(root, "commit", "-m", "docs")

    completed = _validate(root, _status(root, resident_sha))

    assert completed.returncode == 0
    assert "non-runtime drift" in completed.stdout


def test_resident_health_blocks_runtime_critical_drift(tmp_path: Path) -> None:
    root, resident_sha = _repo_with_resident_commit(tmp_path)
    runtime = root / "intelligence" / "llm_equity" / "hourly_service.py"
    runtime.write_text("VERSION = 2\n", encoding="utf-8")
    _git(root, "add", str(runtime.relative_to(root)))
    _git(root, "commit", "-m", "runtime")

    completed = _validate(root, _status(root, resident_sha))

    assert completed.returncode == 1
    assert "runtime-critical drift" in completed.stderr
    assert "intelligence/llm_equity/hourly_service.py" in completed.stderr


def test_resident_health_requires_dry_run_live_disabled_profit_driven(
    tmp_path: Path,
) -> None:
    root, resident_sha = _repo_with_resident_commit(tmp_path)
    invalid_status = _status(root, resident_sha)
    invalid_status["run_mode"] = "live"
    invalid_status["live_order_capability_disabled"] = False
    invalid_status["llm_equity"] = {
        "fill_mode": "shadow",
        "runtime_identity": {
            "service": {
                "git_sha": resident_sha,
                "scheduler_owner": "pipeline:1234",
                "fill_mode": "shadow",
                "risk_mode": "bounded",
            }
        },
    }

    completed = _validate(root, invalid_status)

    assert completed.returncode == 1
    assert "run_mode must be dry_run" in completed.stderr
    assert "live_order_capability_disabled must be true" in completed.stderr
    assert "resident fill_mode must be paper" in completed.stderr
    assert "resident risk_mode must be profit_driven" in completed.stderr
    assert "scheduler_owner must start with resident:" in completed.stderr


def test_resident_health_blocks_independently_stale_rust_authority(tmp_path: Path) -> None:
    root, stale_rust_sha = _repo_with_resident_commit(tmp_path)
    runtime = root / "intelligence" / "llm_equity" / "hourly_service.py"
    runtime.write_text("VERSION = 2\n", encoding="utf-8")
    _git(root, "add", str(runtime.relative_to(root)))
    _git(root, "commit", "-m", "runtime")
    current_sha = _git(root, "rev-parse", "HEAD")

    completed = _validate(
        root,
        _status(root, current_sha, rust_build_ref=stale_rust_sha),
    )

    assert completed.returncode == 1
    assert "rust runtime-critical drift" in completed.stderr


def test_resident_health_blocks_dirty_runtime_source(tmp_path: Path) -> None:
    root, resident_sha = _repo_with_resident_commit(tmp_path)
    status = _status(root, resident_sha)
    runtime = root / "intelligence" / "llm_equity" / "hourly_service.py"
    runtime.write_text("VERSION = 2\n", encoding="utf-8")

    completed = _validate(root, status)

    assert completed.returncode == 1
    assert "dirty runtime-critical paths" in completed.stderr
    assert "intelligence/llm_equity/hourly_service.py" in completed.stderr
    assert "python_source_hash does not match" in completed.stderr


def test_resident_health_blocks_runtime_config_drift(tmp_path: Path) -> None:
    root, resident_sha = _repo_with_resident_commit(tmp_path)
    config = root / "config" / "market_universe" / "llm-equity-dry-run.json"
    config.write_text('{"symbols":["SPY","QQQ"]}\n', encoding="utf-8")
    _git(root, "add", str(config.relative_to(root)))
    _git(root, "commit", "-m", "config")

    completed = _validate(root, _status(root, resident_sha))

    assert completed.returncode == 1
    assert "config/market_universe/llm-equity-dry-run.json" in completed.stderr


def test_resident_health_requires_live_supervised_scheduler_pid(tmp_path: Path) -> None:
    root, resident_sha = _repo_with_resident_commit(tmp_path)
    dead_pid = 999_999_999
    pid_file = root / ".agent" / "state" / "local-dry-run" / "pids" / "llm-equity-service.pid"
    pid_file.write_text(str(dead_pid), encoding="utf-8")

    completed = _validate(
        root,
        _status(root, resident_sha, scheduler_pid=dead_pid),
    )

    assert completed.returncode == 1
    assert "resident scheduler process is not alive" in completed.stderr



def test_fully_invested_resident_validates_loaded_allocation_modules(tmp_path: Path) -> None:
    root, _ = _repo_with_resident_commit(tmp_path)
    (root / 'schemas/llm_portfolio_review_v1.schema.json').write_text('{}')
    for name in ('fully_invested.py', 'fully_invested_service.py'):
        (root / 'intelligence/llm_equity' / name).write_text('VERSION = 1\n')
    _git(root, 'add', 'schemas', 'intelligence')
    _git(root, 'commit', '-m', 'allocation')
    status = _status(root, _git(root, 'rev-parse', 'HEAD'))
    service = status['llm_equity']['runtime_identity']['service']
    service['risk_mode'] = 'fully_invested'
    service['review_schema_hash'] = _sha256(root / 'schemas/llm_portfolio_review_v1.schema.json')
    service['allocation_source_hashes'] = {name: _sha256(root / 'intelligence/llm_equity' / name)
                                         for name in ('fully_invested.py', 'fully_invested_service.py')}
    assert _validate(root, status).returncode == 0
    service['allocation_source_hashes']['fully_invested_service.py'] = 'sha256:' + '0'*64
    rejected = _validate(root, status)
    assert rejected.returncode == 1
    assert 'fully_invested_service.py' in rejected.stderr
