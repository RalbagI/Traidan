from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL_DRY_RUN = ROOT / "scripts/local-dry-run.sh"
README = ROOT / "README.md"
ARCHITECTURE = ROOT / "architecture.md"
OPERATOR_RUNBOOK = ROOT / "deployment/operator-runbook.md"


def test_local_dry_run_defaults_to_fully_invested_paper_fills() -> None:
    script = LOCAL_DRY_RUN.read_text(encoding="utf-8")

    assert 'DEFAULT_LLM_EQUITY_FILL_MODE="${TRAIDAN_LLM_EQUITY_FILL_MODE_DEFAULT:-paper}"' in script
    assert (
        'DEFAULT_LLM_EQUITY_RISK_MODE="${TRAIDAN_LLM_EQUITY_RISK_MODE_DEFAULT:-fully_invested}"'
        in script
    )
    assert (
        script.count(
            'TRAIDAN_LLM_EQUITY_FILL_MODE="${TRAIDAN_LLM_EQUITY_FILL_MODE:-$DEFAULT_LLM_EQUITY_FILL_MODE}"'
        )
        == 2
    )
    assert 'echo "llm_equity_fill_mode=${fill_mode:-$DEFAULT_LLM_EQUITY_FILL_MODE}"' in script
    assert 'echo "llm_equity_risk_mode=${risk_mode:-$DEFAULT_LLM_EQUITY_RISK_MODE}"' in script
    assert (
        'echo "llm_equity_least_bad_forced=${least_bad_forced:-$DEFAULT_ALLOW_LEAST_BAD_FORCED}"'
        in script
    )
    assert (
        'DEFAULT_ALLOW_LEAST_BAD_FORCED="${TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED_DEFAULT:-0}"'
        in script
    )
    assert (
        script.count(
            'TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED="${TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED:-$DEFAULT_ALLOW_LEAST_BAD_FORCED}"'
        )
        == 2
    )
    assert 'CI_COMMIT_SHA="$(git rev-parse HEAD)" cargo build --bin traidan' in script


def test_fully_invested_default_and_bounded_direct_start_are_documented() -> None:
    readme = README.read_text(encoding="utf-8")
    architecture = ARCHITECTURE.read_text(encoding="utf-8")
    operator_runbook = OPERATOR_RUNBOOK.read_text(encoding="utf-8")

    assert "launcher defaults to `paper`" in readme
    assert "TRAIDAN_LLM_EQUITY_RISK_MODE=fully_invested" in readme
    assert "launcher defaults to `paper` plus" in architecture
    assert "TRAIDAN_LLM_EQUITY_RISK_MODE=fully_invested" in operator_runbook


def test_learning_v3_and_resident_health_policy_are_documented() -> None:
    architecture = ARCHITECTURE.read_text(encoding="utf-8")

    assert "strategy-learning.json` schema v3" in architecture
    assert "strategy-learning.v2.archive.json" in architecture
    assert "documentation-only drift" in architecture
    assert "paper + profit_driven" in architecture


def test_fixture_smoke_isolates_and_removes_all_mutable_trading_state() -> None:
    script = LOCAL_DRY_RUN.read_text(encoding="utf-8")

    assert 'smoke_state_root="$(mktemp -d "$STATE_DIR/smoke-state.XXXXXX")"' in script
    assert (
        'TRAIDAN_LLM_EQUITY_AUTHORITY_STATE_FILE="$smoke_state_root/rust-authority-state.json"'
        in script
    )
    assert 'TRAIDAN_LLM_EQUITY_STATE_DIR="$smoke_state_root/python"' in script
    assert 'fixture_result_file="$smoke_state_root/llm-equity-cycle.json"' in script
    assert "/tmp/traidan-llm-equity-cycle.json" not in script
    assert 'rm -rf -- "$smoke_state_root"' in script
    assert script.index('rm -rf -- "$smoke_state_root"') < script.index(
        'if [ "${TRAIDAN_SMOKE_RESTART_AFTER:-$restart_after}" = "1" ]'
    )


def test_fixture_smoke_subscribes_before_posting_the_fixture_cycle() -> None:
    script = LOCAL_DRY_RUN.read_text(encoding="utf-8")

    assert 'websocket_ready_file="$smoke_state_root/websocket-ready"' in script
    assert 'check_websocket "$websocket_ready_file" "$fixture_cycle_id_file" &' in script
    assert 'wait_for_websocket_ready "$websocket_ready_file"' in script
    assert script.index(
        'check_websocket "$websocket_ready_file" "$fixture_cycle_id_file" &'
    ) < script.index(
        'run_fixture_cycle "$fixture_cycle_id_file" "$fixture_result_file" || smoke_status=$?'
    )


def test_fixture_smoke_forces_rth_only_for_its_isolated_fixture_and_verifies_posting() -> None:
    script = LOCAL_DRY_RUN.read_text(encoding="utf-8")

    assert "export TRAIDAN_LLM_EQUITY_FILL_MODE=paper" in script
    assert "export TRAIDAN_LLM_EQUITY_RISK_MODE=forced_aggressive" in script
    assert "TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_RTH=1" in script
    assert 'if result.get("status") != "success":' in script
    assert script.index("export TRAIDAN_LLM_EQUITY_FILL_MODE=paper") < script.index(
        "start_all || smoke_status=$?"
    )


def test_fixture_smoke_requires_server_subscription_and_matching_cycle_frames() -> None:
    script = LOCAL_DRY_RUN.read_text(encoding="utf-8")

    assert 'fixture_cycle_id_file="$smoke_state_root/fixture-cycle-id"' in script
    assert 'run_fixture_cycle "$fixture_cycle_id_file"' in script
    assert 'FIXTURE_CYCLE_ID_FILE="$cycle_id_file"' in script
    assert "message.type === 'llm_equity_service_status'" in script
    assert "message.data?.cycle_id !== expectedCycleId" in script
    assert "setInterval(refreshExpectedCycleId, 100)" in script
    assert "armTimeout(15_000)" in script


def test_epoch_reset_is_recoverable_and_fail_closed() -> None:
    script = LOCAL_DRY_RUN.read_text(encoding="utf-8")

    assert "reset-epoch" in script
    assert 'status.get("live_order_capability_disabled") is not True' in script
    assert 'cp -a -- "$TRADING_STATE_DIR" "$archive_root/llm-equity-dry-run"' in script
    assert 'cp -a -- "$STATE_DIR"' not in script
    assert "sha256sum >SHA256SUMS" in script
    assert "sha256sum -c SHA256SUMS" in script
    assert 'mktemp -d "$TRADING_ARCHIVE_DIR/${archive_stamp}.XXXXXX"' in script
    assert 'refusing reset outside $allowed_state_root' in script
    assert 'echo "initial_cash_usd=10000"' in script
    assert 'echo "risk_mode=forced_aggressive"' in script
    assert 'echo "model_cost_basis=subscription_zero"' in script


def test_epoch_reset_is_repeatable_and_archives_verify(tmp_path: Path) -> None:
    trading_state = ROOT / ".agent" / "state" / f"pytest-reset-{uuid.uuid4().hex}"
    archive_dir = tmp_path / "archives"
    launcher_state = tmp_path / "launcher"
    env = {
        **os.environ,
        "TRAIDAN_LLM_EQUITY_STATE_DIR": str(trading_state),
        "TRAIDAN_LLM_EQUITY_ARCHIVE_DIR": str(archive_dir),
        "TRAIDAN_LOCAL_DRY_RUN_STATE_DIR": str(launcher_state),
    }
    try:
        trading_state.mkdir(parents=True)
        (trading_state / "marker.json").write_text(
            json.dumps({"portfolio": "paper-only"}), encoding="utf-8"
        )
        first = subprocess.run(
            [str(LOCAL_DRY_RUN), "reset-epoch"],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert first.returncode == 0, first.stderr
        (trading_state / "second-marker.json").write_text("{}\n", encoding="utf-8")
        second = subprocess.run(
            [str(LOCAL_DRY_RUN), "reset-epoch"],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert second.returncode == 0, second.stderr

        archives = sorted(path for path in archive_dir.iterdir() if path.is_dir())
        assert len(archives) == 2
        assert archives[0].name != archives[1].name
        for archive in archives:
            verified = subprocess.run(
                ["sha256sum", "-c", "SHA256SUMS"],
                cwd=archive,
                text=True,
                capture_output=True,
                check=False,
            )
            assert verified.returncode == 0, verified.stderr
        descriptor = json.loads((trading_state / "strategy-epoch.json").read_text())
        assert descriptor["epoch_id"].startswith("epoch-")
    finally:
        shutil.rmtree(trading_state, ignore_errors=True)
