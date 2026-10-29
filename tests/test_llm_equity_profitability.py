from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from llm_equity.hourly_service import main
from llm_equity.profitability import build_profitability_report, profitability_report_text
from llm_equity.replay import PointInTimeEventStore

from llm_equity import hourly_service

EPOCH_A = "epoch-1111111111111111"
EPOCH_B = "epoch-2222222222222222"


def test_owner_excluded_overhead_remains_explicit_in_reconciled_report(tmp_path):
    from test_llm_equity_accounting import _state, _write
    epoch = _initialize_epoch(tmp_path, EPOCH_A)
    _write(epoch, [_state('2026-05-28T14:30:00Z', 10000, 0, 100, [])])
    (epoch / 'economic-cost-policy.json').write_text(json.dumps({
        'commission_bps': .5, 'borrow_apr': .05, 'financing_apr': .03,
        'monthly_model_usd': 0, 'monthly_data_usd': 0, 'monthly_hosting_usd': 0,
        'exclude_operating_costs': True}))
    report = build_profitability_report(tmp_path)
    assert report['model_cost_integrity']['status'] == 'excluded_by_owner'
    assert report['account']['economic']['net_pnl_usd'] == 0
    assert 'account_pnl_usd=0.00' in '\n'.join(profitability_report_text(report))


def _timestamp(minutes: int) -> datetime:
    return datetime(2026, 5, 28, 14, 30, tzinfo=UTC) + timedelta(minutes=minutes)


def _observation(minutes: int, symbol: str, mid: float) -> dict[str, object]:
    return {
        "as_of": _timestamp(minutes).isoformat().replace("+00:00", "Z"),
        "symbol": symbol,
        "bid": mid - 0.05,
        "ask": mid + 0.05,
        "session_open": 100.0,
        "volume": 1_000_000.0,
        "shortable": True,
    }


def _initialize_epoch(state_dir: Path, epoch_id: str, *, latest: bool = True) -> Path:
    descriptor = {
        "schema_version": 1,
        "epoch_id": epoch_id,
        "fingerprint": epoch_id.removeprefix("epoch-") * 4,
        "components": {"model": "gpt-5.6-sol", "reasoning_effort": "xhigh"},
    }
    epoch_dir = state_dir / "epochs" / epoch_id
    epoch_dir.mkdir(parents=True)
    (epoch_dir / "strategy-epoch.json").write_text(json.dumps(descriptor), encoding="utf-8")
    if latest:
        (state_dir / "strategy-epoch.json").write_text(json.dumps(descriptor), encoding="utf-8")
    return epoch_dir


def _write_profitable_epoch(
    state_dir: Path,
    epoch_id: str,
    *,
    latest: bool = True,
    imputed_api_cost_usd: float = 2.0,
) -> None:
    epoch_dir = _initialize_epoch(state_dir, epoch_id, latest=latest)
    store = PointInTimeEventStore(epoch_dir / "research-events.jsonl")
    start = _timestamp(0).isoformat().replace("+00:00", "Z")
    store.append(
        "replay_cycle",
        _timestamp(0),
        {
            "schema_version": 3,
            "strategy_epoch_id": epoch_id,
            "decision_id": "decision-1",
            "as_of": start,
            "llm_invoked": True,
            "model_call_cost_usd": 0.0,
            "model_cost_basis": "subscription_zero",
            "imputed_api_cost_usd": imputed_api_cost_usd,
            "cost_disposition": "direct",
            "observations": [
                _observation(0, "SPY", 100.0),
                _observation(0, "QQQ", 100.0),
                _observation(0, "AAPL", 100.0),
                _observation(0, "MSFT", 100.0),
            ],
            "intents": [
                {
                    "candidate_id": "candidate-1-AAPL-long",
                    "symbol": "AAPL",
                    "direction": "long",
                    "horizon": "60m",
                    "target_weight": 0.04,
                    "confidence": 0.65,
                    "edge_class": "positive_edge",
                    "expected_excess_return_bps": 40.0,
                    "probability_positive": 0.62,
                },
                {
                    "candidate_id": "candidate-2-MSFT-short",
                    "symbol": "MSFT",
                    "direction": "short",
                    "horizon": "60m",
                    "target_weight": -0.04,
                    "confidence": 0.64,
                    "edge_class": "least_bad_forced",
                    "expected_excess_return_bps": -2.0,
                    "probability_positive": 0.49,
                },
            ],
            "order_results": [],
        },
    )
    store.append(
        "replay_cycle",
        _timestamp(60),
        {
            "schema_version": 2,
            "decision_id": "mark-1",
            "as_of": _timestamp(60).isoformat().replace("+00:00", "Z"),
            "llm_invoked": False,
            "model_call_cost_usd": 0.0,
            "cost_disposition": "none",
            "observations": [
                _observation(60, "SPY", 100.1),
                _observation(60, "QQQ", 100.2),
                _observation(60, "AAPL", 108.0),
                _observation(60, "MSFT", 92.0),
            ],
            "intents": [],
            "order_results": [],
        },
    )
    audit = [
        {
            "cycle_id": "decision-1",
            "created_at": start,
            "as_of": start,
            "market_session": "rth",
            "mandatory_trade_required": True,
            "mandatory_trade_status": "accepted",
            "llm_invoked": True,
            "source_health_reasons": [],
            "costs": {
                "monthly_data_subscriptions_usd": 30.0,
                "monthly_hosting_usd": 30.0,
                "monthly_safety_margin_usd": 30.0,
            },
        }
    ]
    (epoch_dir / "cycle-audit-ledger.json").write_text(json.dumps(audit), encoding="utf-8")


def test_profitability_report_accounts_for_costs_benchmarks_and_attribution(
    tmp_path: Path,
) -> None:
    _write_profitable_epoch(tmp_path, EPOCH_A)

    report = build_profitability_report(tmp_path, epoch="latest")
    full = report["windows"]["full_epoch"]

    assert report["strategy_epoch_id"] == EPOCH_A
    assert full["net_pnl_usd"] > 0.0
    assert full["return_pct"] > full["benchmarks"]["SPY"]["return_pct"]
    assert full["return_pct"] > full["benchmarks"]["QQQ"]["return_pct"]
    assert full["costs"]["spread_usd"] > 0.0
    assert full["costs"]["impact_usd"] > 0.0
    assert full["costs"]["commission_usd"] > 0.0
    assert full["costs"]["borrow_usd"] > 0.0
    assert full["costs"]["financing_usd"] > 0.0
    assert full["costs"]["configured_fixed_usd"] > 0.0
    assert full["costs"]["model_usd"] == 0.0
    assert full["costs"]["imputed_api_cost_usd"] == 2.0
    assert set(full["attribution"]["side"]) == {"long", "short"}
    assert set(full["attribution"]["edge_class"]) == {
        "positive_edge",
        "least_bad_forced",
    }
    # A profitable horizon replay cannot certify a real account without its receipts.
    assert report["success_thresholds"]["account_reconciled_and_costed"] is False
    assert report["success_thresholds"]["positive_absolute_net_pnl"] is False
    assert "equal_weight_benchmark" in report["promotion_diagnostics_only"]


def test_profitability_report_rejects_unknown_model_cost_for_promotion(
    tmp_path: Path,
) -> None:
    _write_profitable_epoch(tmp_path, EPOCH_A, imputed_api_cost_usd=0.0)

    report = build_profitability_report(tmp_path, epoch="latest")

    assert report["model_cost_integrity"]["status"] == "cost_unknown"
    assert report["model_cost_integrity"]["promotion_eligible"] is False
    assert report["success_thresholds"]["known_actual_or_imputed_model_cost"] is False


def test_profitability_report_filters_epochs_and_flags_hard_gate_violations(
    tmp_path: Path,
) -> None:
    _write_profitable_epoch(tmp_path, EPOCH_A, latest=False)
    _write_profitable_epoch(tmp_path, EPOCH_B, latest=True)
    epoch_b_dir = tmp_path / "epochs" / EPOCH_B
    audit = json.loads((epoch_b_dir / "cycle-audit-ledger.json").read_text(encoding="utf-8"))
    audit[0]["market_session"] = "closed"
    audit[0]["risk_mode"] = "forced_aggressive"
    audit[0]["gross_exposure_pct"] = 1.3
    (epoch_b_dir / "cycle-audit-ledger.json").write_text(json.dumps(audit), encoding="utf-8")

    explicit = build_profitability_report(tmp_path, epoch=EPOCH_A)
    latest = build_profitability_report(tmp_path, epoch="latest")

    assert explicit["strategy_epoch_id"] == EPOCH_A
    assert explicit["hard_gate_violations"] == []
    assert latest["strategy_epoch_id"] == EPOCH_B
    assert latest["hard_gate_violations"][0]["market_session"] == "closed"
    assert latest["risk_envelope_breaches"][0]["exceeded"]["gross_exposure_pct"] == 1.3
    assert latest["success_thresholds"]["zero_hard_gate_violations"] is False
    assert latest["success_thresholds"]["zero_risk_envelope_breaches"] is False


def test_profitability_report_surfaces_blocked_rth_cycles_without_calling_them_fills(
    tmp_path: Path,
) -> None:
    _write_profitable_epoch(tmp_path, EPOCH_A)
    epoch_dir = tmp_path / "epochs" / EPOCH_A
    audit = json.loads((epoch_dir / "cycle-audit-ledger.json").read_text(encoding="utf-8"))
    audit.append(
        {
            "cycle_id": "stale-news-1",
            "cycle_kind": "news_scan",
            "created_at": "2026-01-02T15:05:11Z",
            "as_of": "2026-01-02T15:05:00Z",
            "market_session": "rth",
            "status": "skipped_stale",
            "edge_gate_status": "stale_context",
            "mandatory_trade_required": False,
            "mandatory_trade_status": "not_applicable",
            "llm_invoked": False,
            "source_health_reasons": ["stale:alpaca_market_data"],
        }
    )
    audit.append(
        {
            "cycle_id": "crossed-close-1",
            "cycle_kind": "news_scan",
            "created_at": "2026-01-02T21:00:11Z",
            "as_of": "2026-01-02T21:00:11Z",
            "market_session": "after_hours",
            "status": "research_only",
            "edge_gate_status": "skipped_non_rth",
            "mandatory_trade_required": False,
            "mandatory_trade_status": "not_applicable",
            "llm_invoked": False,
            "source_health_reasons": [],
        }
    )
    (epoch_dir / "cycle-audit-ledger.json").write_text(json.dumps(audit), encoding="utf-8")

    report = build_profitability_report(tmp_path, epoch="latest")
    accounting = report["forced_cycle_accounting"]

    assert accounting["eligible_cycles"] == 1
    assert accounting["sol_invocation_rate"] == 1.0
    assert accounting["observed_rth_cycles"] == 2
    assert accounting["hard_gate_blocked_rth_cycles"] == 1
    assert accounting["rth_sol_invocation_rate"] == 0.5
    assert report["hard_gate_blocks"] == [
        {
            "cycle_id": "stale-news-1",
            "cycle_kind": "news_scan",
            "status": "skipped_stale",
            "source_health_reasons": ["stale:alpaca_market_data"],
        }
    ]
    assert report["hard_gate_violations"] == []
    assert "hard_gate_blocked=1" in "\n".join(profitability_report_text(report))


def test_profitability_report_uses_production_audit_risk_metrics_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_RISK_MODE", "forced_aggressive")
    response = {
        "status": "success",
        "portfolio_nav_usd": 9_900.0,
        "total_pnl_usd": -100.0,
        "gross_exposure_pct": 1.30,
        "net_exposure_abs_pct": 0.70,
        "short_gross_exposure_pct": 0.50,
        "daily_turnover_pct": 3.0,
        "daily_loss_pct": 0.01,
        "max_drawdown_pct": 0.02,
        "accepted_orders": 1,
        "rejected_orders": 0,
        "order_results": [
            {
                "command_id": "risk-breach-1",
                "symbol": "SPY",
                "side": "buy",
                "status": "accepted",
                "notional_usd": 100.0,
                "quantity": 0.2,
                "avg_price": 500.0,
            }
        ],
    }
    hourly_service.record_cycle_audit(
        tmp_path,
        cycle_kind="run_once",
        status="posted",
        snapshot={"as_of": "2026-05-28T14:30:00Z", "market_session": "rth", "symbols": []},
        decision={
            "cycle_id": "risk-breach",
            "commands": [
                {
                    "action": "buy_notional",
                    "symbol": "SPY",
                    "max_notional_usd": 100.0,
                    "horizon": "60m",
                }
            ],
        },
        rust_response=response,
        mandatory_trade={
            "mandatory_trade_required": True,
            "mandatory_trade_status": "accepted",
            "mandatory_required_total_notional_usd": 100.0,
            "mandatory_accepted_total_notional_usd": 100.0,
        },
    )

    report = build_profitability_report(tmp_path, epoch="latest")

    assert report["risk_envelope_breaches"][0]["exceeded"] == {
        "gross_exposure_pct": 1.30
    }
    assert report["success_thresholds"]["zero_risk_envelope_breaches"] is False

    ledger_path = hourly_service.cycle_audit_ledger_path(tmp_path)
    rows = json.loads(ledger_path.read_text(encoding="utf-8"))
    for key in (
        "gross_exposure_pct",
        "net_exposure_abs_pct",
        "short_gross_exposure_pct",
        "daily_turnover_pct",
        "daily_loss_pct",
        "max_drawdown_pct",
    ):
        rows[0].pop(key, None)
    ledger_path.write_text(json.dumps(rows), encoding="utf-8")

    # The capped summary cannot override immutable audit evidence.
    from llm_equity.profitability import _risk_envelope_breaches

    assert build_profitability_report(tmp_path)["risk_envelope_breaches"][0]["exceeded"]
    missing = _risk_envelope_breaches(rows)[0]
    assert set(missing["missing_metrics"]) == {
        "gross_exposure_pct",
        "net_exposure_abs_pct",
        "short_gross_exposure_pct",
        "daily_turnover_pct",
        "daily_loss_pct",
        "max_drawdown_pct",
    }


def test_profitability_report_cli_supports_text_and_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_profitable_epoch(tmp_path, EPOCH_A)

    assert main(
        [
            "profitability-report",
            "--state-dir",
            str(tmp_path),
            "--epoch",
            "latest",
            "--format",
            "json",
        ]
    ) == 0
    output = capsys.readouterr().out
    assert json.loads(output)["strategy_epoch_id"] == EPOCH_A

    assert main(
        [
            "profitability-report",
            "--state-dir",
            str(tmp_path),
            "--epoch",
            EPOCH_A,
            "--format",
            "text",
        ]
    ) == 0
    assert "full_epoch:" in capsys.readouterr().out


def test_historical_cutoff_excludes_future_forecast_maturity(tmp_path):
    _write_profitable_epoch(tmp_path, EPOCH_A)
    epoch = tmp_path / 'epochs' / EPOCH_A
    (epoch / 'profit-trial-registry.json').write_text(json.dumps({'forward_start': '2026-05-01'}))
    outcomes = PointInTimeEventStore(epoch / 'forecast-outcomes.jsonl')
    for i, at in enumerate((_timestamp(30), _timestamp(120))):
        outcomes.append('entry_forecast_settled', at, {'cycle_id': str(i), 'command_index': 1,
            'opened_at': _timestamp(0).isoformat(), 'settled_at': at.isoformat()})
    report = build_profitability_report(tmp_path, end=_timestamp(60).isoformat())
    assert report['forward_promotion']['matured_entry_forecasts'] == 1
