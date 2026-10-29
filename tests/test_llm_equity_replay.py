from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from llm_equity.replay import (
    PointInTimeEventStore,
    ReplayConfig,
    ReplayInputs,
    ReplayIntent,
    ReplayObservation,
    bootstrap_mean_confidence_interval,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    purged_walk_forward_splits,
    replay_inputs_from_event_store,
    run_replay,
)


def at(minutes: int) -> datetime:
    return datetime(2026, 5, 28, 14, 30, tzinfo=UTC) + timedelta(minutes=minutes)


def observation(minutes: int, symbol: str, mid: float) -> ReplayObservation:
    return ReplayObservation(
        as_of=at(minutes),
        symbol=symbol,
        bid=mid - 0.05,
        ask=mid + 0.05,
        session_open=100.0,
        volume=1_000_000.0,
        shortable=True,
    )


def _observation_payload(as_of: datetime, symbol: str, mid: float) -> dict[str, object]:
    return {
        "as_of": as_of.isoformat().replace("+00:00", "Z"),
        "symbol": symbol,
        "bid": mid - 0.05,
        "ask": mid + 0.05,
        "session_open": 100.0,
        "volume": 1_000_000.0,
        "shortable": True,
    }


def test_point_in_time_event_store_is_hash_chained_and_detects_tampering(
    tmp_path: Path,
) -> None:
    path = tmp_path / "research-events.jsonl"
    store = PointInTimeEventStore(path)

    first = store.append("observation", at(0), {"symbol": "SPY", "mid": 100.0})
    second = store.append("intent", at(1), {"candidate_id": "candidate-1-SPY-long"})

    assert second["previous_hash"] == first["event_hash"]
    assert [row["event_type"] for row in store.read_all()] == ["observation", "intent"]

    rows = path.read_text(encoding="utf-8").splitlines()
    tampered = json.loads(rows[0])
    tampered["payload"]["mid"] = 999.0
    rows[0] = json.dumps(tampered, sort_keys=True)
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="payload hash mismatch"):
        store.read_all()


def test_replay_isolates_horizons_and_charges_execution_model_and_borrow_costs() -> None:
    observations = [
        observation(0, "SPY", 100.0),
        observation(30, "SPY", 102.0),
        observation(60, "SPY", 103.0),
        observation(0, "QQQ", 100.0),
        observation(30, "QQQ", 98.0),
        observation(60, "QQQ", 97.0),
    ]
    intents = [
        ReplayIntent(
            decision_id="decision-1",
            as_of=at(0),
            candidate_id="candidate-1-SPY-long",
            symbol="SPY",
            direction="long",
            horizon="30m",
            target_weight=0.05,
            confidence=0.75,
            model_cost_usd=2.0,
        ),
        ReplayIntent(
            decision_id="decision-1",
            as_of=at(0),
            candidate_id="candidate-2-QQQ-short",
            symbol="QQQ",
            direction="short",
            horizon="60m",
            target_weight=-0.05,
            confidence=0.72,
            model_cost_usd=2.0,
        ),
    ]

    report = run_replay(
        observations,
        intents,
        config=ReplayConfig(
            initial_cash_usd=10_000.0,
            commission_bps=1.0,
            base_impact_bps=2.0,
            borrow_apr=0.10,
        ),
    )

    assert set(report.sleeves) == {"30m", "60m"}
    long_trade = report.sleeves["30m"].trades[0]
    short_trade = report.sleeves["60m"].trades[0]
    assert long_trade.gross_pnl_usd > long_trade.net_pnl_usd > 0.0
    assert short_trade.borrow_cost_usd > 0.0
    assert short_trade.gross_pnl_usd > short_trade.net_pnl_usd > 0.0
    assert long_trade.model_cost_usd == pytest.approx(1.0)
    assert short_trade.model_cost_usd == pytest.approx(1.0)


def test_replay_uses_authoritative_fill_dollars_and_one_consolidated_nav_curve() -> None:
    observations = [
        observation(0, "SPY", 100.0),
        observation(30, "SPY", 95.0),
        observation(30, "QQQ", 100.0),
        observation(90, "QQQ", 108.0),
    ]
    intents = [
        ReplayIntent(
            decision_id="decision-1",
            as_of=at(0),
            candidate_id="candidate-1-SPY-long",
            symbol="SPY",
            direction="long",
            horizon="30m",
            target_weight=0.01,
            confidence=0.7,
            model_cost_usd=0.0,
            notional_usd=700.0,
            entry_fill_price=100.07,
            executed_quantity=700.0 / 100.07,
        ),
        ReplayIntent(
            decision_id="decision-2",
            as_of=at(30),
            candidate_id="candidate-1-QQQ-long",
            symbol="QQQ",
            direction="long",
            horizon="60m",
            target_weight=0.01,
            confidence=0.7,
            model_cost_usd=0.0,
            notional_usd=900.0,
            entry_fill_price=100.07,
            executed_quantity=900.0 / 100.07,
        ),
    ]

    report = run_replay(observations, intents)

    assert report.sleeves["30m"].trades[0].notional_usd == pytest.approx(700.0)
    assert report.sleeves["60m"].trades[0].notional_usd == pytest.approx(900.0)
    assert report.ending_nav_usd == pytest.approx(10_000.0 + report.total_net_pnl_usd)
    assert report.max_drawdown_pct > 0.0


def test_replay_skips_trade_without_point_in_time_exit_mark() -> None:
    report = run_replay(
        [observation(0, "SPY", 100.0)],
        [
            ReplayIntent(
                decision_id="decision-1",
                as_of=at(0),
                candidate_id="candidate-1-SPY-long",
                symbol="SPY",
                direction="long",
                horizon="60m",
                target_weight=0.05,
                confidence=0.7,
                model_cost_usd=1.0,
            )
        ],
    )

    assert report.sleeves["60m"].trades == []
    assert report.sleeves["60m"].skipped_intents == 1
    assert report.sleeves["60m"].model_cost_usd == pytest.approx(1.0)
    assert report.sleeves["60m"].skipped_model_cost_usd == pytest.approx(1.0)
    assert report.sleeves["60m"].net_pnl_usd == pytest.approx(-1.0)
    assert report.sleeves["60m"].ending_nav_usd == pytest.approx(9_999.0)


def test_replay_rejects_intraday_exit_mark_beyond_sampling_tolerance() -> None:
    report = run_replay(
        [observation(0, "SPY", 100.0), observation(120, "SPY", 110.0)],
        [
            ReplayIntent(
                decision_id="decision-1",
                as_of=at(0),
                candidate_id="candidate-1-SPY-long",
                symbol="SPY",
                direction="long",
                horizon="30m",
                target_weight=0.05,
                confidence=0.7,
                model_cost_usd=1.0,
            )
        ],
    )

    assert report.sleeves["30m"].trades == []
    assert report.sleeves["30m"].skipped_model_cost_usd == pytest.approx(1.0)


def test_replay_close_uses_last_regular_session_mark() -> None:
    report = run_replay(
        [
            observation(0, "SPY", 100.0),
            observation(330, "SPY", 102.0),
            observation(360, "SPY", 120.0),
        ],
        [
            ReplayIntent(
                decision_id="decision-1",
                as_of=at(0),
                candidate_id="candidate-1-SPY-long",
                symbol="SPY",
                direction="long",
                horizon="close",
                target_weight=0.05,
                confidence=0.7,
                model_cost_usd=1.0,
            )
        ],
    )

    assert report.sleeves["close"].trades[0].exit_at == at(330)
    assert report.sleeves["close"].trades[0].exit_mid == pytest.approx(102.0)


def test_replay_does_not_drop_skipped_intent_share_of_decision_cost() -> None:
    report = run_replay(
        [observation(0, "SPY", 100.0), observation(30, "SPY", 101.0)],
        [
            ReplayIntent(
                decision_id="decision-1",
                as_of=at(0),
                candidate_id="candidate-1-SPY-long",
                symbol="SPY",
                direction="long",
                horizon="30m",
                target_weight=0.05,
                confidence=0.7,
                model_cost_usd=2.0,
            ),
            ReplayIntent(
                decision_id="decision-1",
                as_of=at(0),
                candidate_id="candidate-2-QQQ-long",
                symbol="QQQ",
                direction="long",
                horizon="30m",
                target_weight=0.05,
                confidence=0.7,
                model_cost_usd=2.0,
            ),
        ],
    )

    sleeve = report.sleeves["30m"]
    assert len(sleeve.trades) == 1
    assert sleeve.trades[0].model_cost_usd == pytest.approx(1.0)
    assert sleeve.model_cost_usd == pytest.approx(2.0)
    assert sleeve.skipped_model_cost_usd == pytest.approx(1.0)
    assert sleeve.net_pnl_usd == pytest.approx(
        sleeve.trades[0].net_pnl_usd - sleeve.skipped_model_cost_usd
    )


def test_replay_rejects_inconsistent_cost_for_same_decision() -> None:
    intents = [
        ReplayIntent(
            decision_id="decision-1",
            as_of=at(0),
            candidate_id=f"candidate-{index}-SPY-long",
            symbol="SPY",
            direction="long",
            horizon="30m",
            target_weight=0.05,
            confidence=0.7,
            model_cost_usd=cost,
        )
        for index, cost in ((1, 1.0), (2, 2.0))
    ]

    with pytest.raises(ValueError, match="model cost must be consistent"):
        run_replay([observation(0, "SPY", 100.0), observation(30, "SPY", 101.0)], intents)


def test_purged_walk_forward_splits_leave_purge_and_embargo_gaps() -> None:
    splits = list(
        purged_walk_forward_splits(
            sample_count=50,
            train_size=10,
            test_size=5,
            purge_size=2,
            embargo_size=3,
        )
    )

    assert splits[0].train_indices == tuple(range(10))
    assert splits[0].test_indices == tuple(range(12, 17))
    assert splits[1].train_indices[-1] < splits[1].test_indices[0] - 1
    assert splits[1].train_indices[0] >= 20


def test_research_statistics_penalize_multiple_trials_and_bound_mean() -> None:
    returns = [0.012, 0.009, 0.015, -0.002, 0.011, 0.008, 0.014, 0.006] * 8

    psr = probabilistic_sharpe_ratio(returns)
    dsr_one = deflated_sharpe_ratio(returns, trial_count=1)
    dsr_many = deflated_sharpe_ratio(returns, trial_count=100)
    lower, upper = bootstrap_mean_confidence_interval(returns, seed=7)

    assert psr > 0.95
    assert dsr_one == pytest.approx(psr)
    assert 0.0 <= dsr_many < dsr_one
    assert 0.0 < lower < upper


def test_replay_report_charges_model_cost_for_abstention_without_intents() -> None:
    report = run_replay([], [], unattributed_model_cost_usd=3.0)

    assert report.sleeves == {}
    assert report.total_model_cost_usd == pytest.approx(3.0)
    assert report.unattributed_model_cost_usd == pytest.approx(3.0)
    assert report.total_net_pnl_usd == pytest.approx(-3.0)


def test_replay_inputs_load_sanitized_cycle_pack_from_hash_chain(tmp_path: Path) -> None:
    store = PointInTimeEventStore(tmp_path / "research-events.jsonl")
    store.append(
        "replay_cycle",
        at(0),
        {
            "schema_version": 1,
            "decision_id": "decision-1",
            "llm_invoked": True,
            "model_call_cost_usd": 2.0,
            "observations": [
                {
                    "as_of": at(0).isoformat().replace("+00:00", "Z"),
                    "symbol": "SPY",
                    "bid": 99.95,
                    "ask": 100.05,
                    "session_open": 99.0,
                    "volume": 1_000_000.0,
                    "shortable": True,
                }
            ],
            "intents": [
                {
                    "candidate_id": "candidate-1-SPY-long",
                    "symbol": "SPY",
                    "direction": "long",
                    "horizon": "60m",
                    "target_weight": 0.01,
                    "confidence": 0.75,
                }
            ],
            "order_results": [],
        },
    )

    inputs = replay_inputs_from_event_store(store)

    assert isinstance(inputs, ReplayInputs)
    assert inputs.unattributed_model_cost_usd == 0.0
    assert inputs.observations[0].symbol == "SPY"
    assert inputs.intents[0].model_cost_usd == pytest.approx(2.0)


def test_replay_inputs_retain_authoritative_v3_fill_fields(tmp_path: Path) -> None:
    store = PointInTimeEventStore(tmp_path / "research-events.jsonl")
    store.append(
        "replay_cycle",
        at(0),
        {
            "schema_version": 3,
            "decision_id": "decision-1",
            "as_of": at(0).isoformat().replace("+00:00", "Z"),
            "llm_invoked": True,
            "model_call_cost_usd": 0.0,
            "model_cost_basis": "subscription_zero",
            "imputed_api_cost_usd": 0.0,
            "cost_disposition": "direct",
            "observations": [_observation_payload(at(0), "SPY", 100.0)],
            "intents": [
                {
                    "candidate_id": "candidate-1-SPY-long",
                    "symbol": "SPY",
                    "direction": "long",
                    "horizon": "60m",
                    "target_weight": 0.01,
                    "confidence": 0.75,
                    "notional_usd": 125.0,
                    "entry_fill_price": 100.07,
                    "executed_quantity": 1.249125,
                }
            ],
            "order_results": [],
        },
    )

    intent = replay_inputs_from_event_store(store).intents[0]

    assert intent.notional_usd == pytest.approx(125.0)
    assert intent.entry_fill_price == pytest.approx(100.07)
    assert intent.executed_quantity == pytest.approx(1.249125)


@pytest.mark.parametrize("schema_version", [2, 3])
def test_replay_inputs_load_forecast_attribution_and_imputed_cost(
    tmp_path: Path, schema_version: int
) -> None:
    store = PointInTimeEventStore(tmp_path / "research-events.jsonl")
    store.append(
        "replay_cycle",
        at(0),
        {
            "schema_version": schema_version,
            "strategy_epoch_id": "epoch-0123456789abcdef",
            "decision_id": "decision-forecast",
            "llm_invoked": True,
            "model_call_cost_usd": 0.0,
            "model_cost_basis": "subscription_zero",
            "imputed_api_cost_usd": 3.5,
            "cost_disposition": "direct",
            "as_of": at(0).isoformat().replace("+00:00", "Z"),
            "observations": [_observation_payload(at(0), "SPY", 100.0)],
            "intents": [
                {
                    "candidate_id": "candidate-1-SPY-long",
                    "symbol": "SPY",
                    "direction": "long",
                    "horizon": "60m",
                    "target_weight": 0.01,
                    "confidence": 0.75,
                    "edge_class": "positive_edge",
                    "expected_excess_return_bps": 20.0,
                    "probability_positive": 0.6,
                }
            ],
            "order_results": [],
        },
    )

    inputs = replay_inputs_from_event_store(store)

    assert inputs.total_imputed_api_cost_usd == 3.5
    assert inputs.intents[0].model_cost_usd == 0.0
    assert inputs.intents[0].edge_class == "positive_edge"
    assert inputs.intents[0].expected_excess_return_bps == 20.0


def test_replay_inputs_link_preparation_cost_to_released_intent(tmp_path: Path) -> None:
    store = PointInTimeEventStore(tmp_path / "research-events.jsonl")
    base_payload = {
        "schema_version": 1,
        "decision_id": "prepared-decision-1",
        "llm_invoked": True,
        "model_call_cost_usd": 2.0,
        "cost_disposition": "deferred",
        "observations": [],
        "intents": [],
        "order_results": [],
    }
    store.append("replay_cycle", at(0), base_payload)
    store.append(
        "replay_cycle",
        at(30),
        {
            **base_payload,
            "llm_invoked": False,
            "model_call_cost_usd": 0.0,
            "cost_disposition": "linked",
            "intents": [
                {
                    "candidate_id": "candidate-1-SPY-long",
                    "symbol": "SPY",
                    "direction": "long",
                    "horizon": "60m",
                    "target_weight": 0.01,
                    "confidence": 0.75,
                }
            ],
        },
    )

    inputs = replay_inputs_from_event_store(store)

    assert inputs.unattributed_model_cost_usd == 0.0
    assert inputs.intents[0].model_cost_usd == pytest.approx(2.0)


def test_replay_inputs_reject_link_without_preparation_cost(tmp_path: Path) -> None:
    store = PointInTimeEventStore(tmp_path / "research-events.jsonl")
    store.append(
        "replay_cycle",
        at(0),
        {
            "schema_version": 1,
            "decision_id": "missing-preparation",
            "llm_invoked": False,
            "model_call_cost_usd": 0.0,
            "cost_disposition": "linked",
            "observations": [],
            "intents": [],
            "order_results": [],
        },
    )

    with pytest.raises(ValueError, match="missing its deferred preparation"):
        replay_inputs_from_event_store(store)
