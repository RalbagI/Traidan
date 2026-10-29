"""Verify validation compatibility and safe diagnostics at external boundaries."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from llm_equity.contracts import load_equity_universe
from llm_equity.simulator import EquityPaperPortfolio, build_llm_context

from llm_equity import hourly_service as service
from llm_equity import profitability, replay, research_simulation

PRIVATE_DETAIL = "private-diagnostic-sentinel"


@pytest.mark.parametrize("environment_value", ["", "environment-fixture"])
def test_service_config_preserves_environment_and_file_credential_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, environment_value: str
) -> None:
    credential_file = tmp_path / "credential"
    credential_file.write_text("file-fixture\n")
    monkeypatch.setenv("TRAIDAN_ADMIN_TOKEN", environment_value)
    monkeypatch.setenv("TRAIDAN_ADMIN_TOKEN_FILE", str(credential_file))
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_RTH", "0")

    config = service.HourlyServiceConfig.from_env()

    assert config.admin_token == (environment_value or "file-fixture")


def assert_safe_log(caplog: pytest.LogCaptureFixture, operation: str) -> None:
    records = [record for record in caplog.records if record.name == service.__name__]
    assert records
    assert any(operation in record.getMessage() for record in records)
    assert all(PRIVATE_DETAIL not in record.getMessage() for record in records)
    assert all(not record.exc_info and record.exc_text is None for record in records)


@pytest.mark.parametrize("operation", ["post", "status", "model"])
def test_nonobject_service_responses_remain_runtime_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    monkeypatch.setattr(service.request, "urlopen", lambda *args, **kwargs: BytesIO(b"[]"))
    poster = service.LocalRustPoster("http://fixture.invalid", "fixture-credential")
    if operation != "model":
        with pytest.raises(RuntimeError, match="must be a JSON object"):
            poster.get_status() if operation == "status" else poster("/cycle", {})
        return

    universe = load_equity_universe()
    snapshot = service.build_market_research_snapshot(
        universe, datetime(2026, 5, 28, 14, 30, tzinfo=UTC), fixture_mode=True
    )
    context = build_llm_context(EquityPaperPortfolio.seeded(universe), snapshot, universe=universe)

    def fake_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        Path(command[command.index("--output-last-message") + 1]).write_text("[]")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(service.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="Codex LLM output must be a JSON object"):
        service.CodexLlmClient(tmp_path, timeout_seconds=10).decide(context)


@pytest.mark.parametrize(
    ("reader", "payload"),
    [(profitability._read_json_rows, {}), (profitability._read_json_object, [])],
)
def test_invalid_profitability_file_shapes_remain_value_errors(
    tmp_path: Path, reader: Callable[[Path], object], payload: object
) -> None:
    path = tmp_path / "report.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="must contain a JSON"):
        reader(path)


@pytest.mark.parametrize("row", [[], {"payload": []}])
def test_invalid_event_row_shapes_remain_value_errors(tmp_path: Path, row: object) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps(row) + "\n")
    with pytest.raises(ValueError, match="must be an object"):
        replay.PointInTimeEventStore(path).read_all()


@pytest.mark.parametrize(
    ("field", "value"),
    [("observations", {}), ("observations", [4]), ("intents", {}), ("intents", [4])],
)
def test_invalid_replay_collection_shapes_remain_value_errors(
    tmp_path: Path, field: str, value: object
) -> None:
    at = datetime(2026, 5, 28, 14, 30, tzinfo=UTC)
    store = replay.PointInTimeEventStore(tmp_path / "events.jsonl")
    store.append(
        event_type="replay_cycle",
        observed_at=at,
        payload={
            "schema_version": 1,
            "as_of": at.isoformat(),
            "decision_id": "test-decision",
            "model_call_cost_usd": 0.0,
            "observations": [],
            "intents": [],
            field: value,
        },
    )
    with pytest.raises(ValueError, match="must be an"):
        replay.replay_inputs_from_event_store(store)


@pytest.mark.parametrize(
    ("validator", "value"),
    [
        (research_simulation._finite_number, "bad"),
        (research_simulation._mapping, []),
        (research_simulation._sequence, {}),
    ],
)
def test_research_input_type_failures_remain_value_errors(
    validator: Callable[[object, str], object], value: object
) -> None:
    with pytest.raises(ValueError, match="input must be"):
        validator(value, "input")


@pytest.mark.parametrize(
    "operation", ["profitability", "status", "cycle_summary", "cycle", "settlement", "learning"]
)
def test_best_effort_failures_preserve_audits_without_logging_private_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    operation: str,
) -> None:
    fail = Mock(side_effect=RuntimeError(PRIVATE_DETAIL))
    config = service.HourlyServiceConfig(state_dir=tmp_path, fixture_mode=True)
    if operation == "profitability":
        monkeypatch.setattr(service, "build_profitability_report", fail)
        service.publish_profitability_report_safely(tmp_path)
        label = "profitability_report_publish"
    elif operation == "status":
        service.publish_service_status(config, fail, status="fixture", detail="fixture")
        label = "fixture_post"
    elif operation == "cycle_summary":
        monkeypatch.setenv("TRAIDAN_ADMIN_TOKEN", "fixture-credential")
        monkeypatch.setattr(service, "cycle_audit_dashboard_summary", lambda record: {})
        monkeypatch.setattr(service, "LocalRustPoster", lambda *args: fail)
        service.publish_cycle_audit_dashboard_summary_safely(tmp_path, {})
        label = "cycle_summary_post"
    elif operation == "cycle":
        monkeypatch.setattr(service, "run_once", fail)
        post = Mock(return_value={})
        assert service.run_cycle_safely(config, label="fixture_cycle", poster=post) is None
        assert post.call_args.args[1]["status"] == "llm_review_failed"
        label = "fixture_cycle"
    elif operation == "settlement":
        monkeypatch.setattr(service.StrategyLearningStore, "settle_only", fail)
        assert service.settle_strategy_learning_safely(tmp_path, context={}) is None
        label = "strategy_learning_settlement"
    else:
        monkeypatch.setattr(service.StrategyLearningStore, "update_from_cycle", fail)
        service.update_strategy_learning_safely(
            tmp_path, decision={}, response={}, context={}, post=Mock(spec=service.Poster)
        )
        label = "strategy_learning"
    rows = [json.loads(row) for row in (tmp_path / "service-errors.jsonl").read_text().splitlines()]
    assert rows[-1]["cycle_label"] == label
    assert rows[-1]["error_type"] == "RuntimeError"
    assert_safe_log(caplog, "failed (RuntimeError)")


def test_discovery_and_asset_failures_remain_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fail = Mock(side_effect=RuntimeError(PRIVATE_DETAIL))
    client = SimpleNamespace(
        active_assets=list, top_movers=lambda **kwargs: [], most_active=fail
    )
    _, health = service.discover_research_symbols_with_health(client, load_equity_universe())
    assert health["most_active"]["status"] == "unavailable"
    client.active_assets = fail
    assert service.load_asset_metadata(client, ["SPY"]) == {}
    assert_safe_log(caplog, "load_asset_metadata")


def test_learning_continues_when_after_status_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    update = Mock(return_value={})
    monkeypatch.setattr(service.StrategyLearningStore, "update_from_cycle", update)
    post = SimpleNamespace(get_status=Mock(side_effect=RuntimeError(PRIVATE_DETAIL)))
    service.update_strategy_learning_safely(
        tmp_path, decision={}, response={}, context={}, post=post
    )
    assert update.call_args.kwargs["after_status"] is None
    assert_safe_log(caplog, "update_strategy_learning_safely")
