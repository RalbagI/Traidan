"""Resident scheduler for atomic model-selected allocations; no sell-only path."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError

from . import fully_invested as policy
from .contracts import (
    LlmEquityContractError,
    MarketSession,
    load_equity_universe,
    validate_llm_decision,
)
from .policy import _deadline

if TYPE_CHECKING:
    from .hourly_service import HourlyServiceConfig, Poster

_LOGGER = logging.getLogger(__name__)


def _audit_review(review: Mapping[str, Any]) -> dict[str, Any]:
    from . import hourly_service as s

    result: dict[str, Any] = {
        key: s._redact_diagnostic_text(str(review[key]))[:1000]
        for key in ("rationale", "abstain_reason", "verdict")
        if key in review
    }
    if isinstance(review.get("holdings"), list):
        result["holdings"] = [
            {
                key: s._redact_diagnostic_text(str(row[key]))[:300]
                for key in (
                    "symbol",
                    "weight_bps",
                    "thesis",
                    "invalidation",
                    "horizon",
                    "expected_excess_return_bps",
                )
                if key in row
            }
            for row in review["holdings"][:40]
            if isinstance(row, Mapping)
        ]
    return result


def pending_path(config: HourlyServiceConfig) -> Path:
    from . import hourly_service as s

    return s.strategy_epoch_dir(config.state_dir) / "pending-allocation.json"


def _state_path(config: HourlyServiceConfig) -> Path:
    return pending_path(config).with_name("allocation-scheduler.json")


def _read(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise LlmEquityContractError("allocation state must be an object")
    return value


def _status(
    config: HourlyServiceConfig,
    poster: Poster,
    status: str,
    reason: str,
    snapshot: Mapping[str, Any] | None = None,
) -> None:
    from . import hourly_service as s

    s.publish_service_status(
        config,
        poster,
        status=status,
        detail=reason,
        market_snapshot=snapshot,
        last_error=None if status == "fully_invested" else reason,
        activity_key="fully-invested-allocation",
    )


def _save_audit(config: HourlyServiceConfig, record: Mapping[str, Any]) -> Path:
    from . import hourly_service as s

    path = pending_path(config).parent / "allocation-reviews" / f"{record['cycle_id']}.json"
    s.write_json_atomically(path, record)
    return path


def _record_audit(
    config: HourlyServiceConfig,
    poster: Poster,
    pending: dict[str, Any],
    *,
    response: Mapping[str, Any] | None = None,
) -> None:
    from . import hourly_service as s

    accepted = response is not None
    payload = pending.get("payload", {})
    context = pending.get("audit_context", {"profitability_scorecard": {"costs": {}}})
    decision = payload.get(
        "llm_trading_decision", {"cycle_id": pending["cycle_id"], "commands": []}
    )
    audit = _save_audit(config, pending)
    record = s.record_cycle_audit(
        config.state_dir,
        cycle_kind="run_once",
        status="posted" if accepted else "blocked",
        snapshot=payload.get("market_research_snapshot", context.get("market_research_snapshot")),
        decision=decision,
        context=context,
        rust_response=response
        or {"status": "blocked", "reason": pending.get("reason", "review failed")},
        llm_invoked=bool(pending["attestation"].get("invocation_count")),
        artifact_refs={"allocation_review": str(audit.relative_to(config.state_dir))},
        review_attestation={
            **pending["attestation"],
            "status": "compliant" if accepted else "failed",
        },
        idempotent=True,
    )
    # The legacy publisher is best-effort. For a durable allocation, retain the
    # pending receipt until its complete dashboard summary is acknowledged too.
    if summary := s.cycle_audit_dashboard_summary(record):
        poster(s.LLM_EQUITY_CYCLE_SUMMARY_PATH, summary)


def _finish_blocked(
    config: HourlyServiceConfig, poster: Poster, pending: dict[str, Any]
) -> dict[str, Any]:
    from . import hourly_service as s

    _record_audit(config, poster, pending)
    state = _read(_state_path(config))
    state["pending_proposal"] = pending.get("proposal_to_reapprove")
    state["next_attempt_at"] = (
        datetime.now(UTC) + timedelta(seconds=30 if state["pending_proposal"] else 300)
    ).isoformat()
    s.write_json_atomically(_state_path(config), state)
    pending_path(config).unlink(missing_ok=True)
    reason = str(pending.get("reason", "allocation review failed"))
    _status(config, poster, "allocation_blocked", reason)
    return {"status": "blocked", "reason": reason}


def _finish(
    config: HourlyServiceConfig,
    poster: Poster,
    pending: dict[str, Any],
    response: Mapping[str, Any],
) -> dict[str, Any]:
    from . import hourly_service as s

    if response.get("status") != "success" or int(response.get("rejected_orders", 0)):
        raise LlmEquityContractError("atomic allocation was not accepted")
    pending["status"] = "accepted"
    pending["rust_response"] = dict(response)
    snapshot = pending["payload"]["market_research_snapshot"]
    _record_audit(config, poster, pending, response=response)
    # Persist derivative scheduling state only after the authority's durable receipt.
    at = datetime.fromisoformat(snapshot["as_of"])
    plans = {}
    for holding in pending["review"]["holdings"]:
        symbol = holding["symbol"]
        quote = snapshot["latest_quotes"][symbol]
        mid = (float(quote["bid"]) + float(quote["ask"])) / 2
        horizon = holding["horizon"]
        expiry = max(
            at + timedelta(minutes=5), _deadline(at, horizon, snapshot["market_calendar_state"])
        )
        plans[symbol] = {"stop_price": mid * 0.95, "review_at": expiry.isoformat()}
    s.write_json_atomically(
        _state_path(config),
        {
            **_read(_state_path(config)),
            "last_review_at": at.isoformat(),
            "next_attempt_at": None,
            "pending_proposal": None,
            "plans": plans,
            "review": pending["review"],
        },
    )
    pending_path(config).unlink(missing_ok=True)
    _status(
        config,
        poster,
        "fully_invested",
        "תיק מושקע במלואו; כל מכירה הושלמה עם רכישה חלופית.",
        snapshot,
    )
    return dict(response)


def _submit_pending(
    config: HourlyServiceConfig, poster: Poster, pending: dict[str, Any]
) -> dict[str, Any]:
    from . import hourly_service as s

    if pending.get("status") in {"blocked", "rejected"}:
        return _finish_blocked(config, poster, pending)
    try:
        response = poster(s.LLM_EQUITY_CYCLE_PATH, pending["payload"])
    except (OSError, RuntimeError) as exc:
        http_error = exc if isinstance(exc, HTTPError) else exc.__cause__
        if isinstance(http_error, HTTPError) and http_error.code in {400, 422}:
            # Authority rejects before mutation; its exact reason is retained.
            reason = s.format_http_error(exc) if isinstance(exc, HTTPError) else str(exc)
            pending.update(status="rejected", reason=s._redact_diagnostic_text(reason)[:1000])
            s.write_json_atomically(pending_path(config), pending)
            return _finish_blocked(config, poster, pending)
        # An interrupted POST may already have committed. Keep its exact payload;
        # retrying it returns the stored Rust receipt, even after a restart.
        _status(
            config,
            poster,
            "allocation_pending",
            f"תוצאת ההקצאה ממתינה לאימות: {type(exc).__name__}",
        )
        return {"status": "pending", "reason": type(exc).__name__}
    return _finish(config, poster, pending, response)


def run_cycle(
    config: HourlyServiceConfig,
    *,
    now: datetime,
    poster: Poster | None = None,
    llm_client: Any = None,
    data_client: Any = None,
) -> dict[str, Any]:
    from . import hourly_service as s

    s.ensure_strategy_epoch(config.state_dir)
    post = poster or s.LocalRustPoster(config.rust_url, config.admin_token)
    if pending_path(config).exists():
        return _submit_pending(config, post, _read(pending_path(config)))
    universe = load_equity_universe()
    client = llm_client or s.CodexLlmClient(config.state_dir, config.timeout_seconds)
    record: dict[str, Any] = {
        "schema_version": 1,
        "cycle_id": f"allocation-review-{time.time_ns()}",
        "created_at": now.isoformat(),
        "status": "reviewing",
        "attempts": [],
        "attestation": s._new_review_attestation(required=True),
    }
    context: dict[str, Any] = {}
    saved = policy.pending_review(_read(_state_path(config)), now=now)
    previous_proposal: Mapping[str, Any] | None = saved["review"] if saved else None
    requote_basis: Mapping[str, Any] | None = saved["snapshot"] if saved else None
    if saved:
        record["proposal_origin_cycle_id"] = saved["origin_cycle_id"]
    approve_requote = getattr(client, "approve_portfolio_requote", None)
    for attempt in range(3):
        try:
            requote = (
                previous_proposal is not None
                and requote_basis is not None
                and callable(approve_requote)
            )
            _, snapshot, context = s.build_service_context(
                config,
                universe,
                now,
                post=post,
                data_client=data_client,
                context_fetcher=None,
                required_symbols=tuple(row["symbol"] for row in previous_proposal["holdings"])
                if requote and previous_proposal is not None
                else (),
                required_only=requote,
                current_cycle_kind="run_once",
            )
            record["audit_context"] = {
                "portfolio": context["portfolio"],
                "market_research_snapshot": snapshot,
                "profitability_scorecard": context.get("profitability_scorecard", {"costs": {}}),
            }
            reason = s.release_snapshot_block_reason(snapshot)
            if reason:
                record.update(status="blocked", reason=reason)
                break
            context["previous_allocation_review"] = previous_proposal or _read(
                _state_path(config)
            ).get("review")
            context["allocation_repair_reason"] = record.get("reason")
            approval_packet = None
            try:
                packet = policy.review_context(context)
                if requote and previous_proposal is not None and requote_basis is not None:
                    policy.validate_review(
                        previous_proposal, {row["symbol"] for row in packet["candidates"]}
                    )
                    approval_packet = policy.requote_context(
                        previous_proposal, requote_basis, context
                    )
            except LlmEquityContractError:
                if requote:
                    requote_basis = None
                raise
            s._start_review_invocation(record["attestation"])
            try:
                if approval_packet is not None:
                    assert callable(approve_requote)
                    approval = approve_requote(approval_packet)
                    policy.validate_requote_approval(approval, approval_packet["approval_id"])
                    record["attempts"].append(
                        {"attempt": attempt + 1, "price_approval": dict(approval)}
                    )
                    if not approval["approved"]:
                        requote_basis = None
                        raise LlmEquityContractError(
                            "allocation price approval declined: " + approval["rationale"]
                        )
                    assert previous_proposal is not None
                    review = previous_proposal
                else:
                    review = client.decide_portfolio(context)
            finally:
                s._record_review_usage(record["attestation"], getattr(client, "last_usage", {}))
            # Retain abstention and repair explanations instead of replacing them
            # with llm_veto_no_edge. Never store prompts, raw logs or credentials.
            record["attempts"].append(
                {
                    "attempt": attempt + 1,
                    "review": _audit_review(review),
                    "usage": dict(getattr(client, "last_usage", {})),
                }
            )
            policy.validate_review(review, {row["symbol"] for row in packet["candidates"]})
            previous_proposal = review
            requote_basis = snapshot
            if approval_packet is None:
                record.pop("proposal_origin_cycle_id", None)
            _, fresh_snapshot, fresh = s.build_service_context(
                config,
                universe,
                datetime.now(UTC),
                post=post,
                data_client=data_client,
                context_fetcher=None,
                required_symbols=tuple(row["symbol"] for row in review["holdings"]),
                required_only=True,
                current_cycle_kind="run_once",
            )
            if reason := s.release_snapshot_block_reason(fresh_snapshot):
                raise LlmEquityContractError(reason)
            traded_symbols = {row["symbol"] for row in review["holdings"]} | {
                row["symbol"] for row in context["portfolio"].get("positions", [])
            }
            for symbol in traded_symbols:
                old = snapshot["latest_quotes"][symbol]
                new = fresh_snapshot["latest_quotes"][symbol]
                if abs((new["bid"] + new["ask"]) / (old["bid"] + old["ask"]) - 1) > 0.0025:
                    raise LlmEquityContractError(
                        "material price change requires fresh allocation approval"
                    )
            decision = policy.decision_from_review(review, fresh, sequence=time.time_ns())
            validate_llm_decision(decision, universe)
            record.update(
                cycle_id=decision["cycle_id"],
                review=review,
                status="pending",
                candidates=packet["candidates"],
                audit_context={
                    "portfolio": fresh["portfolio"],
                    "market_research_snapshot": fresh_snapshot,
                    "profitability_scorecard": fresh.get("profitability_scorecard", {"costs": {}}),
                    "allocation_horizons": {
                        row["symbol"]: row["horizon"] for row in review["holdings"]
                    },
                },
                payload={
                    "market_research_snapshot": fresh_snapshot,
                    "llm_trading_decision": decision,
                },
            )
            _save_audit(config, record)
            s.write_json_atomically(pending_path(config), record)
            return _submit_pending(config, post, record)
        except Exception as exc:
            if pending_path(config).exists():
                # Derivative bookkeeping may fail after a committed fill. Do not
                # manufacture another review before reconciling the same receipt.
                raise
            record["reason"] = s._redact_diagnostic_text(str(exc))[:1000]
            record["attempts"].append({"attempt": attempt + 1, "error": record["reason"]})
            _LOGGER.warning("Allocation attempt %s blocked: %s", attempt + 1, record["reason"])
    record["status"] = "blocked"
    if previous_proposal is not None and requote_basis is not None:
        record["proposal_to_reapprove"] = {
            "review": previous_proposal,
            "snapshot": {key: requote_basis[key] for key in ("as_of", "latest_quotes")},
            "origin_cycle_id": record.get("proposal_origin_cycle_id", record["cycle_id"]),
        }
    s.write_json_atomically(pending_path(config), record)
    return _finish_blocked(config, post, record)


def poll(config: HourlyServiceConfig, *, now: datetime) -> None:
    from . import hourly_service as s

    post = s.LocalRustPoster(config.rust_url, config.admin_token)
    s.ensure_strategy_epoch(config.state_dir)
    if pending_path(config).exists():
        run_cycle(config, now=now, poster=post)
        return
    universe = load_equity_universe()
    state = _read(_state_path(config))
    retry = state.get("next_attempt_at")
    if retry and now < datetime.fromisoformat(retry):
        return
    portfolio = s.authoritative_portfolio_for_prompt(post, universe, fixture_mode=False)
    if universe.classify_session(now) != MarketSession.RTH:
        local = now.astimezone(s.NY_ZONE)
        last_scan = state.get("last_news_scan_at")
        due_scans = [
            cycle
            for cycle in s.scheduled_cycles_for_date(
                local.date(),
                open_prepare_lead_minutes=config.open_prepare_lead_minutes,
                hourly_prepare_lead_minutes=config.hourly_prepare_lead_minutes,
            )
            if cycle.kind == "news_scan"
            and cycle.run_at <= local
            and (not last_scan or cycle.run_at > datetime.fromisoformat(last_scan))
        ]
        if due_scans:
            cycle = max(due_scans, key=lambda row: row.run_at)
            s.run_news_scan(config, cycle=cycle, now=now, poster=post)
            state["last_news_scan_at"] = cycle.run_at.isoformat()
            s.write_json_atomically(_state_path(config), state)
        _status(
            config,
            post,
            "fully_invested" if portfolio.cash_usd <= 0.01 else "allocation_waiting_market",
            "האחזקות נשמרות עד לפתיחת המסחר."
            if portfolio.cash_usd <= 0.01
            else "ממתין לשוק פתוח להשקעת המזומן.",
        )
        return
    snapshot = s.build_market_research_snapshot(
        universe,
        now,
        required_only=True,
        required_symbols=tuple(portfolio.positions) + s.DEFAULT_BENCHMARK_SYMBOLS,
    )
    if reason := s.release_snapshot_block_reason(snapshot):
        _status(config, post, "allocation_waiting_market", reason)
        return
    _status(
        config,
        post,
        "fully_invested" if portfolio.cash_usd <= 0.01 else "allocation_required",
        "בדיקת הקצאה מושקעת במלואה."
        if portfolio.cash_usd <= 0.01
        else "מכין הקצאה לכל המזומן הזמין.",
        snapshot,
    )
    last = state.get("last_review_at")
    last_at = datetime.fromisoformat(last) if last else None
    local = now.astimezone(s.NY_ZONE)
    slots = s.scheduled_cycles_for_date(
        local.date(),
        open_prepare_lead_minutes=config.open_prepare_lead_minutes,
        hourly_prepare_lead_minutes=config.hourly_prepare_lead_minutes,
    )
    due = any(
        c.kind in {"news_scan", "discovery_scan", "release"}
        and c.run_at <= local
        and (last_at is None or c.run_at > last_at)
        for c in slots
    )
    needs_review = (
        portfolio.cash_usd > 0.01 or last_at is None or due or bool(state.get("pending_proposal"))
    )
    for symbol in portfolio.positions:
        plan = state.get("plans", {}).get(symbol)
        quote = snapshot["latest_quotes"][symbol]
        mid = (quote["bid"] + quote["ask"]) / 2
        if (
            not plan
            or now >= datetime.fromisoformat(plan["review_at"])
            or mid <= plan["stop_price"]
        ):
            needs_review = True
    if needs_review:
        run_cycle(config, now=now, poster=post)


def run_forever(config: HourlyServiceConfig) -> None:
    from . import hourly_service as s

    while True:
        try:
            poll(config, now=datetime.now(UTC))
        except Exception as exc:
            _LOGGER.exception("Fully invested scheduler failed", exc_info=False)
            s.record_service_error(config.state_dir, label="fully_invested", error=exc)
            _status(
                config,
                s.LocalRustPoster(config.rust_url, config.admin_token),
                "allocation_blocked",
                s._redact_diagnostic_text(str(exc))[:1000],
            )
        time.sleep(30)
