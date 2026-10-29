"""Contracts for LLM-only equity dry-run trading."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, time
from enum import Enum
from pathlib import Path
from typing import Any, Final, Literal, TypeAlias, cast
from zoneinfo import ZoneInfo

from jsonschema import (  # type: ignore[import-untyped]
    Draft202012Validator,
    FormatChecker,
    RefResolver,
    ValidationError,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_UNIVERSE_PATH: Final = ROOT / "config/market_universe/llm-equity-dry-run.json"
SCHEMA_DIR: Final = ROOT / "schemas"
MIN_ORDER_NOTIONAL_EXCLUSIVE_USD: Final = 0.0
MANDATORY_ORDER_NOTIONAL_USD: Final = 25.0
FORCED_RTH_CYCLE_KINDS: Final = frozenset(
    {"run_once", "discovery_scan", "news_scan", "release"}
)
ALLOWED_COMMANDS: Final = {
    "buy_notional",
    "sell_notional",
}
DECISION_KEYS: Final = {
    "execution_policy",
    "target_portfolio",
    "cycle_id",
    "sequence",
    "as_of",
    "market_session",
    "portfolio_nav_usd",
    "commands",
    "evidence",
    "risk_assessment",
    "memory_updates",
}
COMMAND_KEYS: Final = {
    "reduce_only",
    "action",
    "symbol",
    "rationale",
    "confidence",
    "max_notional_usd",
    "evidence_refs",
    "horizon",
    "forecast",
}
FORECAST_KEYS: Final = {
    "edge_class",
    "expected_excess_return_bps",
    "probability_positive",
    "return_p10_bps",
    "return_p50_bps",
    "return_p90_bps",
}
ALLOWED_EDGE_CLASSES: Final = frozenset(
    {"positive_edge", "least_bad_forced", "risk_reduction"}
)
EVIDENCE_KEYS: Final = {
    "id",
    "source",
    "url",
    "summary",
    "retrieved_at",
    "freshness",
}
RISK_ASSESSMENT_KEYS: Final = {
    "summary",
    "max_loss_scenario",
    "risk_limit_verdict",
}
MEMORY_EVENT_KEYS: Final = {
    "event_id",
    "cycle_id",
    "created_at",
    "thesis",
    "outcome",
    "conclusion",
    "prompt_adjustment",
    "next_cycle_watch_items",
}
ALPHA_CARD_KEYS: Final = {
    "schema_version",
    "symbol",
    "side",
    "horizon_minutes",
    "edge_type",
    "thesis",
    "evidence_refs",
    "confidence",
    "expected_alpha_bps",
    "downside_bps",
    "spread_bps",
    "liquidity_score",
    "invalidation",
    "agent_role",
}
TRADE_SLATE_KEYS: Final = {
    "schema_version",
    "cycle_id",
    "generated_at",
    "market_session",
    "alpha_cards",
    "swarm_agent_count",
    "alpha_card_count",
    "consensus_score",
    "evidence_family_count",
    "cost_threshold_bps",
    "edge_gate_status",
    "edge_gate_reason",
    "selected_symbol",
    "selected_side",
    "selected_strategy_book",
    "no_trade_reason",
    "strategy_books",
}
SWARM_REFLECTION_KEYS: Final = {
    "schema_version",
    "cycle_id",
    "created_at",
    "strategy_book",
    "lesson",
    "outcome",
    "next_watch_items",
    "source",
}
STRATEGY_BOOK_KEYS: Final = {"nav_usd", "alpha_usd"}
ALLOWED_ALPHA_SIDES: Final = frozenset({"long", "short"})
ALLOWED_AGENT_ROLES: Final = frozenset(
    {
        "catalyst_scout",
        "text_edge_analyst",
        "neglected_small_cap_analyst",
        "macro_fedspeak_analyst",
        "technical_microstructure_analyst",
        "short_contrarian_analyst",
        "risk_cost_analyst",
        "arbiter",
    }
)
ALLOWED_EDGE_GATE_STATUSES: Final = frozenset(
    {
        "pass",
        "skipped_no_edge",
        "skipped_non_rth",
        "stale_context",
        "high_spread",
        "low_consensus",
        "insufficient_evidence",
    }
)
SNAPSHOT_KEYS: Final = {
    "snapshot_id",
    "as_of",
    "market_session",
    "symbols",
    "latest_bars",
    "latest_quotes",
    "session_bars",
    "previous_daily_bars",
    "asset_metadata",
    "discovery_health",
    "market_calendar_state",
    "trend_summaries",
    "source_freshness",
    "macro_news_summaries",
    "unavailable_data_flags",
}
BAR_KEYS: Final = {"timestamp", "open", "high", "low", "close", "volume", "source"}
QUOTE_KEYS: Final = {"timestamp", "bid", "ask", "source"}
ASSET_METADATA_KEYS: Final = {"tradable", "shortable", "fractionable", "investment_kind"}
CALENDAR_KEYS: Final = {
    "source",
    "retrieved_at",
    "is_open",
    "next_open",
    "next_close",
    "freshness",
}
SOURCE_FRESHNESS_KEYS: Final = {"source", "freshness"}
MACRO_NEWS_KEYS: Final = {
    "source",
    "url",
    "retrieved_at",
    "published_at",
    "content_hash",
    "summary",
    "freshness",
    "source_records",
}
MACRO_NEWS_SOURCE_RECORD_KEYS: Final = {
    "url",
    "published_at",
    "content_hash",
    "summary",
}
REQUIRED_SOURCE_FRESHNESS: Final = frozenset(
    {
        "alpaca_market_data",
        "alpaca_market_clock",
        "macro_calendar",
        "news_summary",
    }
)
REQUIRED_MACRO_NEWS_SOURCES: Final = frozenset({"macro_calendar", "news_summary"})
RAW_FORBIDDEN_KEYS: Final = {
    "raw_prompt",
    "raw_prompts",
    "prompt",
    "prompts",
    "raw_log",
    "raw_logs",
    "stdout",
    "stderr",
    "article_text",
    "full_text",
    "credentials",
    "api_key",
    "secret",
    "token",
    "password",
}
MEMORY_TEXT_LIMIT: Final = 1000
SUMMARY_TEXT_LIMIT: Final = 1000
SYMBOL_RE: Final = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_SECRET_ASSIGNMENT_RE: Final = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\s*[:=]\s*[^,\s;]+"
)

CommandAction: TypeAlias = Literal[
    "buy_notional",
    "sell_notional",
]


class LlmEquityContractError(ValueError):
    """Raised when an LLM equity dry-run contract fails closed."""


class MarketSession(str, Enum):
    PREMARKET = "premarket"
    RTH = "rth"
    AFTER_HOURS = "after_hours"
    CLOSED = "closed"


@dataclass(frozen=True)
class EquityInstrument:
    symbol: str
    asset_class: str
    venue: str
    currency: str
    timezone: str
    lot_size: int
    tick_size: float


@dataclass(frozen=True)
class EquityUniverse:
    schema_version: int
    universe_version: str
    market_family: str
    provider: str
    initial_cash_usd: float
    timezone: str
    regular_session_open: time
    regular_session_close: time
    allowed_order_sessions: frozenset[str]
    core_symbols: tuple[str, ...]
    research_symbol_limit: int
    instruments: Mapping[str, EquityInstrument]

    def require_symbol(self, symbol: str) -> str:
        normalized = normalize_symbol(symbol)
        return normalized

    def require_symbols(self, symbols: Sequence[str]) -> tuple[str, ...]:
        return tuple(self.require_symbol(symbol) for symbol in symbols)

    def instrument(self, symbol: str) -> EquityInstrument:
        normalized = self.require_symbol(symbol)
        instrument = self.instruments.get(normalized)
        if instrument is not None:
            return instrument
        return EquityInstrument(
            symbol=normalized,
            asset_class="equity_stock",
            venue="us_equities_paper",
            currency="USD",
            timezone=self.timezone,
            lot_size=1,
            tick_size=0.01,
        )

    def classify_session(self, as_of: datetime) -> MarketSession:
        local = ensure_utc(as_of).astimezone(ZoneInfo(self.timezone))
        if local.weekday() >= 5:
            return MarketSession.CLOSED
        current = local.time()
        if self.regular_session_open <= current < self.regular_session_close:
            return MarketSession.RTH
        if time(4, 0) <= current < self.regular_session_open:
            return MarketSession.PREMARKET
        if self.regular_session_close <= current < time(20, 0):
            return MarketSession.AFTER_HOURS
        return MarketSession.CLOSED


@dataclass(frozen=True)
class LlmForecast:
    edge_class: str
    expected_excess_return_bps: float
    probability_positive: float
    return_p10_bps: float
    return_p50_bps: float
    return_p90_bps: float


@dataclass(frozen=True)
class LlmTradingCommand:
    action: CommandAction
    symbol: str
    rationale: str
    confidence: float
    max_notional_usd: float
    evidence_refs: tuple[str, ...]
    horizon: str = "60m"
    forecast: LlmForecast | None = None
    reduce_only: bool = False


@dataclass(frozen=True)
class LlmTradingDecision:
    cycle_id: str
    sequence: int
    as_of: datetime
    market_session: MarketSession
    portfolio_nav_usd: float
    commands: tuple[LlmTradingCommand, ...]
    evidence: tuple[Mapping[str, Any], ...]
    risk_assessment: Mapping[str, Any]
    memory_updates: tuple[Mapping[str, Any], ...]
    execution_policy: str | None = None
    target_portfolio: Mapping[str, Any] | None = None


def load_equity_universe(path: Path = DEFAULT_UNIVERSE_PATH) -> EquityUniverse:
    payload = _read_json_object(path)
    session = _mapping(payload, "regular_session")
    instruments_payload = _sequence(payload, "instruments")
    instruments: dict[str, EquityInstrument] = {}
    for item in instruments_payload:
        row = _as_mapping(item, "instrument")
        symbol = normalize_symbol(_string(row, "symbol"))
        instruments[symbol] = EquityInstrument(
            symbol=symbol,
            asset_class=_string(row, "asset_class"),
            venue=_string(row, "venue"),
            currency=_string(row, "currency"),
            timezone=_string(row, "timezone"),
            lot_size=_positive_int(row, "lot_size"),
            tick_size=_positive_float(row, "tick_size"),
        )
    core_symbols = tuple(
        normalize_symbol(symbol) for symbol in _string_sequence(payload, "core_symbols")
    )
    if not core_symbols:
        raise LlmEquityContractError("core_symbols must not be empty")
    if not set(core_symbols).issubset(set(instruments)):
        raise LlmEquityContractError("core_symbols must be present in instruments")
    return EquityUniverse(
        schema_version=_positive_int(payload, "schema_version"),
        universe_version=_string(payload, "universe_version"),
        market_family=_string(payload, "market_family"),
        provider=_string(payload, "provider"),
        initial_cash_usd=_positive_float(payload, "initial_cash_usd"),
        timezone=_string(session, "timezone"),
        regular_session_open=_parse_time(_string(session, "open")),
        regular_session_close=_parse_time(_string(session, "close")),
        allowed_order_sessions=frozenset(_string_sequence(payload, "allowed_sessions_for_orders")),
        core_symbols=core_symbols,
        research_symbol_limit=_positive_int(payload, "research_symbol_limit"),
        instruments=instruments,
    )


def validate_llm_decision(
    payload: Mapping[str, Any],
    universe: EquityUniverse | None = None,
) -> LlmTradingDecision:
    universe = universe or load_equity_universe()
    reject_forbidden_fields(payload)
    _reject_unknown_keys(payload, DECISION_KEYS, "llm_trading_decision")
    policy = payload.get("execution_policy")
    if policy is not None and policy not in {"profit_driven_v1", "fully_invested_v1"}:
        raise LlmEquityContractError("execution_policy is invalid")
    if policy == "fully_invested_v1":
        try:
            schema = load_schema("llm_trading_decision.schema.json")
            memory_schema = load_schema("llm_memory_event.schema.json")
            resolver = RefResolver.from_schema(schema, store={str(memory_schema["$id"]): memory_schema})
            Draft202012Validator(schema, format_checker=FormatChecker(), resolver=resolver).validate(payload)
        except ValidationError as exc:
            raise LlmEquityContractError(f"allocation contract: {exc.message}") from exc
        holdings = payload["target_portfolio"]["holdings"]
        if (sum(row["weight_bps"] for row in holdings) != 10000
                or len({row["symbol"] for row in holdings}) != len(holdings)):
            raise LlmEquityContractError("allocation weights or symbols invalid")
    elif "target_portfolio" in payload:
        raise LlmEquityContractError("target_portfolio requires fully invested policy")
    cycle_id = _string(payload, "cycle_id")
    as_of = parse_utc_datetime(_string(payload, "as_of"))
    market_session = _market_session(_string(payload, "market_session"))
    portfolio_nav_usd = _positive_float(payload, "portfolio_nav_usd")
    evidence = tuple(
        _strict_mapping(item, EVIDENCE_KEYS, "evidence") for item in _sequence(payload, "evidence")
    )
    evidence_ids = {_string(item, "id") for item in evidence}
    if not evidence_ids:
        raise LlmEquityContractError("decision evidence must not be empty")
    commands = tuple(
        _command_from_mapping(item, evidence_ids, universe)
        for item in _sequence(payload, "commands")
    )
    if not commands and policy != "fully_invested_v1":
        raise LlmEquityContractError("decision commands must not be empty")
    if policy is None and not any(
        command.max_notional_usd >= MANDATORY_ORDER_NOTIONAL_USD for command in commands
    ):
        raise LlmEquityContractError(
            f"mandatory order command must be at least {MANDATORY_ORDER_NOTIONAL_USD:g} USD"
        )
    risk_assessment = _strict_mapping(
        payload.get("risk_assessment"), RISK_ASSESSMENT_KEYS, "risk_assessment"
    )
    if _string(risk_assessment, "risk_limit_verdict") != "pass":
        raise LlmEquityContractError("risk_assessment.risk_limit_verdict is invalid")
    memory_updates = tuple(
        sanitize_memory_event(_strict_mapping(item, MEMORY_EVENT_KEYS, "memory_update"))
        for item in _sequence(payload, "memory_updates")
    )
    if not memory_updates:
        raise LlmEquityContractError("decision memory_updates must not be empty")
    return LlmTradingDecision(
        cycle_id=cycle_id,
        sequence=_positive_int(payload, "sequence"),
        as_of=as_of,
        market_session=market_session,
        portfolio_nav_usd=portfolio_nav_usd,
        commands=commands,
        evidence=evidence,
        risk_assessment=risk_assessment,
        memory_updates=memory_updates,
        execution_policy=policy,
        target_portfolio=payload.get("target_portfolio"),
    )


def validate_market_research_snapshot(
    payload: Mapping[str, Any],
    universe: EquityUniverse | None = None,
) -> Mapping[str, Any]:
    universe = universe or load_equity_universe()
    reject_forbidden_fields(payload)
    _reject_unknown_keys(payload, SNAPSHOT_KEYS, "market_research_snapshot")
    symbols = universe.require_symbols(_string_sequence(payload, "symbols"))
    latest_bars = _mapping(payload, "latest_bars")
    latest_quotes = _mapping(payload, "latest_quotes")
    session_bars = payload.get("session_bars", {})
    previous_daily_bars = payload.get("previous_daily_bars", {})
    asset_metadata = payload.get("asset_metadata", {})
    if not isinstance(session_bars, Mapping) or not isinstance(previous_daily_bars, Mapping):
        raise LlmEquityContractError("session and previous daily bars must be objects")
    if not isinstance(asset_metadata, Mapping):
        raise LlmEquityContractError("asset_metadata must be an object")
    calendar_state = _strict_mapping(
        payload.get("market_calendar_state"), CALENDAR_KEYS, "market_calendar_state"
    )
    if _string(calendar_state, "source") != "alpaca_market_clock":
        raise LlmEquityContractError("market_calendar_state.source must be alpaca_market_clock")
    parse_utc_datetime(_string(calendar_state, "retrieved_at"))
    if not isinstance(calendar_state.get("is_open"), bool):
        raise LlmEquityContractError("market_calendar_state.is_open must be boolean")
    if _string(calendar_state, "freshness") not in {"fresh", "stale", "unavailable"}:
        raise LlmEquityContractError("market_calendar_state.freshness is invalid")
    for symbol in symbols:
        if symbol not in latest_bars:
            raise LlmEquityContractError(f"latest_bars missing {symbol}")
        if symbol not in latest_quotes:
            raise LlmEquityContractError(f"latest_quotes missing {symbol}")
        bar = _strict_mapping(latest_bars[symbol], BAR_KEYS, f"bar[{symbol}]")
        quote = _strict_mapping(latest_quotes[symbol], QUOTE_KEYS, f"quote[{symbol}]")
        parse_utc_datetime(_string(bar, "timestamp"))
        quote_source = _string(quote, "source")
        if "unavailable" in quote_source:
            _non_negative_float(quote, "bid")
            _non_negative_float(quote, "ask")
        else:
            _positive_float(quote, "bid")
            _positive_float(quote, "ask")
        parse_utc_datetime(_string(quote, "timestamp"))
        if session_bars:
            if symbol not in session_bars:
                raise LlmEquityContractError(f"session_bars missing {symbol}")
            session_bar = _strict_mapping(session_bars[symbol], BAR_KEYS, f"session_bar[{symbol}]")
            parse_utc_datetime(_string(session_bar, "timestamp"))
        if symbol in previous_daily_bars:
            previous_bar = _strict_mapping(
                previous_daily_bars[symbol], BAR_KEYS, f"previous_daily_bar[{symbol}]"
            )
            parse_utc_datetime(_string(previous_bar, "timestamp"))
        if asset_metadata:
            if symbol not in asset_metadata:
                raise LlmEquityContractError(f"asset_metadata missing {symbol}")
            metadata = _strict_mapping(
                asset_metadata[symbol], ASSET_METADATA_KEYS, f"asset_metadata[{symbol}]"
            )
            if not all(isinstance(metadata.get(key), bool) for key in ("tradable", "shortable", "fractionable")):
                raise LlmEquityContractError(f"asset_metadata[{symbol}] flags must be boolean")
            if "investment_kind" in metadata and metadata["investment_kind"] not in {"equity_stock", "equity_etf", "excluded"}:
                raise LlmEquityContractError("asset investment_kind is invalid")
    _market_session(_string(payload, "market_session"))
    parse_utc_datetime(_string(payload, "as_of"))
    _source_freshness_by_source(_source_freshness_rows(payload))
    _macro_news_by_source(_macro_news_rows(payload))
    for flag in _sequence(payload, "unavailable_data_flags"):
        if not isinstance(flag, str) or flag.strip() == "":
            raise LlmEquityContractError("unavailable_data_flags must contain non-empty strings")
    for row in _macro_news_rows(payload):
        if row.get("published_at") is not None:
            parse_utc_datetime(_string(row, "published_at"))
        if row.get("content_hash") is not None:
            content_hash = _string(row, "content_hash")
            if not re.fullmatch(r"sha256:[a-f0-9]{64}", content_hash):
                raise LlmEquityContractError("macro/news content_hash is invalid")
    trend_summaries = _mapping(payload, "trend_summaries")
    for symbol, summary in trend_summaries.items():
        universe.require_symbol(str(symbol))
        _redacted_text(str(summary), field=f"trend_summaries.{symbol}")
    return payload


def validate_cycle_audit_record(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    reject_forbidden_fields(payload)
    schema_version = payload.get("schema_version")
    if schema_version == 1:
        schema_name = "llm_equity_cycle_audit.schema.json"
    elif schema_version == 2:
        schema_name = "llm_equity_cycle_audit_v2.schema.json"
    elif schema_version == 3:
        schema_name = "llm_equity_cycle_audit_v3.schema.json"
    else:
        raise LlmEquityContractError("cycle audit schema_version must be 1, 2, or 3")
    schema = load_schema(schema_name)
    schema_store: dict[str, Mapping[str, Any]] = {}
    if schema_version == 3:
        for dependency_name in (
            "llm_equity_cycle_audit_v2.schema.json",
            "llm_trading_decision.schema.json",
        ):
            dependency = load_schema(dependency_name)
            dependency_id = str(dependency.get("$id", ""))
            schema_store[dependency_id] = dependency
    resolver = RefResolver.from_schema(schema, store=schema_store)
    try:
        Draft202012Validator(
            schema,
            format_checker=FormatChecker(),
            resolver=resolver,
        ).validate(payload)
    except ValidationError as exc:
        raise LlmEquityContractError(f"cycle audit schema violation: {exc.message}") from exc
    return payload


def validate_llm_review_result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Validate the compact model-owned review surface before deterministic adaptation."""
    reject_forbidden_fields(payload)
    schema_version = payload.get("schema_version")
    if schema_version == 2:
        schema_name = "llm_review_result_v2.schema.json"
    elif schema_version == 3:
        schema_name = "llm_review_result_v3.schema.json"
    else:
        raise LlmEquityContractError("review result schema_version must be 2 or 3")
    schema = load_schema(schema_name)
    try:
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
    except ValidationError as exc:
        raise LlmEquityContractError(f"review result schema violation: {exc.message}") from exc
    if schema_version == 3:
        for intent in _sequence(payload, "intents"):
            intent_payload = _as_mapping(intent, "review intent")
            _forecast_from_mapping(
                {key: intent_payload.get(key) for key in FORECAST_KEYS}
            )
    return payload


def validate_alpha_card(
    payload: Mapping[str, Any],
    universe: EquityUniverse | None = None,
) -> Mapping[str, Any]:
    universe = universe or load_equity_universe()
    reject_forbidden_fields(payload)
    _reject_unknown_keys(payload, ALPHA_CARD_KEYS, "llm_alpha_card")
    _schema_validate("llm_alpha_card.schema.json", payload, "alpha card")
    _schema_version(payload, "llm_alpha_card")
    universe.require_symbol(_string(payload, "symbol"))
    side = _string(payload, "side")
    if side not in ALLOWED_ALPHA_SIDES:
        raise LlmEquityContractError(f"invalid alpha card side: {side}")
    role = _string(payload, "agent_role")
    if role not in ALLOWED_AGENT_ROLES:
        raise LlmEquityContractError(f"invalid alpha card agent_role: {role}")
    if not _string_sequence(payload, "evidence_refs"):
        raise LlmEquityContractError("alpha card evidence_refs must not be empty")
    _positive_int(payload, "horizon_minutes")
    _redacted_text(_string(payload, "edge_type"), field="edge_type")
    _redacted_text(_string(payload, "thesis"), field="thesis")
    _redacted_text(_string(payload, "invalidation"), field="invalidation")
    _bounded_float(payload, "confidence", 0.0, 1.0)
    _bounded_float(payload, "expected_alpha_bps", -10000.0, 10000.0)
    _bounded_float(payload, "downside_bps", 0.0, 10000.0)
    _bounded_float(payload, "spread_bps", 0.0, 10000.0)
    _bounded_float(payload, "liquidity_score", 0.0, 1.0)
    return payload


def validate_trade_slate(
    payload: Mapping[str, Any],
    universe: EquityUniverse | None = None,
) -> Mapping[str, Any]:
    universe = universe or load_equity_universe()
    reject_forbidden_fields(payload)
    _reject_unknown_keys(payload, TRADE_SLATE_KEYS, "llm_trade_slate")
    _schema_validate("llm_trade_slate.schema.json", payload, "trade slate")
    _schema_version(payload, "llm_trade_slate")
    parse_utc_datetime(_string(payload, "generated_at"))
    _market_session(_string(payload, "market_session"))
    cards = _sequence(payload, "alpha_cards")
    for card in cards:
        validate_alpha_card(_as_mapping(card, "alpha_card"), universe)
    if _positive_int_or_zero(payload, "alpha_card_count") != len(cards):
        raise LlmEquityContractError("alpha_card_count must match alpha_cards length")
    selected_symbol = payload.get("selected_symbol")
    if selected_symbol is not None:
        universe.require_symbol(str(selected_symbol))
    selected_side = payload.get("selected_side")
    if selected_side is not None and str(selected_side) not in ALLOWED_ALPHA_SIDES:
        raise LlmEquityContractError("selected_side is invalid")
    edge_gate_status = _string(payload, "edge_gate_status")
    if edge_gate_status not in ALLOWED_EDGE_GATE_STATUSES:
        raise LlmEquityContractError("edge_gate_status is invalid")
    _redacted_text(_string(payload, "edge_gate_reason"), field="edge_gate_reason")
    no_trade_reason = payload.get("no_trade_reason")
    if no_trade_reason is not None:
        _redacted_text(str(no_trade_reason), field="no_trade_reason")
    _bounded_float(payload, "consensus_score", 0.0, 1.0)
    _positive_int_or_zero(payload, "swarm_agent_count")
    _positive_int_or_zero(payload, "evidence_family_count")
    _bounded_float(payload, "cost_threshold_bps", 0.0, 20000.0)
    strategy_books = _mapping(payload, "strategy_books")
    selected_book = payload.get("selected_strategy_book")
    if selected_book is not None and str(selected_book) not in strategy_books:
        raise LlmEquityContractError("selected_strategy_book must exist in strategy_books")
    selected_values = (selected_symbol, selected_side, selected_book)
    if edge_gate_status == "pass":
        if any(value is None for value in selected_values):
            raise LlmEquityContractError(
                "pass trade slate requires selected symbol, side, and book"
            )
        if no_trade_reason is not None:
            raise LlmEquityContractError("pass trade slate must not include no_trade_reason")
    else:
        if any(value is not None for value in selected_values):
            raise LlmEquityContractError("non-pass trade slate must not include selected fields")
        if no_trade_reason is None:
            raise LlmEquityContractError("non-pass trade slate requires no_trade_reason")
    for book, row in strategy_books.items():
        book_row = _strict_mapping(row, STRATEGY_BOOK_KEYS, f"strategy_books[{book}]")
        _bounded_number(book_row.get("nav_usd"), -float("inf"), float("inf"), key="nav_usd")
        _bounded_number(book_row.get("alpha_usd"), -float("inf"), float("inf"), key="alpha_usd")
    return payload


def validate_swarm_reflection(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    reject_forbidden_fields(payload)
    _schema_validate("llm_swarm_reflection.schema.json", payload, "swarm reflection")
    _reject_unknown_keys(payload, SWARM_REFLECTION_KEYS, "llm_swarm_reflection")
    _schema_version(payload, "llm_swarm_reflection")
    parse_utc_datetime(_string(payload, "created_at"))
    _redacted_text(_string(payload, "lesson"), field="lesson")
    _redacted_text(_string(payload, "outcome"), field="outcome")
    for item in _sequence(payload, "next_watch_items"):
        if not isinstance(item, str) or not item.strip():
            raise LlmEquityContractError("next_watch_items must contain non-empty strings")
        _redacted_text(item, field="next_watch_items")
    return payload


def snapshot_requires_hold(snapshot: Mapping[str, Any]) -> bool:
    try:
        source_freshness = _source_freshness_rows(snapshot)
        freshness_by_source = _source_freshness_by_source(source_freshness, require_all=False)
        if not REQUIRED_SOURCE_FRESHNESS.issubset(freshness_by_source):
            return True
        if any(freshness != "fresh" for freshness in freshness_by_source.values()):
            return True
        calendar_state = _mapping(snapshot, "market_calendar_state")
        if _string(calendar_state, "source") != "alpaca_market_clock":
            return True
        if _string(calendar_state, "freshness") != "fresh":
            return True
        if _sequence(snapshot, "unavailable_data_flags"):
            return True
        macro_news_by_source = _macro_news_by_source(_macro_news_rows(snapshot), require_all=False)
    except LlmEquityContractError:
        return True
    if not REQUIRED_MACRO_NEWS_SOURCES.issubset(macro_news_by_source):
        return True
    return any(_string(summary, "freshness") != "fresh" for summary in macro_news_by_source.values())


def sanitize_memory_event(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    reject_forbidden_fields(payload)
    _reject_unknown_keys(payload, MEMORY_EVENT_KEYS, "memory_update")
    sanitized: dict[str, Any] = {
        "event_id": _string(payload, "event_id")[:120],
        "cycle_id": _string(payload, "cycle_id")[:120],
        "created_at": _format_utc(parse_utc_datetime(_string(payload, "created_at"))),
        "thesis": _redacted_text(_string(payload, "thesis"), field="thesis")[:MEMORY_TEXT_LIMIT],
        "outcome": _redacted_text(_string(payload, "outcome"), field="outcome")[:MEMORY_TEXT_LIMIT],
        "conclusion": _redacted_text(_string(payload, "conclusion"), field="conclusion")[
            :MEMORY_TEXT_LIMIT
        ],
        "prompt_adjustment": _redacted_text(
            str(payload.get("prompt_adjustment", "")),
            field="prompt_adjustment",
        )[:MEMORY_TEXT_LIMIT],
        "next_cycle_watch_items": [
            _redacted_text(str(item), field="next_cycle_watch_items")[:240]
            for item in _sequence(payload, "next_cycle_watch_items")
        ][:12],
    }
    return sanitized


def _source_freshness_rows(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rows = tuple(
        _strict_mapping(item, SOURCE_FRESHNESS_KEYS, "source_freshness")
        for item in _sequence(payload, "source_freshness")
    )
    if not rows:
        raise LlmEquityContractError("source_freshness must not be empty")
    return rows


def _source_freshness_by_source(
    rows: Sequence[Mapping[str, Any]],
    *,
    require_all: bool = True,
) -> Mapping[str, str]:
    freshness_by_source: dict[str, str] = {}
    for item in rows:
        source = _string(item, "source")
        freshness = _string(item, "freshness")
        if freshness not in {"fresh", "stale", "unavailable"}:
            raise LlmEquityContractError(f"source_freshness[{source!r}] is invalid")
        if source in freshness_by_source:
            raise LlmEquityContractError(f"source_freshness contains duplicate source: {source}")
        freshness_by_source[source] = freshness
    if require_all:
        missing = sorted(REQUIRED_SOURCE_FRESHNESS.difference(freshness_by_source))
        if missing:
            raise LlmEquityContractError(f"source_freshness missing required source(s): {missing}")
    return freshness_by_source


def _macro_news_rows(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        _strict_mapping(item, MACRO_NEWS_KEYS, "macro_news_summary")
        for item in _sequence(payload, "macro_news_summaries")
    )


def _macro_news_by_source(
    rows: Sequence[Mapping[str, Any]],
    *,
    require_all: bool = True,
) -> Mapping[str, Mapping[str, Any]]:
    summaries_by_source: dict[str, Mapping[str, Any]] = {}
    for summary in rows:
        source = _string(summary, "source")
        parse_utc_datetime(_string(summary, "retrieved_at"))
        _redacted_text(_string(summary, "summary"), field="macro_news_summaries.summary")
        source_records = summary.get("source_records")
        if source_records is not None:
            if not isinstance(source_records, Sequence) or isinstance(
                source_records, str | bytes | bytearray
            ):
                raise LlmEquityContractError("macro_news_summaries.source_records must be an array")
            if len(source_records) > 6:
                raise LlmEquityContractError(
                    "macro_news_summaries.source_records must contain at most 6 items"
                )
            for record in source_records:
                clean_record = _strict_mapping(
                    record,
                    MACRO_NEWS_SOURCE_RECORD_KEYS,
                    "macro_news_summaries.source_records",
                )
                _string(clean_record, "url")
                parse_utc_datetime(_string(clean_record, "published_at"))
                content_hash = _string(clean_record, "content_hash")
                if re.fullmatch(r"sha256:[a-f0-9]{64}", content_hash) is None:
                    raise LlmEquityContractError(
                        "macro_news_summaries.source_records.content_hash is invalid"
                    )
                _redacted_text(
                    _string(clean_record, "summary"),
                    field="macro_news_summaries.source_records.summary",
                )
        if _string(summary, "freshness") not in {"fresh", "stale", "unavailable"}:
            raise LlmEquityContractError("macro_news_summaries.freshness is invalid")
        if source in summaries_by_source:
            raise LlmEquityContractError(
                f"macro_news_summaries contains duplicate source: {source}"
            )
        summaries_by_source[source] = summary
    if require_all:
        missing = sorted(REQUIRED_MACRO_NEWS_SOURCES.difference(summaries_by_source))
        if missing:
            raise LlmEquityContractError(
                f"macro_news_summaries missing required source(s): {missing}"
            )
    return summaries_by_source


def load_schema(name: str) -> Mapping[str, Any]:
    if name not in {
        "llm_trading_decision.schema.json",
        "market_research_snapshot.schema.json",
        "llm_memory_event.schema.json",
        "llm_equity_cycle_audit.schema.json",
        "llm_equity_cycle_audit_v2.schema.json",
        "llm_equity_cycle_audit_v3.schema.json",
        "llm_alpha_card.schema.json",
        "llm_trade_slate.schema.json",
        "llm_swarm_reflection.schema.json",
        "llm_review_result_v2.schema.json",
        "llm_review_result_v3.schema.json",
    }:
        raise LlmEquityContractError(f"unknown schema: {name}")
    return _read_json_object(SCHEMA_DIR / name)


def _schema_validate(name: str, payload: Mapping[str, Any], label: str) -> None:
    try:
        Draft202012Validator(load_schema(name), format_checker=FormatChecker()).validate(payload)
    except ValidationError as exc:
        raise LlmEquityContractError(f"{label} schema violation: {exc.message}") from exc


def _schema_version(payload: Mapping[str, Any], label: str) -> None:
    if _positive_int(payload, "schema_version") != 1:
        raise LlmEquityContractError(f"{label}.schema_version must be 1")


def parse_utc_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return ensure_utc(parsed)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise LlmEquityContractError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not SYMBOL_RE.fullmatch(symbol):
        raise LlmEquityContractError(f"invalid equity symbol: {value!r}")
    return symbol


def reject_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            if key_text.lower() in RAW_FORBIDDEN_KEYS:
                raise LlmEquityContractError(
                    f"forbidden raw or credential field at {path}.{key_text}"
                )
            reject_forbidden_fields(child, f"{path}.{key_text}")
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, child in enumerate(value):
            reject_forbidden_fields(child, f"{path}[{index}]")
    elif isinstance(value, str):
        _redacted_text(value, field=path)


def _command_from_mapping(
    raw: Any,
    evidence_ids: set[str],
    universe: EquityUniverse,
) -> LlmTradingCommand:
    payload = _strict_mapping(raw, COMMAND_KEYS, "command")
    action = _string(payload, "action")
    if action not in ALLOWED_COMMANDS:
        raise LlmEquityContractError(f"unsupported LLM trading command: {action}")
    evidence_refs = tuple(_string_sequence(payload, "evidence_refs"))
    missing_refs = sorted(set(evidence_refs).difference(evidence_ids))
    if missing_refs:
        raise LlmEquityContractError(f"command evidence_refs missing evidence: {missing_refs}")
    max_notional_usd = _bounded_float(
        payload,
        "max_notional_usd",
        MIN_ORDER_NOTIONAL_EXCLUSIVE_USD,
        float("inf"),
        exclusive_min=True,
    )
    horizon = str(payload.get("horizon", "60m"))
    if horizon not in {"30m", "60m", "close", "1d", "5d"}:
        raise LlmEquityContractError("command horizon is invalid")
    forecast = (
        _forecast_from_mapping(payload.get("forecast"))
        if payload.get("forecast") is not None
        else None
    )
    reduce_only = payload.get("reduce_only", False)
    if not isinstance(reduce_only, bool):
        raise LlmEquityContractError("reduce_only must be a boolean")
    return LlmTradingCommand(
        action=cast(CommandAction, action),
        symbol=universe.require_symbol(_string(payload, "symbol")),
        rationale=_redacted_text(_string(payload, "rationale"), field="rationale")[
            :SUMMARY_TEXT_LIMIT
        ],
        confidence=_bounded_float(payload, "confidence", 0.0, 1.0),
        max_notional_usd=max_notional_usd,
        evidence_refs=evidence_refs,
        horizon=horizon,
        forecast=forecast,
        reduce_only=reduce_only,
    )


def _forecast_from_mapping(raw: Any) -> LlmForecast:
    payload = _strict_mapping(raw, FORECAST_KEYS, "forecast")
    edge_class = _string(payload, "edge_class")
    if edge_class not in ALLOWED_EDGE_CLASSES:
        raise LlmEquityContractError("forecast.edge_class is invalid")
    expected = _bounded_float(payload, "expected_excess_return_bps", -5000.0, 5000.0)
    probability = _bounded_float(payload, "probability_positive", 0.0, 1.0)
    p10 = _bounded_float(payload, "return_p10_bps", -5000.0, 5000.0)
    p50 = _bounded_float(payload, "return_p50_bps", -5000.0, 5000.0)
    p90 = _bounded_float(payload, "return_p90_bps", -5000.0, 5000.0)
    if not p10 <= p50 <= p90:
        raise LlmEquityContractError(
            "forecast return quantiles must satisfy p10 <= p50 <= p90"
        )
    return LlmForecast(
        edge_class=edge_class,
        expected_excess_return_bps=expected,
        probability_positive=probability,
        return_p10_bps=p10,
        return_p50_bps=p50,
        return_p90_bps=p90,
    )


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise LlmEquityContractError(f"{path} must contain a JSON object")
    return payload


def _market_session(value: str) -> MarketSession:
    try:
        return MarketSession(value)
    except ValueError as exc:
        raise LlmEquityContractError(f"invalid market_session: {value}") from exc


def _as_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LlmEquityContractError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _strict_mapping(value: Any, allowed_keys: set[str], name: str) -> Mapping[str, Any]:
    payload = _as_mapping(value, name)
    _reject_unknown_keys(payload, allowed_keys, name)
    return payload


def _reject_unknown_keys(payload: Mapping[str, Any], allowed_keys: set[str], name: str) -> None:
    unknown = sorted(str(key) for key in payload if str(key) not in allowed_keys)
    if unknown:
        raise LlmEquityContractError(f"{name} contains unsupported fields: {unknown}")


def _mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    return _as_mapping(payload.get(key), key)


def _sequence(payload: Mapping[str, Any], key: str) -> Sequence[Any]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        raise LlmEquityContractError(f"{key} must be an array")
    return value


def _string_sequence(payload: Mapping[str, Any], key: str) -> tuple[str, ...]:
    values = _sequence(payload, key)
    if not all(isinstance(item, str) and item.strip() for item in values):
        raise LlmEquityContractError(f"{key} must contain non-empty strings")
    return tuple(str(item).strip() for item in values)


def _string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or value.strip() == "":
        raise LlmEquityContractError(f"{key} must be a non-empty string")
    return value.strip()


def _positive_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value <= 0:
        raise LlmEquityContractError(f"{key} must be a positive integer")
    return value


def _positive_int_or_zero(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value < 0:
        raise LlmEquityContractError(f"{key} must be a non-negative integer")
    return value


def _positive_float(payload: Mapping[str, Any], key: str) -> float:
    return _bounded_float(payload, key, 0.0, float("inf"), exclusive_min=True)


def _non_negative_float(payload: Mapping[str, Any], key: str) -> float:
    return _bounded_float(payload, key, 0.0, float("inf"))


def _bounded_float(
    payload: Mapping[str, Any],
    key: str,
    minimum: float,
    maximum: float,
    *,
    exclusive_min: bool = False,
) -> float:
    return _bounded_number(payload.get(key), minimum, maximum, key=key, exclusive_min=exclusive_min)


def _bounded_number(
    value: Any,
    minimum: float,
    maximum: float,
    *,
    key: str = "number",
    exclusive_min: bool = False,
) -> float:
    if not isinstance(value, int | float):
        raise LlmEquityContractError(f"{key} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise LlmEquityContractError(f"{key} must be finite")
    lower_ok = number > minimum if exclusive_min else number >= minimum
    if not lower_ok or number > maximum:
        raise LlmEquityContractError(f"{key} must be between {minimum} and {maximum}")
    return number


def _parse_time(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise LlmEquityContractError(f"invalid time: {value}") from exc


def _redacted_text(value: str, *, field: str) -> str:
    if _SECRET_ASSIGNMENT_RE.search(value):
        raise LlmEquityContractError(f"{field} contains credential-like text")
    return value


def _format_utc(value: datetime) -> str:
    return ensure_utc(value).isoformat().replace("+00:00", "Z")
