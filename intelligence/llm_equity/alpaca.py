"""Alpaca market-data adapter for the LLM equity dry-run."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

from .contracts import LlmEquityContractError, normalize_symbol

ALPACA_MOVERS_MAX_TOP = 50
ALPACA_MOST_ACTIVE_MAX_TOP = 100


class HttpClient(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, str],
        timeout: float,
    ) -> Any:
        """Return a response object with raise_for_status/json methods."""


@dataclass(frozen=True)
class AlpacaMarketDataConfig:
    key_id: str
    secret_key: str
    data_base_url: str = "https://data.alpaca.markets"
    trading_base_url: str = "https://paper-api.alpaca.markets"
    feed: str = "iex"
    timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> AlpacaMarketDataConfig:
        key_id = (
            os.environ.get("ALPACA_API_KEY_ID") or os.environ.get("APCA_API_KEY_ID", "")
        ).strip()
        secret_key = (
            os.environ.get("ALPACA_API_SECRET_KEY") or os.environ.get("APCA_API_SECRET_KEY", "")
        ).strip()
        if not key_id or not secret_key:
            raise RuntimeError(
                "ALPACA_API_KEY_ID/APCA_API_KEY_ID and "
                "ALPACA_API_SECRET_KEY/APCA_API_SECRET_KEY are required for Alpaca data"
            )
        return cls(
            key_id=key_id,
            secret_key=secret_key,
            data_base_url=_normalize_base_url(
                os.environ.get("ALPACA_DATA_BASE_URL", cls.data_base_url)
            ),
            trading_base_url=_normalize_base_url(
                os.environ.get(
                    "ALPACA_TRADING_BASE_URL",
                    os.environ.get("ALPACA_PAPER_API_BASE_URL", cls.trading_base_url),
                )
            ),
            feed=os.environ.get("ALPACA_DATA_FEED", cls.feed),
        )


class AlpacaMarketDataClient:
    """Small Alpaca REST client that never submits orders."""

    def __init__(self, config: AlpacaMarketDataConfig, http_client: HttpClient | None = None) -> None:
        self._config = config
        self._http_client = http_client

    def latest_bars(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
        response = self._get(
            f"{self._config.data_base_url}/v2/stocks/bars/latest",
            params={"symbols": ",".join(_normalize_symbols(symbols)), "feed": self._config.feed},
        )
        bars = response.get("bars")
        if not isinstance(bars, dict):
            raise LlmEquityContractError("Alpaca latest bars response missing bars object")
        return {symbol: _normalize_bar(symbol, payload) for symbol, payload in bars.items()}

    def latest_quotes(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
        response = self._get(
            f"{self._config.data_base_url}/v2/stocks/quotes/latest",
            params={"symbols": ",".join(_normalize_symbols(symbols)), "feed": self._config.feed},
        )
        quotes = response.get("quotes")
        if not isinstance(quotes, dict):
            raise LlmEquityContractError("Alpaca latest quotes response missing quotes object")
        return {symbol: _normalize_quote(symbol, payload) for symbol, payload in quotes.items()}

    def stock_snapshots(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Return minute, session, previous-session, and quote data without conflating bars."""
        response = self._get(
            f"{self._config.data_base_url}/v2/stocks/snapshots",
            params={"symbols": ",".join(_normalize_symbols(symbols)), "feed": self._config.feed},
        )
        rows: Mapping[str, Any]
        nested = response.get("snapshots")
        if isinstance(nested, Mapping):
            rows = nested
        else:
            rows = response
        snapshots: dict[str, dict[str, Any]] = {}
        for raw_symbol, payload in rows.items():
            try:
                symbol = normalize_symbol(str(raw_symbol))
                row = _mapping(payload, f"snapshot[{symbol}]")
                snapshots[symbol] = {
                    "minute_bar": _normalize_bar(symbol, row.get("minuteBar")),
                    "session_bar": _normalize_bar(symbol, row.get("dailyBar")),
                    "previous_daily_bar": _normalize_bar(symbol, row.get("prevDailyBar")),
                    "latest_quote": _normalize_quote(symbol, row.get("latestQuote")),
                }
            except LlmEquityContractError:
                continue
        if not snapshots:
            raise LlmEquityContractError("Alpaca snapshots response has no complete stock snapshots")
        return snapshots

    def historical_bars(
        self,
        symbols: Sequence[str],
        *,
        timeframe: str = "1Hour",
        start: datetime,
        end: datetime | None = None,
        adjustment: str = "split",
        limit: int = 10_000,
        max_pages: int = 5,
        require_complete: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        if adjustment not in {"raw", "split", "dividend", "all"}:
            raise LlmEquityContractError("Alpaca historical bar adjustment is invalid")
        collected: dict[str, list[dict[str, Any]]] = {}
        params: dict[str, str] = {
            "symbols": ",".join(_normalize_symbols(symbols)),
            "timeframe": timeframe,
            "start": _format_alpaca_query_datetime(start),
            "feed": self._config.feed,
            "adjustment": adjustment,
            "sort": "asc",
            "limit": str(limit),
        }
        if end is not None:
            params["end"] = _format_alpaca_query_datetime(end)
        for _ in range(max(max_pages, 1)):
            response = self._get(f"{self._config.data_base_url}/v2/stocks/bars", params=params)
            bars = response.get("bars")
            if bars is None:
                bars = {}
            if not isinstance(bars, dict):
                raise LlmEquityContractError("Alpaca historical bars response missing bars object")
            for symbol, rows in bars.items():
                if not isinstance(rows, Sequence) or isinstance(rows, str | bytes | bytearray):
                    raise LlmEquityContractError(
                        f"Alpaca historical bars for {symbol} must be an array"
                    )
                collected.setdefault(symbol, []).extend(
                    _normalize_bar(symbol, row) for row in rows
                )
            next_page_token = response.get("next_page_token")
            if not isinstance(next_page_token, str) or not next_page_token:
                break
            params["page_token"] = next_page_token
        else:
            if require_complete:
                raise LlmEquityContractError(
                    "Alpaca historical bars exceeded the configured page limit"
                )
        return collected

    def historical_quotes(
        self,
        symbols: Sequence[str],
        *,
        start: datetime,
        end: datetime | None = None,
        limit: int = 10_000,
        max_pages: int = 20,
        require_complete: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return timestamped historical quotes from the configured Alpaca feed."""

        collected: dict[str, list[dict[str, Any]]] = {}
        for page in self.iter_historical_quote_pages(
            symbols,
            start=start,
            end=end,
            limit=limit,
            max_pages=max_pages,
            require_complete=require_complete,
        ):
            for symbol, rows in page.items():
                collected.setdefault(symbol, []).extend(rows)
        return collected

    def iter_historical_quote_pages(
        self,
        symbols: Sequence[str],
        *,
        start: datetime,
        end: datetime | None = None,
        limit: int = 10_000,
        max_pages: int = 20,
        require_complete: bool = False,
    ) -> Iterator[dict[str, list[dict[str, Any]]]]:
        """Yield normalized quote pages without retaining prior pages in memory."""

        params: dict[str, str] = {
            "symbols": ",".join(_normalize_symbols(symbols)),
            "start": _format_alpaca_query_datetime(start),
            "feed": self._config.feed,
            "sort": "asc",
            "limit": str(limit),
        }
        if end is not None:
            params["end"] = _format_alpaca_query_datetime(end)
        for _ in range(max(max_pages, 1)):
            response = self._get(f"{self._config.data_base_url}/v2/stocks/quotes", params=params)
            quotes = response.get("quotes", {})
            if not isinstance(quotes, Mapping):
                raise LlmEquityContractError("Alpaca historical quotes response missing quotes")
            page: dict[str, list[dict[str, Any]]] = {}
            for raw_symbol, rows in quotes.items():
                symbol = normalize_symbol(str(raw_symbol))
                if not isinstance(rows, Sequence) or isinstance(rows, str | bytes | bytearray):
                    raise LlmEquityContractError(
                        f"Alpaca historical quotes for {symbol} must be an array"
                    )
                page[symbol] = [_normalize_historical_quote(symbol, row) for row in rows]
            yield page
            next_page_token = response.get("next_page_token")
            if not isinstance(next_page_token, str) or not next_page_token:
                break
            params["page_token"] = next_page_token
        else:
            if require_complete:
                raise LlmEquityContractError(
                    "Alpaca historical quotes exceeded the configured page limit"
                )

    def historical_news(
        self,
        symbols: Sequence[str],
        *,
        start: datetime,
        end: datetime | None = None,
        limit: int = 50,
        max_pages: int = 100,
        require_complete: bool = False,
    ) -> list[dict[str, Any]]:
        """Return bounded news metadata and summaries, never article bodies.

        Alpaca accepts at most 50 news rows per page; larger requested page
        sizes are clamped and completeness is determined by pagination.
        """

        params: dict[str, str] = {
            "symbols": ",".join(_normalize_symbols(symbols)),
            "start": _format_alpaca_query_datetime(start),
            "sort": "asc",
            "limit": str(max(1, min(limit, 50))),
            "include_content": "false",
        }
        if end is not None:
            params["end"] = _format_alpaca_query_datetime(end)
        collected: list[dict[str, Any]] = []
        for _ in range(max(max_pages, 1)):
            response = self._get(f"{self._config.data_base_url}/v1beta1/news", params=params)
            rows = response.get("news", ())
            if not isinstance(rows, Sequence) or isinstance(rows, str | bytes | bytearray):
                raise LlmEquityContractError("Alpaca historical news response missing news array")
            collected.extend(_normalize_news(row) for row in rows)
            next_page_token = response.get("next_page_token")
            if not isinstance(next_page_token, str) or not next_page_token:
                break
            params["page_token"] = next_page_token
        else:
            if require_complete:
                raise LlmEquityContractError(
                    "Alpaca historical news exceeded the configured page limit"
                )
        return collected

    def historical_corporate_actions(
        self,
        symbols: Sequence[str],
        *,
        start: date,
        end: date,
        limit: int = 1000,
        max_pages: int = 20,
        require_complete: bool = False,
    ) -> list[dict[str, Any]]:
        """Return historical corporate-action records without current asset state.

        Alpaca accepts at most 1,000 actions per page; larger requested page
        sizes are clamped and completeness is determined by pagination.
        """

        if end < start:
            raise LlmEquityContractError("Alpaca corporate-action end cannot precede start")
        params: dict[str, str] = {
            "symbols": ",".join(_normalize_symbols(symbols)),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sort": "asc",
            "limit": str(max(1, min(int(limit), 1000))),
        }
        collected: list[dict[str, Any]] = []
        for _ in range(max(max_pages, 1)):
            response = self._get(
                f"{self._config.data_base_url}/v1/corporate-actions", params=params
            )
            families = response.get("corporate_actions", {})
            if not isinstance(families, Mapping):
                raise LlmEquityContractError(
                    "Alpaca historical corporate actions response is malformed"
                )
            for raw_family, raw_rows in families.items():
                family = str(raw_family).strip()
                if not family or not isinstance(raw_rows, Sequence) or isinstance(
                    raw_rows, str | bytes | bytearray
                ):
                    raise LlmEquityContractError(
                        "Alpaca historical corporate action family is malformed"
                    )
                for raw_row in raw_rows:
                    row = dict(_mapping(raw_row, "historical corporate action"))
                    if "family" in row:
                        raise LlmEquityContractError(
                            "Alpaca historical corporate action contains a reserved field"
                        )
                    collected.append({"family": family, **row})
            next_page_token = response.get("next_page_token")
            if not isinstance(next_page_token, str) or not next_page_token:
                break
            params["page_token"] = next_page_token
        else:
            if require_complete:
                raise LlmEquityContractError(
                    "Alpaca historical corporate actions exceeded the configured page limit"
                )
        return collected

    def market_calendar(self, *, start: date, end: date) -> list[dict[str, str]]:
        """Return official session opens/closes without consulting the current clock."""

        if end < start:
            raise LlmEquityContractError("Alpaca calendar end cannot precede start")
        response = self._request_json(
            f"{self._config.trading_base_url}/v2/calendar",
            params={"start": start.isoformat(), "end": end.isoformat()},
        )
        if not isinstance(response, Sequence) or isinstance(response, str | bytes | bytearray):
            raise LlmEquityContractError("Alpaca calendar response must be an array")
        return [_normalize_calendar_row(row) for row in response]

    def active_assets(self) -> list[dict[str, Any]]:
        response = self._request_json(
            f"{self._config.trading_base_url}/v2/assets",
            params={"status": "active", "asset_class": "us_equity"},
        )
        if not isinstance(response, Sequence) or isinstance(response, str | bytes | bytearray):
            raise LlmEquityContractError("Alpaca assets response must be an array")
        assets: list[dict[str, Any]] = []
        for item in response:
            asset = _normalize_asset(item)
            if asset is not None:
                assets.append(asset)
        return assets

    def top_movers(self, *, top: int) -> list[dict[str, Any]]:
        bounded_top = max(1, min(int(top), ALPACA_MOVERS_MAX_TOP))
        response = self._get(
            f"{self._config.data_base_url}/v1beta1/screener/stocks/movers",
            params={"top": str(bounded_top)},
        )
        movers: list[dict[str, Any]] = []
        for family in ("gainers", "losers"):
            rows = response.get(family, [])
            if not isinstance(rows, Sequence) or isinstance(rows, str | bytes | bytearray):
                raise LlmEquityContractError(f"Alpaca movers response {family} must be an array")
            for item in rows:
                mover = _normalize_mover(item, family)
                if mover is not None:
                    movers.append(mover)
        return movers

    def most_active(self, *, top: int = ALPACA_MOST_ACTIVE_MAX_TOP) -> list[dict[str, Any]]:
        bounded_top = max(1, min(int(top), ALPACA_MOST_ACTIVE_MAX_TOP))
        response = self._get(
            f"{self._config.data_base_url}/v1beta1/screener/stocks/most-actives",
            params={"top": str(bounded_top), "by": "volume"},
        )
        rows = response.get("most_actives", [])
        if not isinstance(rows, Sequence) or isinstance(rows, str | bytes | bytearray):
            raise LlmEquityContractError("Alpaca most-active response must contain an array")
        active: list[dict[str, Any]] = []
        for item in rows:
            try:
                row = _mapping(item, "most_active")
                active.append({
                    "symbol": normalize_symbol(str(row.get("symbol", ""))),
                    "volume": _optional_number(row.get("volume")),
                    "trade_count": _optional_number(row.get("trade_count")),
                })
            except LlmEquityContractError:
                continue
        return active

    def market_clock(self) -> dict[str, Any]:
        response = self._get(f"{self._config.trading_base_url}/v2/clock", params={})
        timestamp = _parse_alpaca_datetime(str(response.get("timestamp", "")))
        return {
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "is_open": bool(response.get("is_open", False)),
            "next_open": str(response.get("next_open", "")),
            "next_close": str(response.get("next_close", "")),
        }

    def _get(self, url: str, *, params: Mapping[str, str]) -> Mapping[str, Any]:
        payload = self._request_json(url, params=params)
        if not isinstance(payload, Mapping):
            raise LlmEquityContractError("Alpaca response must be a JSON object")
        return payload

    def _request_json(self, url: str, *, params: Mapping[str, str]) -> Any:
        import httpx

        raw_attempts = os.environ.get("TRAIDAN_ALPACA_READ_ATTEMPTS", "8")
        try:
            configured_attempts = int(raw_attempts)
        except ValueError:
            configured_attempts = 8
        attempts = max(1, min(configured_attempts, 20))
        request = self._http_client.get if self._http_client is not None else httpx.get
        for attempt in range(attempts):
            try:
                response = request(
                    url,
                    headers={
                        "APCA-API-KEY-ID": self._config.key_id,
                        "APCA-API-SECRET-KEY": self._config.secret_key,
                    },
                    params=params,
                    timeout=self._config.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                status = (
                    exc.response.status_code
                    if isinstance(exc, httpx.HTTPStatusError)
                    else None
                )
                retryable = status in {408, 425, 429, 500, 502, 503, 504} or status is None
                if not retryable or attempt + 1 >= attempts:
                    raise
                retry_after = 0.0
                if isinstance(exc, httpx.HTTPStatusError):
                    raw_retry_after = exc.response.headers.get("retry-after", "")
                    try:
                        retry_after = float(raw_retry_after)
                    except ValueError:
                        retry_after = 0.0
                delay = min(max(retry_after, 0.25 * (2**attempt)), 30.0)
                time.sleep(delay)
        raise RuntimeError("unreachable Alpaca retry state")


def _normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(normalize_symbol(symbol) for symbol in symbols)
    if not normalized:
        raise LlmEquityContractError("at least one symbol is required")
    return normalized


def _normalize_asset(payload: Any) -> dict[str, Any] | None:
    try:
        row = _mapping(payload, "asset")
        symbol = normalize_symbol(str(row.get("symbol", "")))
    except LlmEquityContractError:
        return None
    asset_class = str(row.get("class", row.get("asset_class", ""))).strip()
    status = str(row.get("status", "")).strip()
    if asset_class != "us_equity" or status != "active" or row.get("tradable") is not True:
        return None
    return {
        "symbol": symbol,
        "name": str(row.get("name", "")).strip(),
        "asset_class": asset_class,
        "exchange": str(row.get("exchange", "")).strip(),
        "shortable": bool(row.get("shortable", False)),
        "fractionable": bool(row.get("fractionable", False)),
    }


def _normalize_mover(payload: Any, family: str) -> dict[str, Any] | None:
    try:
        row = _mapping(payload, "mover")
        symbol = normalize_symbol(str(row.get("symbol", "")))
        percent_change = _optional_number(row.get("percent_change"))
    except LlmEquityContractError:
        return None
    return {
        "symbol": symbol,
        "family": family,
        "percent_change": percent_change,
    }


def _normalize_base_url(value: str) -> str:
    trimmed = value.strip().rstrip("/")
    if trimmed.endswith("/v2"):
        return trimmed[: -len("/v2")]
    return trimmed


def _normalize_bar(symbol: str, payload: Any) -> dict[str, Any]:
    row = _mapping(payload, f"bar[{symbol}]")
    return {
        "timestamp": _format_alpaca_timestamp(row.get("t")),
        "open": _number(row.get("o"), "open"),
        "high": _number(row.get("h"), "high"),
        "low": _number(row.get("l"), "low"),
        "close": _number(row.get("c"), "close"),
        "volume": _number(row.get("v"), "volume"),
        "source": "alpaca_market_data",
    }


def _normalize_quote(symbol: str, payload: Any) -> dict[str, Any]:
    row = _mapping(payload, f"quote[{symbol}]")
    timestamp = _format_alpaca_timestamp(row.get("t"))
    bid = _number(row.get("bp"), "bid")
    ask = _number(row.get("ap"), "ask")
    if bid <= 0.0 or ask <= 0.0:
        return {
            "timestamp": timestamp,
            "bid": 0.0,
            "ask": 0.0,
            "source": "alpaca_market_data_unavailable",
        }
    return {
        "timestamp": timestamp,
        "bid": bid,
        "ask": ask,
        "source": "alpaca_market_data",
    }


def _normalize_historical_quote(symbol: str, payload: Any) -> dict[str, Any]:
    quote = _normalize_quote(symbol, payload)
    if "unavailable" in quote["source"]:
        return quote
    row = _mapping(payload, f"quote[{symbol}]")
    return {
        **quote,
        "bid_size": _optional_number(row.get("bs")),
        "ask_size": _optional_number(row.get("as")),
        "bid_exchange": str(row.get("bx", "")).strip()[:16],
        "ask_exchange": str(row.get("ax", "")).strip()[:16],
        "conditions": [str(item)[:32] for item in row.get("c", ())]
        if isinstance(row.get("c", ()), Sequence)
        and not isinstance(row.get("c", ()), str | bytes | bytearray)
        else [],
    }


def _normalize_news(payload: Any) -> dict[str, Any]:
    row = _mapping(payload, "news")
    identifier = str(row.get("id", "")).strip()
    headline = str(row.get("headline", "")).strip()[:500]
    summary = str(row.get("summary", "")).strip()[:2_000]
    if not identifier or not headline:
        raise LlmEquityContractError("Alpaca historical news id and headline are required")
    symbols_value = row.get("symbols", ())
    if not isinstance(symbols_value, Sequence) or isinstance(
        symbols_value, str | bytes | bytearray
    ):
        raise LlmEquityContractError("Alpaca historical news symbols must be an array")
    symbols: list[str] = []
    for raw_symbol in symbols_value:
        try:
            symbol = normalize_symbol(str(raw_symbol))
        except LlmEquityContractError:
            continue
        if symbol not in symbols:
            symbols.append(symbol)
    return {
        "id": identifier,
        "headline": headline,
        "summary": summary,
        "created_at": _format_alpaca_timestamp(row.get("created_at")),
        "updated_at": _format_alpaca_timestamp(row.get("updated_at", row.get("created_at"))),
        "url": str(row.get("url", "")).strip()[:2_000],
        "symbols": symbols,
        "source": str(row.get("source", "")).strip()[:120],
    }


def _normalize_calendar_row(payload: Any) -> dict[str, str]:
    row = _mapping(payload, "calendar")
    session_date = str(row.get("date", "")).strip()
    open_time = str(row.get("open", "")).strip()
    close_time = str(row.get("close", "")).strip()
    try:
        date.fromisoformat(session_date)
        datetime.strptime(open_time, "%H:%M").replace(tzinfo=UTC)
        datetime.strptime(close_time, "%H:%M").replace(tzinfo=UTC)
    except ValueError as exc:
        raise LlmEquityContractError("Alpaca calendar row is invalid") from exc
    return {"session_date": session_date, "open": open_time, "close": close_time}


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LlmEquityContractError(f"{label} must be an object")
    return value


def _number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise LlmEquityContractError(f"Alpaca {label} must be numeric")
    parsed = float(value)
    if not (parsed < float("inf") and parsed > -float("inf")):
        raise LlmEquityContractError(f"Alpaca {label} must be finite")
    return parsed


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    return _number(value, "percent_change")


def _format_alpaca_query_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        raise LlmEquityContractError("Alpaca query datetime must include timezone")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _format_alpaca_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise LlmEquityContractError("Alpaca timestamp is required")
    return _parse_alpaca_datetime(value).isoformat().replace("+00:00", "Z")


def _parse_alpaca_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise LlmEquityContractError("Alpaca timestamp must include timezone")
    return parsed.astimezone(UTC)
