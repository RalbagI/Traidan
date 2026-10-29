"""Full allocation is mandatory; model direction and failure remain auditable."""

import copy

import pytest
from llm_equity.contracts import LlmEquityContractError

from llm_equity import fully_invested as f


def review():
    return {
        "schema_version": 1,
        "holdings": [
            {
                "symbol": f"STK{i}",
                "weight_bps": 500,
                "thesis": "best available",
                "invalidation": "review on changed evidence",
                "horizon": "1d",
                "expected_excess_return_bps": -12.0,
            }
            for i in range(20)
        ],
        "rationale": "Fully invested despite negative forecasts",
    }


def test_complete_portfolio_accepts_honest_negative_forecasts():
    result = f.validate_review(review(), {f"STK{i}" for i in range(25)})
    assert len(result["holdings"]) == 20
    assert result["holdings"][0]["expected_excess_return_bps"] == -12


@pytest.mark.parametrize(
    "mutation",
    [
        lambda r: r["holdings"].pop(),
        lambda r: r["holdings"][0].update(weight_bps=501),
        lambda r: r["holdings"][0].update(weight_bps=499),
        lambda r: r["holdings"][0].update(symbol="SGOV"),
        lambda r: r["holdings"][0].update(symbol="STK1"),
        lambda r: r.update(verdict="abstain"),
        lambda r: r["holdings"][0].update(weight_bps=True),
    ],
)
def test_invalid_allocations_cannot_become_orders(mutation):
    value = review()
    mutation(value)
    with pytest.raises(LlmEquityContractError):
        f.validate_review(value, {f"STK{i}" for i in range(25)})


@pytest.mark.parametrize(
    "symbol,name,expected",
    [
        ("SPY", "SPDR S&P 500 ETF Trust", "equity_etf"),
        ("TLT", "iShares 20+ Year Treasury Bond ETF", "excluded"),
        ("GLD", "SPDR Gold Shares", "excluded"),
        ("SGOV", "iShares 0-3 Month Treasury Bond ETF", "excluded"),
        ("IBIT", "iShares Bitcoin Trust ETF", "excluded"),
        ("SQQQ", "ProShares UltraPro Short QQQ", "excluded"),
        ("AAPL", "Apple Inc. Common Stock", "equity_stock"),
        ("XYZ", "Example plc Ordinary Shares", "equity_stock"),
        ("UNKNOWN", "Unclassified investment product", "excluded"),
    ],
)
def test_only_identified_stocks_and_equity_etfs_are_eligible(symbol, name, expected):
    assert f.investment_kind(symbol, name) == expected


def test_review_packet_includes_twenty_plus_candidates_without_positive_edge():
    metadata = {
        f"STK{i}": {"tradable": True, "fractionable": True, "investment_kind": "equity_stock"}
        for i in range(30)
    }
    quotes = {k: {"bid": 99, "ask": 101} for k in metadata}
    context = {
        "portfolio": {"cash_usd": 10000, "nav_usd": 10000, "positions": []},
        "market_research_snapshot": {
            "as_of": "2026-10-22T14:00:00Z",
            "market_session": "rth",
            "asset_metadata": metadata,
            "latest_quotes": quotes,
            "latest_bars": {},
            "macro_news_summaries": [],
        },
        "ranked_trade_candidates": [
            {"symbol": k, "expected_alpha_bps": -10, "edge_gate_status": "blocked"}
            for k in metadata
        ],
    }
    packet = f.review_context(context)
    assert len(packet["candidates"]) == 30
    result = f.decision_from_review(review(), context, sequence=42)
    assert result["execution_policy"] == "fully_invested_v1"
    assert result["commands"] == []
    assert result["target_portfolio"]["account_cash_usd"] == 10000
    assert sum(r["weight_bps"] for r in result["target_portfolio"]["holdings"]) == 10000
    invalid = copy.deepcopy(context)
    invalid["market_research_snapshot"]["asset_metadata"] = {}
    with pytest.raises(LlmEquityContractError, match="20"):
        f.review_context(invalid)


def test_prior_review_and_broad_candidates_fit_budget_without_losing_held_inventory():
    import json

    from test_fully_invested_integration import packet

    ctx = packet()
    snap = ctx["market_research_snapshot"]
    for i in range(100):
        symbol = f"STK{i}"
        snap["asset_metadata"][symbol] = {
            "tradable": True,
            "fractionable": True,
            "investment_kind": "equity_stock",
        }
        snap["latest_quotes"][symbol] = {"bid": 99, "ask": 101}
        snap["latest_bars"][symbol] = {
            "timestamp": "2026-10-22T14:00:00Z",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 100000,
        }
        snap["trend_summaries"][symbol] = "א" * 300
    previous = review()
    for row in previous["holdings"]:
        row.update(thesis="א" * 300, invalidation="ב" * 200)
    ctx["previous_allocation_review"] = previous
    ctx["portfolio"]["positions"] = [{"symbol": f"STK{i}", "quantity": 1} for i in range(20)]
    result = f.review_context(ctx)
    assert len(json.dumps(result, ensure_ascii=False).encode()) <= 96000
    assert 20 <= len(result["candidates"]) <= 100
    assert {f"STK{i}" for i in range(20)} <= {r["symbol"] for r in result["candidates"]}
