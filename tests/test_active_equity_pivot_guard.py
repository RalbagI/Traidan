from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ACTIVE_PATHS = [
    ROOT / "src",
    ROOT / "frontend/src",
    ROOT / "scripts/local-dry-run.sh",
    ROOT / "scripts/check-public-wss.js",
    ROOT / "README.md",
    ROOT / "architecture.md",
    ROOT / "deployment/operator-runbook.md",
    ROOT / "prompts/current_trading_master_prompt.txt",
    ROOT / "config/market_universe/llm-equity-dry-run.json",
]
FORBIDDEN_REASONING_PROFILES = ("ultra",)
FORBIDDEN_TERMS = (
    "BTC",
    "Binance",
    "Coinbase",
    "crypto",
    "phase8",
    "ETF paper strategy",
    "ai_signal",
    "llm_cycle_summary",
    "strategy-executor",
)


def test_active_runtime_paths_have_no_legacy_market_terms() -> None:
    hits: list[str] = []
    for path in ACTIVE_PATHS:
        files = [path] if path.is_file() else [item for item in path.rglob("*") if item.is_file()]
        for file_path in files:
            if file_path.suffix in {".png", ".jpg", ".jpeg", ".ico"}:
                continue
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            for term in FORBIDDEN_TERMS:
                if term in text:
                    hits.append(f"{file_path.relative_to(ROOT)}: {term}")

    assert hits == []


def test_active_runtime_paths_pin_the_xhigh_reasoning_profile() -> None:
    hits: list[str] = []
    for path in ACTIVE_PATHS:
        files = [path] if path.is_file() else [item for item in path.rglob("*") if item.is_file()]
        for file_path in files:
            if file_path.suffix in {".png", ".jpg", ".jpeg", ".ico"}:
                continue
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            for term in FORBIDDEN_REASONING_PROFILES:
                if term in text:
                    hits.append(f"{file_path.relative_to(ROOT)}: {term}")

    assert hits == []
