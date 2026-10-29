"""Keep legacy module forwarding compatible without wildcard lint suppressions."""

import runpy
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("filename", ["__init__.py", "strategist.py"])
@pytest.mark.parametrize("explicit_exports", [False, True])
def test_strategist_forwards_legacy_exports(
    monkeypatch: pytest.MonkeyPatch, filename: str, explicit_exports: bool
) -> None:
    package = ModuleType("trading_master")
    implementation = ModuleType("trading_master.trading_master")
    implementation.public_value = object()
    implementation._exported = object()
    implementation.main = lambda: 7
    if explicit_exports:
        implementation.__all__ = ["public_value", "_exported"]
    package.trading_master = implementation
    monkeypatch.setitem(sys.modules, "trading_master", package)
    monkeypatch.setitem(sys.modules, "trading_master.trading_master", implementation)

    forwarded = runpy.run_path(str(ROOT / "strategist" / filename))

    assert forwarded["public_value"] is implementation.public_value
    assert ("_exported" in forwarded) is explicit_exports
    assert ("main" in forwarded) is (not explicit_exports or filename == "strategist.py")
    if filename == "strategist.py":
        with pytest.raises(SystemExit) as exc:
            runpy.run_path(str(ROOT / "strategist" / filename), run_name="__main__")
        assert exc.value.code == 7
