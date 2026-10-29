"""Smoke tests for the maturin-built Rust extension."""

from __future__ import annotations

import importlib
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("TRAIDAN_EXPECT_RUST_EXTENSION") != "1",
    reason="Rust extension is built by the dedicated maturin import check",
)


def test_rust_extension_exports_version_stub() -> None:
    rust = importlib.import_module("traidan._rust")

    assert isinstance(rust.rust_engine_version(), str)
    assert rust.rust_engine_version()
