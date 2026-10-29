"""Compatibility package for the renamed Trading Master (TM)."""

from trading_master import trading_master as _trading_master

globals().update(
    {
        name: getattr(_trading_master, name)
        for name in getattr(
            _trading_master, "__all__", [key for key in vars(_trading_master) if not key.startswith("_")]
        )
    }
)
