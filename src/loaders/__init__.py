"""
Data Loaders Package

This package provides loaders for Snowflake data warehouse.
"""


def __getattr__(name):
    """Lazy import loaders to avoid requiring all dependencies."""
    if name == "SnowflakeLoader":
        from src.loaders.snowflake_loader import SnowflakeLoader
        return SnowflakeLoader
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["SnowflakeLoader"]
