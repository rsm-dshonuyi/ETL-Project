"""
Data Extractors Package

This package provides extractors for various data sources:
- CSV files
- XML files
- PostgreSQL databases
- Snowflake Marketplace
"""


def __getattr__(name):
    """Lazy import extractors to avoid requiring all dependencies."""
    if name == "CSVExtractor":
        from src.extractors.csv_extractor import CSVExtractor
        return CSVExtractor
    elif name == "XMLExtractor":
        from src.extractors.xml_extractor import XMLExtractor
        return XMLExtractor
    elif name == "PostgresExtractor":
        from src.extractors.postgres_extractor import PostgresExtractor
        return PostgresExtractor
    elif name == "SnowflakeMarketplaceExtractor":
        from src.extractors.snowflake_extractor import SnowflakeMarketplaceExtractor
        return SnowflakeMarketplaceExtractor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CSVExtractor",
    "XMLExtractor",
    "PostgresExtractor",
    "SnowflakeMarketplaceExtractor"
]
