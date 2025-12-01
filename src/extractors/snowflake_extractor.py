"""
Snowflake Marketplace Data Extractor Module

Extracts weather data from Snowflake Marketplace datasets.
"""

import logging
from contextlib import contextmanager
from typing import Generator, Optional

import pandas as pd
import snowflake.connector

logger = logging.getLogger(__name__)


class SnowflakeMarketplaceExtractor:
    """Extractor for Snowflake Marketplace data sources."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str,
        role: Optional[str] = None
    ):
        """
        Initialize Snowflake Marketplace extractor.

        Args:
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            warehouse: Virtual warehouse name
            database: Database name
            schema: Schema name
            role: Role to use (optional)
        """
        self.connection_params = {
            "account": account,
            "user": user,
            "password": password,
            "warehouse": warehouse,
            "database": database,
            "schema": schema
        }
        if role:
            self.connection_params["role"] = role

    @contextmanager
    def _get_connection(self):
        """Context manager for Snowflake connections."""
        conn = None
        try:
            conn = snowflake.connector.connect(**self.connection_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def extract(
        self,
        query: str,
        params: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Extract data using a SQL query.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            DataFrame containing the query results
        """
        logger.info(f"Executing Snowflake query: {query[:100]}...")

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
            finally:
                cursor.close()

        logger.info(f"Extracted {len(df)} rows from Snowflake Marketplace")
        return df

    def extract_weather_history(
        self,
        location: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract historical weather data from Marketplace.

        Args:
            location: Filter by location
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum number of rows

        Returns:
            DataFrame containing weather history
        """
        query = """
            SELECT 
                DATE,
                CITY,
                STATE,
                COUNTRY,
                AVG_TEMPERATURE_F,
                MIN_TEMPERATURE_F,
                MAX_TEMPERATURE_F,
                PRECIPITATION_INCHES,
                HUMIDITY_PCT,
                WIND_SPEED_MPH
            FROM HISTORY
            WHERE 1=1
        """

        if location:
            query += f" AND CITY = '{location}'"
        if start_date:
            query += f" AND DATE >= '{start_date}'"
        if end_date:
            query += f" AND DATE <= '{end_date}'"

        query += " ORDER BY DATE DESC"

        if limit:
            query += f" LIMIT {limit}"

        return self.extract(query)

    def list_available_tables(self) -> pd.DataFrame:
        """
        List available tables in the Marketplace database.

        Returns:
            DataFrame with table information
        """
        query = """
            SELECT 
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
        """
        return self.extract(query)

    def extract_in_chunks(
        self,
        query: str,
        chunk_size: int = 10000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Extract data in chunks.

        Args:
            query: SQL query to execute
            chunk_size: Number of rows per chunk

        Yields:
            DataFrame chunks
        """
        # Get total count first
        count_query = f"SELECT COUNT(*) as cnt FROM ({query}) subq"
        count_df = self.extract(count_query)
        total_rows = count_df.iloc[0]["CNT"]

        logger.info(f"Total rows to extract: {total_rows}")

        offset = 0
        while offset < total_rows:
            chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
            yield self.extract(chunk_query)
            offset += chunk_size
