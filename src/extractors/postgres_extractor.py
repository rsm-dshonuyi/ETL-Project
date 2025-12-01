"""
PostgreSQL Data Extractor Module

Extracts data from PostgreSQL databases.
"""

import logging
from contextlib import contextmanager
from typing import Generator, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class PostgresExtractor:
    """Extractor for PostgreSQL data sources."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        """
        Initialize PostgreSQL extractor.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self._connection = None

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def extract(
        self,
        query: str,
        params: Optional[tuple] = None
    ) -> pd.DataFrame:
        """
        Extract data using a SQL query.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            DataFrame containing the query results
        """
        logger.info(f"Executing PostgreSQL query: {query[:100]}...")

        with self._get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        logger.info(f"Extracted {len(df)} rows from PostgreSQL")
        return df

    def extract_table(
        self,
        table_name: str,
        columns: Optional[list] = None,
        where_clause: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract data from a specific table.

        Args:
            table_name: Name of the table
            columns: Specific columns to select
            where_clause: WHERE clause conditions
            limit: Maximum number of rows

        Returns:
            DataFrame containing the table data
        """
        col_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_str} FROM {table_name}"

        if where_clause:
            query += f" WHERE {where_clause}"

        if limit:
            query += f" LIMIT {limit}"

        return self.extract(query)

    def extract_in_chunks(
        self,
        query: str,
        chunk_size: int = 10000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Extract data in chunks using server-side cursor.

        Args:
            query: SQL query to execute
            chunk_size: Number of rows per chunk

        Yields:
            DataFrame chunks
        """
        logger.info(f"Extracting in chunks: {query[:100]}...")

        with self._get_connection() as conn:
            with conn.cursor(name="etl_cursor", cursor_factory=RealDictCursor) as cursor:
                cursor.itersize = chunk_size
                cursor.execute(query)

                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield pd.DataFrame(rows)

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame with column information
        """
        query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        return self.extract(query, (table_name,))
