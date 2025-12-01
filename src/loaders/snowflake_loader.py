"""
Snowflake Data Loader Module

Loads transformed data into Snowflake data warehouse.
"""

import logging
from contextlib import contextmanager
from typing import List, Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Loader for Snowflake data warehouse."""

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
        Initialize Snowflake loader.

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

        self.database = database
        self.schema = schema

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

    def execute_query(self, query: str) -> None:
        """
        Execute a SQL query.

        Args:
            query: SQL query to execute
        """
        logger.info(f"Executing query: {query[:100]}...")

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
            finally:
                cursor.close()

    def create_table(
        self,
        table_name: str,
        columns: dict,
        if_not_exists: bool = True
    ) -> None:
        """
        Create a table in Snowflake.

        Args:
            table_name: Name of the table
            columns: Dictionary of column names and types
            if_not_exists: Whether to use IF NOT EXISTS clause
        """
        cols_sql = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        query = f"CREATE TABLE {exists_clause}{table_name} ({cols_sql})"
        self.execute_query(query)
        logger.info(f"Created table: {table_name}")

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        chunk_size: int = 10000
    ) -> int:
        """
        Load DataFrame into Snowflake table.

        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            chunk_size: Number of rows per insert batch

        Returns:
            Number of rows loaded
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to load")
            return 0

        logger.info(f"Loading {len(df)} rows into {table_name}")

        with self._get_connection() as conn:
            if if_exists == "replace":
                cursor = conn.cursor()
                try:
                    cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
                finally:
                    cursor.close()

            # Use write_pandas for efficient bulk loading
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name,
                database=self.database,
                schema=self.schema,
                chunk_size=chunk_size,
                auto_create_table=True,
                overwrite=(if_exists == "replace")
            )

            if success:
                logger.info(f"Successfully loaded {nrows} rows in {nchunks} chunks")
                return nrows
            else:
                raise RuntimeError(f"Failed to load data into {table_name}")

    def load_from_stage(
        self,
        stage_name: str,
        table_name: str,
        file_format: str = "CSV",
        pattern: Optional[str] = None
    ) -> None:
        """
        Load data from a Snowflake stage.

        Args:
            stage_name: Name of the stage
            table_name: Target table name
            file_format: File format (CSV, JSON, PARQUET)
            pattern: File pattern to match
        """
        query = f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = '{file_format}')
        """

        if pattern:
            query += f" PATTERN = '{pattern}'"

        self.execute_query(query)
        logger.info(f"Loaded data from stage {stage_name} into {table_name}")

    def merge(
        self,
        df: pd.DataFrame,
        table_name: str,
        merge_keys: List[str],
        temp_table_prefix: str = "TEMP_"
    ) -> None:
        """
        Merge DataFrame into existing table (upsert).

        Args:
            df: DataFrame to merge
            table_name: Target table name
            merge_keys: Columns to use for matching
            temp_table_prefix: Prefix for temporary staging table
        """
        temp_table = f"{temp_table_prefix}{table_name}"

        # Load to temporary table
        self.load(df, temp_table, if_exists="replace")

        # Build merge statement
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )

        columns = list(df.columns)
        update_cols = [col for col in columns if col not in merge_keys]
        update_clause = ", ".join([f"target.{col} = source.{col}" for col in update_cols])
        insert_cols = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        merge_query = f"""
            MERGE INTO {table_name} AS target
            USING {temp_table} AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})
        """

        self.execute_query(merge_query)

        # Drop temporary table
        self.execute_query(f"DROP TABLE IF EXISTS {temp_table}")

        logger.info(f"Merged {len(df)} rows into {table_name}")

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        query = f"""
            SELECT COUNT(*) as cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
            AND TABLE_NAME = '{table_name.upper()}'
        """

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] > 0
            finally:
                cursor.close()
