"""
Base Data Transformer Module

Provides common transformation operations for ETL pipelines.
"""

import logging
from typing import Callable, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Base transformer class with common transformation operations."""

    def __init__(self, df: pd.DataFrame):
        """
        Initialize transformer with a DataFrame.

        Args:
            df: Input DataFrame to transform
        """
        self.df = df.copy()
        self._original_row_count = len(df)

    @property
    def row_count(self) -> int:
        """Return current row count."""
        return len(self.df)

    @property
    def column_count(self) -> int:
        """Return current column count."""
        return len(self.df.columns)

    def get_result(self) -> pd.DataFrame:
        """
        Get the transformed DataFrame.

        Returns:
            Transformed DataFrame
        """
        logger.info(
            f"Transformation complete: {self._original_row_count} -> {len(self.df)} rows"
        )
        return self.df

    def drop_nulls(
        self,
        columns: Optional[List[str]] = None,
        how: str = "any"
    ) -> "DataTransformer":
        """
        Drop rows with null values.

        Args:
            columns: Specific columns to check
            how: 'any' or 'all'

        Returns:
            Self for method chaining
        """
        before_count = len(self.df)
        self.df = self.df.dropna(subset=columns, how=how)
        logger.info(f"Dropped {before_count - len(self.df)} rows with null values")
        return self

    def fill_nulls(
        self,
        fill_values: Dict[str, any]
    ) -> "DataTransformer":
        """
        Fill null values with specified values.

        Args:
            fill_values: Dictionary of column names to fill values

        Returns:
            Self for method chaining
        """
        self.df = self.df.fillna(fill_values)
        logger.info(f"Filled null values in columns: {list(fill_values.keys())}")
        return self

    def drop_duplicates(
        self,
        columns: Optional[List[str]] = None,
        keep: str = "first"
    ) -> "DataTransformer":
        """
        Drop duplicate rows.

        Args:
            columns: Columns to consider for duplicates
            keep: 'first', 'last', or False

        Returns:
            Self for method chaining
        """
        before_count = len(self.df)
        self.df = self.df.drop_duplicates(subset=columns, keep=keep)
        logger.info(f"Dropped {before_count - len(self.df)} duplicate rows")
        return self

    def rename_columns(
        self,
        column_mapping: Dict[str, str]
    ) -> "DataTransformer":
        """
        Rename columns.

        Args:
            column_mapping: Dictionary of old to new column names

        Returns:
            Self for method chaining
        """
        self.df = self.df.rename(columns=column_mapping)
        logger.info(f"Renamed columns: {column_mapping}")
        return self

    def select_columns(
        self,
        columns: List[str]
    ) -> "DataTransformer":
        """
        Select specific columns.

        Args:
            columns: List of columns to keep

        Returns:
            Self for method chaining
        """
        self.df = self.df[columns]
        logger.info(f"Selected {len(columns)} columns")
        return self

    def add_column(
        self,
        column_name: str,
        value: Union[any, Callable]
    ) -> "DataTransformer":
        """
        Add a new column.

        Args:
            column_name: Name of the new column
            value: Static value or function to apply

        Returns:
            Self for method chaining
        """
        if callable(value):
            self.df[column_name] = self.df.apply(value, axis=1)
        else:
            self.df[column_name] = value
        logger.info(f"Added column: {column_name}")
        return self

    def convert_types(
        self,
        type_mapping: Dict[str, str]
    ) -> "DataTransformer":
        """
        Convert column data types.

        Args:
            type_mapping: Dictionary of column names to data types

        Returns:
            Self for method chaining
        """
        for column, dtype in type_mapping.items():
            if column in self.df.columns:
                if dtype == "datetime":
                    self.df[column] = pd.to_datetime(self.df[column])
                elif dtype == "float":
                    self.df[column] = pd.to_numeric(self.df[column], errors="coerce")
                elif dtype == "int":
                    self.df[column] = pd.to_numeric(
                        self.df[column], errors="coerce"
                    ).astype("Int64")
                else:
                    self.df[column] = self.df[column].astype(dtype)
        logger.info(f"Converted types for columns: {list(type_mapping.keys())}")
        return self

    def filter_rows(
        self,
        condition: Union[pd.Series, Callable]
    ) -> "DataTransformer":
        """
        Filter rows based on condition.

        Args:
            condition: Boolean Series or callable that returns one

        Returns:
            Self for method chaining
        """
        before_count = len(self.df)
        if callable(condition):
            self.df = self.df[condition(self.df)]
        else:
            self.df = self.df[condition]
        logger.info(f"Filtered {before_count - len(self.df)} rows")
        return self

    def standardize_text(
        self,
        columns: List[str],
        case: str = "upper"
    ) -> "DataTransformer":
        """
        Standardize text in columns.

        Args:
            columns: Columns to standardize
            case: 'upper', 'lower', or 'title'

        Returns:
            Self for method chaining
        """
        for col in columns:
            if col in self.df.columns:
                if case == "upper":
                    self.df[col] = self.df[col].str.upper()
                elif case == "lower":
                    self.df[col] = self.df[col].str.lower()
                elif case == "title":
                    self.df[col] = self.df[col].str.title()
                self.df[col] = self.df[col].str.strip()
        logger.info(f"Standardized text in columns: {columns}")
        return self

    def apply_custom(
        self,
        func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "DataTransformer":
        """
        Apply a custom transformation function.

        Args:
            func: Function that takes and returns a DataFrame

        Returns:
            Self for method chaining
        """
        self.df = func(self.df)
        logger.info("Applied custom transformation")
        return self
