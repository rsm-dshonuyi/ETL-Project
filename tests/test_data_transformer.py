"""
Tests for Data Transformer
"""

import pandas as pd
import pytest

from src.transformers.data_transformer import DataTransformer


class TestDataTransformer:
    """Tests for DataTransformer class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            "id": [1, 2, 3, 3, 4],
            "name": ["  Alice  ", "Bob", "Charlie", "Charlie", None],
            "value": [100, 200, None, 300, 400],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
        })

    def test_drop_nulls(self, sample_df):
        """Test dropping null values."""
        transformer = DataTransformer(sample_df)
        result = transformer.drop_nulls(columns=["name"]).get_result()

        assert len(result) == 4
        assert result["name"].isna().sum() == 0

    def test_drop_duplicates(self, sample_df):
        """Test dropping duplicate rows."""
        transformer = DataTransformer(sample_df)
        result = transformer.drop_duplicates(columns=["id"]).get_result()

        assert len(result) == 4

    def test_rename_columns(self, sample_df):
        """Test renaming columns."""
        transformer = DataTransformer(sample_df)
        result = transformer.rename_columns({"id": "ID", "name": "NAME"}).get_result()

        assert "ID" in result.columns
        assert "NAME" in result.columns
        assert "id" not in result.columns

    def test_select_columns(self, sample_df):
        """Test selecting specific columns."""
        transformer = DataTransformer(sample_df)
        result = transformer.select_columns(["id", "name"]).get_result()

        assert len(result.columns) == 2
        assert "value" not in result.columns

    def test_add_column_static(self, sample_df):
        """Test adding a static column."""
        transformer = DataTransformer(sample_df)
        result = transformer.add_column("category", "A").get_result()

        assert "category" in result.columns
        assert (result["category"] == "A").all()

    def test_add_column_callable(self, sample_df):
        """Test adding a calculated column."""
        transformer = DataTransformer(sample_df)
        result = transformer.add_column(
            "doubled",
            lambda row: row["value"] * 2 if pd.notna(row["value"]) else 0
        ).get_result()

        assert "doubled" in result.columns
        assert result.iloc[0]["doubled"] == 200

    def test_fill_nulls(self, sample_df):
        """Test filling null values."""
        transformer = DataTransformer(sample_df)
        result = transformer.fill_nulls({"name": "Unknown", "value": 0}).get_result()

        assert result["name"].isna().sum() == 0
        assert result["value"].isna().sum() == 0
        assert (result.loc[result["id"] == 4, "name"] == "Unknown").all()

    def test_standardize_text(self, sample_df):
        """Test text standardization."""
        transformer = DataTransformer(sample_df)
        result = transformer.standardize_text(["name"], case="upper").get_result()

        assert result.iloc[0]["name"] == "ALICE"
        assert result.iloc[1]["name"] == "BOB"

    def test_convert_types(self, sample_df):
        """Test type conversion."""
        transformer = DataTransformer(sample_df)
        result = transformer.convert_types({"date": "datetime"}).get_result()

        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_filter_rows(self, sample_df):
        """Test row filtering."""
        transformer = DataTransformer(sample_df)
        result = transformer.filter_rows(lambda df: df["value"] > 150).get_result()

        assert len(result) == 3
        assert (result["value"] > 150).all()

    def test_method_chaining(self, sample_df):
        """Test method chaining."""
        transformer = DataTransformer(sample_df)
        result = (
            transformer
            .drop_nulls(columns=["name"])
            .drop_duplicates(columns=["id"])
            .rename_columns({"id": "ID"})
            .select_columns(["ID", "name"])
            .get_result()
        )

        assert len(result) == 3
        assert "ID" in result.columns
        assert len(result.columns) == 2
