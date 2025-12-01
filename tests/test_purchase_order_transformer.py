"""
Tests for Purchase Order Transformer
"""

import pandas as pd
import pytest

from src.transformers.purchase_order_transformer import PurchaseOrderTransformer


class TestPurchaseOrderTransformer:
    """Tests for PurchaseOrderTransformer class."""

    @pytest.fixture
    def sample_po_df(self):
        """Create sample purchase order DataFrame."""
        return pd.DataFrame({
            "po_number": ["PO-001", "PO-002", "PO-003"],
            "order_date": ["2024-01-15", "2024-06-20", "2024-10-05"],
            "vendor": ["Acme Corp", "Global Supplies", "Tech Parts"],
            "item": ["Widget A", "Component B", "Gadget C"],
            "quantity": [100, 50, 200],
            "unit_price": [25.50, 150.00, 10.25],
            "status": ["Completed", "Pending", "Completed"],
            "location": ["New York", "Los Angeles", "Chicago"]
        })

    def test_standardize_columns(self, sample_po_df):
        """Test column standardization."""
        transformer = PurchaseOrderTransformer(sample_po_df)
        result = transformer.standardize_columns().get_result()

        assert "PURCHASE_ORDER_ID" in result.columns
        assert "ORDER_DATE" in result.columns
        assert "VENDOR_NAME" in result.columns
        assert "po_number" not in result.columns

    def test_calculate_totals(self, sample_po_df):
        """Test total calculation."""
        transformer = PurchaseOrderTransformer(sample_po_df)
        result = (
            transformer
            .standardize_columns()
            .calculate_totals()
            .get_result()
        )

        assert "TOTAL_AMOUNT" in result.columns
        assert result.iloc[0]["TOTAL_AMOUNT"] == 100 * 25.50

    def test_categorize_orders(self, sample_po_df):
        """Test order categorization."""
        transformer = PurchaseOrderTransformer(sample_po_df)
        result = (
            transformer
            .standardize_columns()
            .calculate_totals()
            .categorize_orders()
            .get_result()
        )

        assert "ORDER_CATEGORY" in result.columns
        # 100 * 25.50 = 2550 -> LARGE
        assert result.iloc[0]["ORDER_CATEGORY"] == "LARGE"
        # 50 * 150 = 7500 -> LARGE
        assert result.iloc[1]["ORDER_CATEGORY"] == "LARGE"
        # 200 * 10.25 = 2050 -> LARGE
        assert result.iloc[2]["ORDER_CATEGORY"] == "LARGE"

    def test_add_fiscal_info(self, sample_po_df):
        """Test fiscal year and quarter addition."""
        transformer = PurchaseOrderTransformer(sample_po_df)
        result = (
            transformer
            .standardize_columns()
            .add_fiscal_info()
            .get_result()
        )

        assert "FISCAL_YEAR" in result.columns
        assert "FISCAL_QUARTER" in result.columns
        assert result.iloc[0]["FISCAL_YEAR"] == 2024  # January
        assert result.iloc[0]["FISCAL_QUARTER"] == 1

    def test_validate_order_dates(self, sample_po_df):
        """Test date validation."""
        # Add an invalid date
        sample_po_df.loc[3] = [
            "PO-004", "invalid-date", "Test", "Item", 10, 10, "Pending", "Test"
        ]

        transformer = PurchaseOrderTransformer(sample_po_df)
        result = (
            transformer
            .standardize_columns()
            .validate_order_dates()
            .get_result()
        )

        # Invalid date should be dropped
        assert len(result) == 3

    def test_aggregate_by_vendor(self, sample_po_df):
        """Test vendor aggregation."""
        # Add another order for same vendor
        sample_po_df.loc[3] = [
            "PO-004", "2024-01-20", "Acme Corp", "Widget B", 50, 30, "Pending", "NYC"
        ]

        transformer = PurchaseOrderTransformer(sample_po_df)
        transformer.standardize_columns().calculate_totals()
        result = transformer.aggregate_by_vendor()

        assert len(result) == 3  # 3 unique vendors
        acme = result[result["VENDOR_NAME"] == "Acme Corp"].iloc[0]
        assert acme["PURCHASE_ORDER_ID_count"] == 2
