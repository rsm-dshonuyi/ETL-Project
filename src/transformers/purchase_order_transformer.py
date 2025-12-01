"""
Purchase Order Transformer Module

Specialized transformations for purchase order data.
"""

import logging
from typing import Optional

import pandas as pd

from src.transformers.data_transformer import DataTransformer

logger = logging.getLogger(__name__)


class PurchaseOrderTransformer(DataTransformer):
    """Transformer for purchase order data."""

    def __init__(self, df: pd.DataFrame):
        """
        Initialize purchase order transformer.

        Args:
            df: Input DataFrame with purchase order data
        """
        super().__init__(df)

    def standardize_columns(self) -> "PurchaseOrderTransformer":
        """
        Standardize purchase order column names.

        Returns:
            Self for method chaining
        """
        column_mapping = {
            "po_number": "PURCHASE_ORDER_ID",
            "po_id": "PURCHASE_ORDER_ID",
            "order_id": "PURCHASE_ORDER_ID",
            "order_date": "ORDER_DATE",
            "date": "ORDER_DATE",
            "vendor": "VENDOR_NAME",
            "vendor_name": "VENDOR_NAME",
            "supplier": "VENDOR_NAME",
            "item": "ITEM_DESCRIPTION",
            "item_description": "ITEM_DESCRIPTION",
            "product": "ITEM_DESCRIPTION",
            "quantity": "QUANTITY",
            "qty": "QUANTITY",
            "unit_price": "UNIT_PRICE",
            "price": "UNIT_PRICE",
            "total": "TOTAL_AMOUNT",
            "total_amount": "TOTAL_AMOUNT",
            "status": "ORDER_STATUS",
            "order_status": "ORDER_STATUS",
            "location": "DELIVERY_LOCATION",
            "delivery_location": "DELIVERY_LOCATION",
        }

        # Only rename columns that exist in the DataFrame
        existing_columns = {
            k: v for k, v in column_mapping.items()
            if k in self.df.columns
        }

        if existing_columns:
            self.df = self.df.rename(columns=existing_columns)
            logger.info(f"Standardized columns: {existing_columns}")

        return self

    def calculate_totals(self) -> "PurchaseOrderTransformer":
        """
        Calculate total amount if not present.

        Returns:
            Self for method chaining
        """
        if "TOTAL_AMOUNT" not in self.df.columns:
            if "QUANTITY" in self.df.columns and "UNIT_PRICE" in self.df.columns:
                self.df["TOTAL_AMOUNT"] = (
                    self.df["QUANTITY"] * self.df["UNIT_PRICE"]
                )
                logger.info("Calculated TOTAL_AMOUNT from QUANTITY and UNIT_PRICE")
        return self

    def validate_order_dates(
        self,
        date_column: str = "ORDER_DATE",
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> "PurchaseOrderTransformer":
        """
        Validate and filter order dates.

        Args:
            date_column: Name of the date column
            min_date: Minimum valid date (YYYY-MM-DD)
            max_date: Maximum valid date (YYYY-MM-DD)

        Returns:
            Self for method chaining
        """
        if date_column not in self.df.columns:
            logger.warning(f"Date column {date_column} not found")
            return self

        # Convert to datetime if not already
        self.df[date_column] = pd.to_datetime(self.df[date_column], errors="coerce")

        before_count = len(self.df)

        # Filter by date range
        if min_date:
            self.df = self.df[self.df[date_column] >= pd.to_datetime(min_date)]
        if max_date:
            self.df = self.df[self.df[date_column] <= pd.to_datetime(max_date)]

        # Drop invalid dates
        self.df = self.df.dropna(subset=[date_column])

        logger.info(
            f"Validated dates: {before_count} -> {len(self.df)} rows"
        )
        return self

    def categorize_orders(
        self,
        amount_column: str = "TOTAL_AMOUNT"
    ) -> "PurchaseOrderTransformer":
        """
        Categorize orders by amount.

        Args:
            amount_column: Column containing order amounts

        Returns:
            Self for method chaining
        """
        if amount_column not in self.df.columns:
            logger.warning(f"Amount column {amount_column} not found")
            return self

        def categorize(amount):
            if pd.isna(amount):
                return "UNKNOWN"
            elif amount < 100:
                return "SMALL"
            elif amount < 1000:
                return "MEDIUM"
            elif amount < 10000:
                return "LARGE"
            else:
                return "ENTERPRISE"

        self.df["ORDER_CATEGORY"] = self.df[amount_column].apply(categorize)
        logger.info("Added ORDER_CATEGORY column")
        return self

    def add_fiscal_info(
        self,
        date_column: str = "ORDER_DATE",
        fiscal_year_start_month: int = 1
    ) -> "PurchaseOrderTransformer":
        """
        Add fiscal year and quarter information.

        Args:
            date_column: Name of the date column
            fiscal_year_start_month: Starting month of fiscal year

        Returns:
            Self for method chaining
        """
        if date_column not in self.df.columns:
            logger.warning(f"Date column {date_column} not found")
            return self

        # Ensure datetime type
        if not pd.api.types.is_datetime64_any_dtype(self.df[date_column]):
            self.df[date_column] = pd.to_datetime(self.df[date_column])

        # Calculate fiscal year
        self.df["FISCAL_YEAR"] = self.df[date_column].apply(
            lambda x: x.year if x.month >= fiscal_year_start_month else x.year - 1
        )

        # Calculate fiscal quarter
        self.df["FISCAL_QUARTER"] = self.df[date_column].apply(
            lambda x: ((x.month - fiscal_year_start_month) % 12) // 3 + 1
        )

        logger.info("Added FISCAL_YEAR and FISCAL_QUARTER columns")
        return self

    def aggregate_by_vendor(self) -> pd.DataFrame:
        """
        Aggregate purchase orders by vendor.

        Returns:
            Aggregated DataFrame
        """
        if "VENDOR_NAME" not in self.df.columns:
            raise ValueError("VENDOR_NAME column not found")

        agg_columns = {"PURCHASE_ORDER_ID": "count"}

        if "TOTAL_AMOUNT" in self.df.columns:
            agg_columns["TOTAL_AMOUNT"] = ["sum", "mean"]

        if "QUANTITY" in self.df.columns:
            agg_columns["QUANTITY"] = "sum"

        result = self.df.groupby("VENDOR_NAME").agg(agg_columns)
        result.columns = ["_".join(col).strip("_") for col in result.columns]
        result = result.reset_index()

        logger.info(f"Aggregated to {len(result)} vendors")
        return result
