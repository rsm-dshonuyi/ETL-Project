"""
Weather Data Transformer Module

Specialized transformations for weather data.
"""

import logging
from typing import List, Optional

import pandas as pd

from src.transformers.data_transformer import DataTransformer

logger = logging.getLogger(__name__)


class WeatherTransformer(DataTransformer):
    """Transformer for weather data."""

    def __init__(self, df: pd.DataFrame):
        """
        Initialize weather transformer.

        Args:
            df: Input DataFrame with weather data
        """
        super().__init__(df)

    def standardize_columns(self) -> "WeatherTransformer":
        """
        Standardize weather data column names.

        Returns:
            Self for method chaining
        """
        column_mapping = {
            "date": "OBSERVATION_DATE",
            "observation_date": "OBSERVATION_DATE",
            "datetime": "OBSERVATION_DATE",
            "city": "CITY",
            "location": "CITY",
            "state": "STATE",
            "region": "STATE",
            "country": "COUNTRY",
            "temperature": "TEMPERATURE_F",
            "temp": "TEMPERATURE_F",
            "avg_temperature_f": "TEMPERATURE_F",
            "temperature_f": "TEMPERATURE_F",
            "min_temperature_f": "MIN_TEMPERATURE_F",
            "min_temp": "MIN_TEMPERATURE_F",
            "max_temperature_f": "MAX_TEMPERATURE_F",
            "max_temp": "MAX_TEMPERATURE_F",
            "humidity": "HUMIDITY_PCT",
            "humidity_pct": "HUMIDITY_PCT",
            "precipitation": "PRECIPITATION_INCHES",
            "precipitation_inches": "PRECIPITATION_INCHES",
            "rain": "PRECIPITATION_INCHES",
            "wind_speed": "WIND_SPEED_MPH",
            "wind_speed_mph": "WIND_SPEED_MPH",
            "wind": "WIND_SPEED_MPH",
            "conditions": "WEATHER_CONDITIONS",
            "weather": "WEATHER_CONDITIONS",
        }

        # Only rename columns that exist (case insensitive)
        df_lower_cols = {col.lower(): col for col in self.df.columns}
        existing_columns = {}

        for old_name, new_name in column_mapping.items():
            if old_name.lower() in df_lower_cols:
                existing_columns[df_lower_cols[old_name.lower()]] = new_name

        if existing_columns:
            self.df = self.df.rename(columns=existing_columns)
            logger.info(f"Standardized columns: {len(existing_columns)} columns")

        return self

    def convert_temperature(
        self,
        from_unit: str = "celsius",
        columns: Optional[List[str]] = None
    ) -> "WeatherTransformer":
        """
        Convert temperature between units.

        Args:
            from_unit: Source unit ('celsius' or 'fahrenheit')
            columns: Columns to convert

        Returns:
            Self for method chaining
        """
        if columns is None:
            columns = [
                col for col in self.df.columns
                if "TEMPERATURE" in col.upper()
            ]

        for col in columns:
            if col in self.df.columns:
                if from_unit.lower() == "celsius":
                    # Celsius to Fahrenheit
                    self.df[col] = (self.df[col] * 9/5) + 32
                elif from_unit.lower() == "fahrenheit":
                    # Fahrenheit to Celsius
                    self.df[col] = (self.df[col] - 32) * 5/9

        logger.info(f"Converted temperature in columns: {columns}")
        return self

    def calculate_daily_stats(
        self,
        date_column: str = "OBSERVATION_DATE",
        location_column: str = "CITY"
    ) -> "WeatherTransformer":
        """
        Calculate daily weather statistics.

        Args:
            date_column: Name of the date column
            location_column: Name of the location column

        Returns:
            Self for method chaining
        """
        if date_column not in self.df.columns:
            logger.warning(f"Date column {date_column} not found")
            return self

        # Ensure datetime type
        self.df[date_column] = pd.to_datetime(self.df[date_column])

        # Extract date part for grouping
        self.df["DATE_ONLY"] = self.df[date_column].dt.date

        group_cols = ["DATE_ONLY"]
        if location_column in self.df.columns:
            group_cols.append(location_column)

        # Aggregate metrics
        agg_dict = {}
        if "TEMPERATURE_F" in self.df.columns:
            agg_dict["TEMPERATURE_F"] = "mean"
        if "MIN_TEMPERATURE_F" in self.df.columns:
            agg_dict["MIN_TEMPERATURE_F"] = "min"
        if "MAX_TEMPERATURE_F" in self.df.columns:
            agg_dict["MAX_TEMPERATURE_F"] = "max"
        if "HUMIDITY_PCT" in self.df.columns:
            agg_dict["HUMIDITY_PCT"] = "mean"
        if "PRECIPITATION_INCHES" in self.df.columns:
            agg_dict["PRECIPITATION_INCHES"] = "sum"
        if "WIND_SPEED_MPH" in self.df.columns:
            agg_dict["WIND_SPEED_MPH"] = "mean"

        if agg_dict:
            self.df = self.df.groupby(group_cols).agg(agg_dict).reset_index()
            self.df = self.df.rename(columns={"DATE_ONLY": date_column})

        logger.info("Calculated daily statistics")
        return self

    def add_weather_severity(self) -> "WeatherTransformer":
        """
        Add weather severity classification.

        Returns:
            Self for method chaining
        """
        def calculate_severity(row):
            severity_score = 0

            # Temperature extremes
            if "TEMPERATURE_F" in row.index and pd.notna(row["TEMPERATURE_F"]):
                temp = row["TEMPERATURE_F"]
                if temp < 32 or temp > 100:
                    severity_score += 2
                elif temp < 40 or temp > 90:
                    severity_score += 1

            # High precipitation
            if "PRECIPITATION_INCHES" in row.index and pd.notna(row["PRECIPITATION_INCHES"]):
                precip = row["PRECIPITATION_INCHES"]
                if precip > 1:
                    severity_score += 2
                elif precip > 0.5:
                    severity_score += 1

            # High winds
            if "WIND_SPEED_MPH" in row.index and pd.notna(row["WIND_SPEED_MPH"]):
                wind = row["WIND_SPEED_MPH"]
                if wind > 30:
                    severity_score += 2
                elif wind > 15:
                    severity_score += 1

            # Classify severity
            if severity_score >= 4:
                return "SEVERE"
            elif severity_score >= 2:
                return "MODERATE"
            else:
                return "NORMAL"

        self.df["WEATHER_SEVERITY"] = self.df.apply(calculate_severity, axis=1)
        logger.info("Added WEATHER_SEVERITY column")
        return self

    def add_season(
        self,
        date_column: str = "OBSERVATION_DATE",
        hemisphere: str = "northern"
    ) -> "WeatherTransformer":
        """
        Add season based on date.

        Args:
            date_column: Name of the date column
            hemisphere: 'northern' or 'southern'

        Returns:
            Self for method chaining
        """
        if date_column not in self.df.columns:
            logger.warning(f"Date column {date_column} not found")
            return self

        # Ensure datetime type
        if not pd.api.types.is_datetime64_any_dtype(self.df[date_column]):
            self.df[date_column] = pd.to_datetime(self.df[date_column])

        def get_season(month):
            if hemisphere.lower() == "northern":
                if month in [12, 1, 2]:
                    return "WINTER"
                elif month in [3, 4, 5]:
                    return "SPRING"
                elif month in [6, 7, 8]:
                    return "SUMMER"
                else:
                    return "FALL"
            else:
                if month in [12, 1, 2]:
                    return "SUMMER"
                elif month in [3, 4, 5]:
                    return "FALL"
                elif month in [6, 7, 8]:
                    return "WINTER"
                else:
                    return "SPRING"

        self.df["SEASON"] = self.df[date_column].dt.month.apply(get_season)
        logger.info("Added SEASON column")
        return self

    def correlate_with_purchases(
        self,
        purchase_df: pd.DataFrame,
        weather_date_col: str = "OBSERVATION_DATE",
        purchase_date_col: str = "ORDER_DATE",
        weather_location_col: str = "CITY",
        purchase_location_col: str = "DELIVERY_LOCATION"
    ) -> pd.DataFrame:
        """
        Correlate weather data with purchase orders.

        Args:
            purchase_df: DataFrame with purchase order data
            weather_date_col: Date column in weather data
            purchase_date_col: Date column in purchase data
            weather_location_col: Location column in weather data
            purchase_location_col: Location column in purchase data

        Returns:
            Combined DataFrame with weather and purchase data
        """
        # Ensure datetime types
        weather_df = self.df.copy()
        weather_df[weather_date_col] = pd.to_datetime(weather_df[weather_date_col])
        purchase_df = purchase_df.copy()
        purchase_df[purchase_date_col] = pd.to_datetime(purchase_df[purchase_date_col])

        # Extract date only for joining
        weather_df["JOIN_DATE"] = weather_df[weather_date_col].dt.date
        purchase_df["JOIN_DATE"] = purchase_df[purchase_date_col].dt.date

        # Merge on date and location
        merge_cols = ["JOIN_DATE"]
        if (weather_location_col in weather_df.columns and
                purchase_location_col in purchase_df.columns):
            weather_df["JOIN_LOCATION"] = weather_df[weather_location_col].str.upper()
            purchase_df["JOIN_LOCATION"] = purchase_df[purchase_location_col].str.upper()
            merge_cols.append("JOIN_LOCATION")

        result = purchase_df.merge(
            weather_df,
            on=merge_cols,
            how="left",
            suffixes=("", "_WEATHER")
        )

        # Clean up join columns
        result = result.drop(columns=["JOIN_DATE"] + (
            ["JOIN_LOCATION"] if "JOIN_LOCATION" in result.columns else []
        ))

        logger.info(f"Correlated weather with {len(result)} purchase records")
        return result
