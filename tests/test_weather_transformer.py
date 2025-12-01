"""
Tests for Weather Transformer
"""

import pandas as pd
import pytest

from src.transformers.weather_transformer import WeatherTransformer


class TestWeatherTransformer:
    """Tests for WeatherTransformer class."""

    @pytest.fixture
    def sample_weather_df(self):
        """Create sample weather DataFrame."""
        return pd.DataFrame({
            "date": ["2024-01-15", "2024-04-20", "2024-07-10", "2024-10-25"],
            "location": ["New York", "Los Angeles", "Chicago", "Miami"],
            "temperature": [32, 72, 85, 68],
            "humidity": [65, 45, 55, 80],
            "precipitation": [0.1, 0, 0.5, 1.5],
            "wind_speed": [12, 5, 8, 35],
            "conditions": ["Cloudy", "Sunny", "Clear", "Stormy"]
        })

    def test_standardize_columns(self, sample_weather_df):
        """Test column standardization."""
        transformer = WeatherTransformer(sample_weather_df)
        result = transformer.standardize_columns().get_result()

        assert "OBSERVATION_DATE" in result.columns
        assert "CITY" in result.columns
        assert "TEMPERATURE_F" in result.columns

    def test_add_weather_severity(self, sample_weather_df):
        """Test weather severity classification."""
        transformer = WeatherTransformer(sample_weather_df)
        result = (
            transformer
            .standardize_columns()
            .add_weather_severity()
            .get_result()
        )

        assert "WEATHER_SEVERITY" in result.columns
        # Miami has high precipitation (1.5) and high wind (35)
        assert result.iloc[3]["WEATHER_SEVERITY"] == "SEVERE"

    def test_add_season(self, sample_weather_df):
        """Test season addition."""
        transformer = WeatherTransformer(sample_weather_df)
        result = (
            transformer
            .standardize_columns()
            .add_season()
            .get_result()
        )

        assert "SEASON" in result.columns
        assert result.iloc[0]["SEASON"] == "WINTER"  # January
        assert result.iloc[1]["SEASON"] == "SPRING"  # April
        assert result.iloc[2]["SEASON"] == "SUMMER"  # July
        assert result.iloc[3]["SEASON"] == "FALL"    # October

    def test_convert_temperature_celsius_to_fahrenheit(self):
        """Test temperature conversion from Celsius."""
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "TEMPERATURE_F": [0]  # 0°C = 32°F
        })

        transformer = WeatherTransformer(df)
        result = transformer.convert_temperature(from_unit="celsius").get_result()

        assert result.iloc[0]["TEMPERATURE_F"] == 32

    def test_convert_temperature_fahrenheit_to_celsius(self):
        """Test temperature conversion from Fahrenheit."""
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "TEMPERATURE_F": [32]  # 32°F = 0°C
        })

        transformer = WeatherTransformer(df)
        result = transformer.convert_temperature(from_unit="fahrenheit").get_result()

        assert result.iloc[0]["TEMPERATURE_F"] == 0

    def test_correlate_with_purchases(self, sample_weather_df):
        """Test correlation with purchase data."""
        purchase_df = pd.DataFrame({
            "PURCHASE_ORDER_ID": ["PO-001", "PO-002"],
            "ORDER_DATE": ["2024-01-15", "2024-04-20"],
            "DELIVERY_LOCATION": ["New York", "Los Angeles"],
            "TOTAL_AMOUNT": [1000, 2000]
        })

        transformer = WeatherTransformer(sample_weather_df)
        transformer.standardize_columns()
        result = transformer.correlate_with_purchases(purchase_df)

        assert len(result) == 2
        # Check that weather data was joined
        assert "TEMPERATURE_F" in result.columns or "temperature" in result.columns

    def test_add_season_southern_hemisphere(self, sample_weather_df):
        """Test season addition for southern hemisphere."""
        transformer = WeatherTransformer(sample_weather_df)
        result = (
            transformer
            .standardize_columns()
            .add_season(hemisphere="southern")
            .get_result()
        )

        assert "SEASON" in result.columns
        assert result.iloc[0]["SEASON"] == "SUMMER"  # January in S. hemisphere
        assert result.iloc[2]["SEASON"] == "WINTER"  # July in S. hemisphere
