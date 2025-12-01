"""
ETL Pipeline Orchestrator

Main entry point for the end-to-end ETL pipeline that:
1. Extracts data from CSV, XML, PostgreSQL, and Snowflake Marketplace
2. Transforms purchase order and weather data
3. Loads transformed data into Snowflake data warehouse
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors import (
    CSVExtractor,
    XMLExtractor,
    PostgresExtractor,
    SnowflakeMarketplaceExtractor
)
from src.loaders import SnowflakeLoader
from src.transformers import (
    PurchaseOrderTransformer,
    WeatherTransformer
)

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_filename = log_dir / f"etl_pipeline_{datetime.now():%Y%m%d}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filename)
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    End-to-end ETL Pipeline orchestrator.

    Coordinates extraction from multiple sources, transformation,
    and loading into Snowflake data warehouse.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the ETL pipeline.

        Args:
            config_path: Path to configuration file
        """
        # Load environment variables
        load_dotenv()

        # Load configuration
        self.config = self._load_config(config_path)

        # Initialize components
        self.extractors = {}
        self.loader = None
        self._initialize_components()

        logger.info("ETL Pipeline initialized successfully")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Substitute environment variables
        config = self._substitute_env_vars(config)
        return config

    def _substitute_env_vars(self, config: Dict) -> Dict:
        """Recursively substitute environment variables in config."""
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            env_var = config[2:-1]
            return os.getenv(env_var, "")
        return config

    def _initialize_components(self):
        """Initialize extractors and loader based on configuration."""
        # Initialize CSV extractor
        csv_config = self.config.get("data_sources", {}).get("csv", {})
        if csv_config.get("purchase_orders"):
            self.extractors["csv"] = CSVExtractor(
                csv_config["purchase_orders"]
            )

        # Initialize XML extractor
        xml_config = self.config.get("data_sources", {}).get("xml", {})
        if xml_config.get("weather_data"):
            self.extractors["xml"] = XMLExtractor(
                xml_config["weather_data"]
            )

        # Initialize PostgreSQL extractor
        pg_config = self.config.get("postgres", {})
        if all([pg_config.get("host"), pg_config.get("database"),
                pg_config.get("user"), pg_config.get("password")]):
            self.extractors["postgres"] = PostgresExtractor(
                host=pg_config["host"],
                port=pg_config.get("port", 5432),
                database=pg_config["database"],
                user=pg_config["user"],
                password=pg_config["password"]
            )

        # Initialize Snowflake Marketplace extractor
        sf_config = self.config.get("snowflake", {})
        mp_config = self.config.get("data_sources", {}).get("snowflake_marketplace", {})
        if all([sf_config.get("account"), sf_config.get("user"),
                sf_config.get("password"), mp_config.get("database")]):
            self.extractors["snowflake_marketplace"] = SnowflakeMarketplaceExtractor(
                account=sf_config["account"],
                user=sf_config["user"],
                password=sf_config["password"],
                warehouse=sf_config.get("warehouse", "COMPUTE_WH"),
                database=mp_config["database"],
                schema=mp_config.get("schema", "PUBLIC"),
                role=sf_config.get("role")
            )

        # Initialize Snowflake loader
        if all([sf_config.get("account"), sf_config.get("user"),
                sf_config.get("password"), sf_config.get("database")]):
            self.loader = SnowflakeLoader(
                account=sf_config["account"],
                user=sf_config["user"],
                password=sf_config["password"],
                warehouse=sf_config.get("warehouse", "COMPUTE_WH"),
                database=sf_config["database"],
                schema=sf_config.get("schema", "PUBLIC"),
                role=sf_config.get("role")
            )

    def extract_purchase_orders(self) -> pd.DataFrame:
        """
        Extract purchase order data from all configured sources.

        Returns:
            Combined DataFrame with purchase order data
        """
        logger.info("Extracting purchase order data...")
        dataframes = []

        # Extract from CSV
        if "csv" in self.extractors:
            try:
                csv_df = self.extractors["csv"].extract()
                csv_df["SOURCE"] = "CSV"
                dataframes.append(csv_df)
                logger.info(f"Extracted {len(csv_df)} rows from CSV")
            except FileNotFoundError:
                logger.warning("CSV file not found, skipping")

        # Extract from PostgreSQL
        if "postgres" in self.extractors:
            try:
                table_name = self.config.get("data_sources", {}).get(
                    "postgres", {}
                ).get("table_name", "purchase_orders")
                pg_df = self.extractors["postgres"].extract_table(table_name)
                pg_df["SOURCE"] = "POSTGRESQL"
                dataframes.append(pg_df)
                logger.info(f"Extracted {len(pg_df)} rows from PostgreSQL")
            except Exception as e:
                logger.warning(f"PostgreSQL extraction failed: {e}")

        if not dataframes:
            logger.warning("No purchase order data extracted from any source")
            return pd.DataFrame()

        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Total purchase orders extracted: {len(combined_df)}")
        return combined_df

    def extract_weather_data(self) -> pd.DataFrame:
        """
        Extract weather data from all configured sources.

        Returns:
            Combined DataFrame with weather data
        """
        logger.info("Extracting weather data...")
        dataframes = []

        # Extract from XML
        if "xml" in self.extractors:
            try:
                xml_df = self.extractors["xml"].extract_weather_data()
                xml_df["SOURCE"] = "XML"
                dataframes.append(xml_df)
                logger.info(f"Extracted {len(xml_df)} rows from XML")
            except FileNotFoundError:
                logger.warning("XML file not found, skipping")

        # Extract from Snowflake Marketplace
        if "snowflake_marketplace" in self.extractors:
            try:
                sf_df = self.extractors["snowflake_marketplace"].extract_weather_history(
                    limit=10000
                )
                sf_df["SOURCE"] = "SNOWFLAKE_MARKETPLACE"
                dataframes.append(sf_df)
                logger.info(f"Extracted {len(sf_df)} rows from Snowflake Marketplace")
            except Exception as e:
                logger.warning(f"Snowflake Marketplace extraction failed: {e}")

        if not dataframes:
            logger.warning("No weather data extracted from any source")
            return pd.DataFrame()

        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Total weather records extracted: {len(combined_df)}")
        return combined_df

    def transform_purchase_orders(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform purchase order data.

        Args:
            df: Raw purchase order DataFrame

        Returns:
            Transformed DataFrame
        """
        if df.empty:
            return df

        logger.info("Transforming purchase order data...")

        transformer = PurchaseOrderTransformer(df)
        transformed_df = (
            transformer
            .standardize_columns()
            .drop_nulls(columns=["PURCHASE_ORDER_ID"])
            .drop_duplicates(columns=["PURCHASE_ORDER_ID"])
            .calculate_totals()
            .validate_order_dates()
            .categorize_orders()
            .add_fiscal_info()
            .add_column("ETL_TIMESTAMP", datetime.now())
            .get_result()
        )

        logger.info(f"Transformed {len(transformed_df)} purchase orders")
        return transformed_df

    def transform_weather_data(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform weather data.

        Args:
            df: Raw weather DataFrame

        Returns:
            Transformed DataFrame
        """
        if df.empty:
            return df

        logger.info("Transforming weather data...")

        transformer = WeatherTransformer(df)
        transformed_df = (
            transformer
            .standardize_columns()
            .drop_nulls(columns=["OBSERVATION_DATE"])
            .add_weather_severity()
            .add_season()
            .add_column("ETL_TIMESTAMP", datetime.now())
            .get_result()
        )

        logger.info(f"Transformed {len(transformed_df)} weather records")
        return transformed_df

    def create_analytics_table(
        self,
        purchase_df: pd.DataFrame,
        weather_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Create combined analytics table.

        Args:
            purchase_df: Transformed purchase order data
            weather_df: Transformed weather data

        Returns:
            Combined analytics DataFrame
        """
        if purchase_df.empty or weather_df.empty:
            logger.warning("Cannot create analytics table: missing data")
            return pd.DataFrame()

        logger.info("Creating combined analytics table...")

        weather_transformer = WeatherTransformer(weather_df)
        analytics_df = weather_transformer.correlate_with_purchases(
            purchase_df,
            weather_date_col="OBSERVATION_DATE",
            purchase_date_col="ORDER_DATE",
            weather_location_col="CITY",
            purchase_location_col="DELIVERY_LOCATION"
        )

        logger.info(f"Created analytics table with {len(analytics_df)} records")
        return analytics_df

    def load_to_snowflake(
        self,
        purchase_df: pd.DataFrame,
        weather_df: pd.DataFrame,
        analytics_df: pd.DataFrame
    ) -> Dict[str, int]:
        """
        Load all transformed data to Snowflake.

        Args:
            purchase_df: Transformed purchase order data
            weather_df: Transformed weather data
            analytics_df: Combined analytics data

        Returns:
            Dictionary with row counts for each table
        """
        if self.loader is None:
            logger.error("Snowflake loader not configured")
            return {}

        results = {}
        target_tables = self.config.get("target_tables", {})

        # Load purchase orders
        if not purchase_df.empty:
            table_name = target_tables.get("purchase_orders", "PURCHASE_ORDERS")
            rows = self.loader.load(purchase_df, table_name, if_exists="append")
            results[table_name] = rows
            logger.info(f"Loaded {rows} rows to {table_name}")

        # Load weather data
        if not weather_df.empty:
            table_name = target_tables.get("weather_data", "WEATHER_DATA")
            rows = self.loader.load(weather_df, table_name, if_exists="append")
            results[table_name] = rows
            logger.info(f"Loaded {rows} rows to {table_name}")

        # Load analytics
        if not analytics_df.empty:
            table_name = target_tables.get(
                "combined_analytics", "PURCHASE_WEATHER_ANALYTICS"
            )
            rows = self.loader.load(analytics_df, table_name, if_exists="append")
            results[table_name] = rows
            logger.info(f"Loaded {rows} rows to {table_name}")

        return results

    def run(
        self,
        extract: bool = True,
        transform: bool = True,
        load: bool = True
    ) -> Dict:
        """
        Execute the complete ETL pipeline.

        Args:
            extract: Whether to run extraction
            transform: Whether to run transformation
            load: Whether to run loading

        Returns:
            Dictionary with pipeline execution results
        """
        start_time = datetime.now()
        logger.info(f"Starting ETL Pipeline at {start_time}")

        results = {
            "status": "success",
            "start_time": start_time.isoformat(),
            "steps": {}
        }

        try:
            # Extraction phase
            purchase_df = pd.DataFrame()
            weather_df = pd.DataFrame()

            if extract:
                logger.info("=" * 50)
                logger.info("EXTRACTION PHASE")
                logger.info("=" * 50)
                purchase_df = self.extract_purchase_orders()
                weather_df = self.extract_weather_data()
                results["steps"]["extract"] = {
                    "purchase_orders": len(purchase_df),
                    "weather_data": len(weather_df)
                }

            # Transformation phase
            transformed_purchase_df = pd.DataFrame()
            transformed_weather_df = pd.DataFrame()
            analytics_df = pd.DataFrame()

            if transform:
                logger.info("=" * 50)
                logger.info("TRANSFORMATION PHASE")
                logger.info("=" * 50)
                transformed_purchase_df = self.transform_purchase_orders(purchase_df)
                transformed_weather_df = self.transform_weather_data(weather_df)
                analytics_df = self.create_analytics_table(
                    transformed_purchase_df,
                    transformed_weather_df
                )
                results["steps"]["transform"] = {
                    "purchase_orders": len(transformed_purchase_df),
                    "weather_data": len(transformed_weather_df),
                    "analytics": len(analytics_df)
                }

            # Loading phase
            if load:
                logger.info("=" * 50)
                logger.info("LOADING PHASE")
                logger.info("=" * 50)
                load_results = self.load_to_snowflake(
                    transformed_purchase_df,
                    transformed_weather_df,
                    analytics_df
                )
                results["steps"]["load"] = load_results

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            raise

        end_time = datetime.now()
        results["end_time"] = end_time.isoformat()
        results["duration_seconds"] = (end_time - start_time).total_seconds()

        logger.info("=" * 50)
        logger.info(f"Pipeline completed in {results['duration_seconds']:.2f} seconds")
        logger.info(f"Status: {results['status']}")
        logger.info("=" * 50)

        return results


def main():
    """Main entry point for the ETL pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the ETL Pipeline")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Run only extraction phase"
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Run only transformation phase"
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Run only loading phase"
    )

    args = parser.parse_args()

    # Determine which phases to run
    if args.extract_only or args.transform_only or args.load_only:
        extract = args.extract_only
        transform = args.transform_only
        load = args.load_only
    else:
        extract = transform = load = True

    # Run pipeline
    pipeline = ETLPipeline(config_path=args.config)
    results = pipeline.run(extract=extract, transform=transform, load=load)

    print("\n" + "=" * 50)
    print("PIPELINE RESULTS")
    print("=" * 50)
    print(yaml.dump(results, default_flow_style=False))


if __name__ == "__main__":
    main()
