"""
CSV Data Extractor Module

Extracts purchase order data from CSV files.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CSVExtractor:
    """Extractor for CSV data sources."""

    def __init__(self, file_path: str, delimiter: str = ",", encoding: str = "utf-8"):
        """
        Initialize CSV extractor.

        Args:
            file_path: Path to the CSV file
            delimiter: Field delimiter character
            encoding: File encoding
        """
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding

    def extract(
        self,
        columns: Optional[list] = None,
        dtype: Optional[dict] = None,
        parse_dates: Optional[list] = None
    ) -> pd.DataFrame:
        """
        Extract data from CSV file.

        Args:
            columns: Specific columns to extract
            dtype: Data types for columns
            parse_dates: Columns to parse as dates

        Returns:
            DataFrame containing the extracted data
        """
        if not self.file_path.exists():
            logger.error(f"CSV file not found: {self.file_path}")
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        logger.info(f"Extracting data from CSV: {self.file_path}")

        df = pd.read_csv(
            self.file_path,
            delimiter=self.delimiter,
            encoding=self.encoding,
            usecols=columns,
            dtype=dtype,
            parse_dates=parse_dates
        )

        logger.info(f"Extracted {len(df)} rows from CSV")
        return df

    def extract_in_chunks(
        self,
        chunk_size: int = 10000,
        columns: Optional[list] = None
    ):
        """
        Extract data from CSV in chunks for large files.

        Args:
            chunk_size: Number of rows per chunk
            columns: Specific columns to extract

        Yields:
            DataFrame chunks
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        logger.info(f"Extracting data in chunks from CSV: {self.file_path}")

        for chunk in pd.read_csv(
            self.file_path,
            delimiter=self.delimiter,
            encoding=self.encoding,
            usecols=columns,
            chunksize=chunk_size
        ):
            yield chunk
