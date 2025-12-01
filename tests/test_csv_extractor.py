"""
Tests for CSV Extractor
"""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.extractors.csv_extractor import CSVExtractor


class TestCSVExtractor:
    """Tests for CSVExtractor class."""

    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        content = """po_number,order_date,vendor,quantity,unit_price
PO-001,2024-01-15,Acme Corp,100,25.50
PO-002,2024-01-16,Global Supplies,50,150.00
PO-003,2024-01-17,Tech Parts Inc,200,10.25
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        yield temp_path

        # Cleanup
        os.unlink(temp_path)

    def test_extract_basic(self, sample_csv_file):
        """Test basic CSV extraction."""
        extractor = CSVExtractor(sample_csv_file)
        df = extractor.extract()

        assert len(df) == 3
        assert "po_number" in df.columns
        assert "order_date" in df.columns
        assert df.iloc[0]["po_number"] == "PO-001"

    def test_extract_with_columns(self, sample_csv_file):
        """Test extraction with specific columns."""
        extractor = CSVExtractor(sample_csv_file)
        df = extractor.extract(columns=["po_number", "vendor"])

        assert len(df.columns) == 2
        assert "po_number" in df.columns
        assert "vendor" in df.columns
        assert "quantity" not in df.columns

    def test_extract_file_not_found(self):
        """Test extraction with non-existent file."""
        extractor = CSVExtractor("/nonexistent/path.csv")

        with pytest.raises(FileNotFoundError):
            extractor.extract()

    def test_extract_in_chunks(self, sample_csv_file):
        """Test chunked extraction."""
        extractor = CSVExtractor(sample_csv_file)
        chunks = list(extractor.extract_in_chunks(chunk_size=2))

        assert len(chunks) == 2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 1
