"""
Tests for XML Extractor
"""

import os
import tempfile

import pytest

from src.extractors.xml_extractor import XMLExtractor, parse_xml_to_records


class TestXMLExtractor:
    """Tests for XMLExtractor class."""

    @pytest.fixture
    def sample_xml_file(self):
        """Create a temporary XML file for testing."""
        content = """<?xml version="1.0" encoding="UTF-8"?>
<weather_data>
    <weather_record>
        <date>2024-01-15</date>
        <location>New York</location>
        <temperature>32</temperature>
        <humidity>65</humidity>
        <precipitation>0.1</precipitation>
        <wind_speed>12</wind_speed>
        <conditions>Cloudy</conditions>
    </weather_record>
    <weather_record>
        <date>2024-01-16</date>
        <location>Los Angeles</location>
        <temperature>72</temperature>
        <humidity>45</humidity>
        <precipitation>0</precipitation>
        <wind_speed>5</wind_speed>
        <conditions>Sunny</conditions>
    </weather_record>
</weather_data>
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".xml", delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        yield temp_path

        # Cleanup
        os.unlink(temp_path)

    def test_extract_weather_data(self, sample_xml_file):
        """Test weather data extraction."""
        extractor = XMLExtractor(sample_xml_file)
        df = extractor.extract_weather_data()

        assert len(df) == 2
        assert "date" in df.columns
        assert "location" in df.columns
        assert df.iloc[0]["location"] == "New York"
        assert df.iloc[1]["temperature"] == "72"

    def test_extract_with_custom_mappings(self, sample_xml_file):
        """Test extraction with custom XPath mappings."""
        extractor = XMLExtractor(sample_xml_file)
        field_mappings = {
            "weather_date": "./date/text()",
            "city": "./location/text()",
            "temp": "./temperature/text()"
        }

        df = extractor.extract(
            row_xpath="//weather_record",
            field_mappings=field_mappings
        )

        assert len(df) == 2
        assert "weather_date" in df.columns
        assert "city" in df.columns
        assert "temp" in df.columns

    def test_extract_file_not_found(self):
        """Test extraction with non-existent file."""
        extractor = XMLExtractor("/nonexistent/path.xml")

        with pytest.raises(FileNotFoundError):
            extractor.extract_weather_data()


class TestParseXMLToRecords:
    """Tests for parse_xml_to_records function."""

    def test_parse_simple_xml(self):
        """Test parsing simple XML string."""
        xml_string = """
        <data>
            <record><id>1</id><name>Test</name></record>
            <record><id>2</id><name>Test2</name></record>
        </data>
        """
        records = parse_xml_to_records(xml_string, "record")

        assert len(records) == 2
        assert records[0]["id"] == "1"
        assert records[1]["name"] == "Test2"
