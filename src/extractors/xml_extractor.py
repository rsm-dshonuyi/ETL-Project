"""
XML Data Extractor Module

Extracts weather data from XML files.
"""

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
from lxml import etree

logger = logging.getLogger(__name__)


class XMLExtractor:
    """Extractor for XML data sources."""

    def __init__(self, file_path: str, encoding: str = "utf-8"):
        """
        Initialize XML extractor.

        Args:
            file_path: Path to the XML file
            encoding: File encoding
        """
        self.file_path = Path(file_path)
        self.encoding = encoding

    def extract(
        self,
        row_xpath: str,
        field_mappings: dict,
        namespaces: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Extract data from XML file using XPath.

        Args:
            row_xpath: XPath expression to select row elements
            field_mappings: Dict mapping column names to XPath expressions
            namespaces: XML namespaces if applicable

        Returns:
            DataFrame containing the extracted data
        """
        if not self.file_path.exists():
            logger.error(f"XML file not found: {self.file_path}")
            raise FileNotFoundError(f"XML file not found: {self.file_path}")

        logger.info(f"Extracting data from XML: {self.file_path}")

        tree = etree.parse(str(self.file_path))
        root = tree.getroot()

        rows = root.xpath(row_xpath, namespaces=namespaces)
        data = []

        for row in rows:
            record = {}
            for column_name, xpath_expr in field_mappings.items():
                elements = row.xpath(xpath_expr, namespaces=namespaces)
                if elements:
                    if isinstance(elements[0], str):
                        record[column_name] = elements[0]
                    else:
                        record[column_name] = elements[0].text
                else:
                    record[column_name] = None
            data.append(record)

        df = pd.DataFrame(data)
        logger.info(f"Extracted {len(df)} rows from XML")
        return df

    def extract_weather_data(self) -> pd.DataFrame:
        """
        Extract weather data with predefined mappings.

        Returns:
            DataFrame containing weather data
        """
        field_mappings = {
            "date": "./date/text()",
            "location": "./location/text()",
            "temperature": "./temperature/text()",
            "humidity": "./humidity/text()",
            "precipitation": "./precipitation/text()",
            "wind_speed": "./wind_speed/text()",
            "conditions": "./conditions/text()"
        }

        return self.extract(
            row_xpath="//weather_record",
            field_mappings=field_mappings
        )


def parse_xml_to_records(xml_string: str, record_tag: str) -> List[dict]:
    """
    Parse XML string to list of dictionaries.

    Args:
        xml_string: XML content as string
        record_tag: Tag name for individual records

    Returns:
        List of dictionaries representing records
    """
    root = etree.fromstring(xml_string.encode())
    records = []

    for element in root.iter(record_tag):
        record = {}
        for child in element:
            record[child.tag] = child.text
        records.append(record)

    return records
