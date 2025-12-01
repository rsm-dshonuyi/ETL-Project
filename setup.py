"""
Setup configuration for ETL Pipeline
"""

from setuptools import setup, find_packages

setup(
    name="etl-pipeline",
    version="1.0.0",
    description="End-to-end ETL pipeline for purchase orders and weather data",
    author="ETL Pipeline Team",
    python_requires=">=3.9",
    packages=find_packages(),
    install_requires=[
        "snowflake-connector-python>=3.0.0",
        "pandas>=2.0.0",
        "psycopg2-binary>=2.9.0",
        "python-dotenv>=1.0.0",
        "lxml>=4.9.0",
        "requests>=2.28.0",
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "etl-pipeline=src.pipeline:main",
        ],
    },
)
