# ETL Pipeline Project

A production-ready, end-to-end ETL (Extract, Transform, Load) pipeline built with Python and Snowflake for processing purchase order and weather data from multiple sources.

## 🎯 Overview

This project demonstrates a complete ETL solution that:
- **Extracts** data from diverse sources (CSV, XML, PostgreSQL, Snowflake Marketplace)
- **Transforms** purchase orders and weather data with comprehensive data cleaning and enrichment
- **Loads** processed data into Snowflake data warehouse for analytics

## 📁 Project Structure

```
ETL-Project/
├── src/
│   ├── extractors/          # Data extraction modules
│   │   ├── csv_extractor.py
│   │   ├── xml_extractor.py
│   │   ├── postgres_extractor.py
│   │   └── snowflake_extractor.py
│   ├── transformers/        # Data transformation modules
│   │   ├── data_transformer.py
│   │   ├── purchase_order_transformer.py
│   │   └── weather_transformer.py
│   ├── loaders/             # Data loading modules
│   │   └── snowflake_loader.py
│   └── pipeline.py          # Main orchestration script
├── config/
│   └── config.yaml          # Pipeline configuration
├── data/
│   ├── raw/                 # Source data files
│   └── processed/           # Processed data output
├── tests/                   # Unit tests
├── requirements.txt
└── setup.py
```

## 🚀 Features

### Data Extraction
- **CSV Extractor**: Batch and chunked reading of CSV files
- **XML Extractor**: XPath-based data extraction from XML sources
- **PostgreSQL Extractor**: SQL query execution with connection pooling
- **Snowflake Marketplace Extractor**: Access to Snowflake shared datasets

### Data Transformation
- Column standardization and renaming
- Data type conversion and validation
- Null handling and deduplication
- Business logic enrichment (fiscal periods, order categories)
- Weather severity classification and seasonal analysis
- Purchase order and weather data correlation

### Data Loading
- Bulk loading with Snowflake's write_pandas
- Merge (upsert) operations for incremental updates
- Stage-based file loading
- Transaction management

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/rsm-dshonuyi/ETL-Project.git
cd ETL-Project
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

## ⚙️ Configuration

Edit `config/config.yaml` to configure:
- Snowflake connection settings
- PostgreSQL connection settings
- Data source paths
- Target table names
- Transformation parameters

## 📊 Usage

### Run the Complete Pipeline
```bash
python -m src.pipeline
```

### Run Specific Phases
```bash
# Extract only
python -m src.pipeline --extract-only

# Transform only
python -m src.pipeline --transform-only

# Load only
python -m src.pipeline --load-only
```

### Use Custom Configuration
```bash
python -m src.pipeline --config path/to/custom/config.yaml
```

## 🧪 Testing

Run the test suite:
```bash
pytest tests/ -v
```

Run with coverage:
```bash
pytest tests/ --cov=src --cov-report=html
```

## 📈 Data Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   CSV File  │───►│             │    │             │
├─────────────┤    │             │    │             │
│   XML File  │───►│  TRANSFORM  │───►│  SNOWFLAKE  │
├─────────────┤    │             │    │             │
│  PostgreSQL │───►│             │    │             │
├─────────────┤    │             │    │             │
│  Marketplace│───►│             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

## 🔧 Key Components

### Extractors
Each extractor provides a consistent interface:
```python
from src.extractors import CSVExtractor

extractor = CSVExtractor("data/raw/purchase_orders.csv")
df = extractor.extract()
```

### Transformers
Transformers use method chaining for readability:
```python
from src.transformers import PurchaseOrderTransformer

transformer = PurchaseOrderTransformer(df)
result = (
    transformer
    .standardize_columns()
    .drop_nulls()
    .calculate_totals()
    .categorize_orders()
    .get_result()
)
```

### Loader
The loader handles Snowflake operations:
```python
from src.loaders import SnowflakeLoader

loader = SnowflakeLoader(account, user, password, ...)
loader.load(df, "TARGET_TABLE", if_exists="append")
```

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📧 Contact

For questions or support, please open an issue in this repository.
