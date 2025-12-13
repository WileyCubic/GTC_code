# GTC Data Pipeline

A comprehensive data pipeline to serve the data processing needs of GTC. Handels sales data from Square and Shopify, providing ETL and ELT functionalities for various processes. Will be expanded to include expense management in the future.

## 🚀 Features

### Core Pipelines
- **Raw Orders Pipeline**: Processes CSV files from Square and Shopify into standardized database tables
- **Line Item Analysis Pipeline**: Performs detailed product analysis including pattern matching for sizes, colors, and garment types
- **Additional ELT Pipelines**: Under development
- **Expenses ETL**: This section is under development for the time being

### Key Capabilities
- ✅ **ETL Framework**: Import from CSV to MySQL for further processing
- ✅ **ELT Framework**: Extract from MySQL initial raw table to tables better suited for business needs
- ✅ **Dual Database Support**: MySQL and SQLite backends
- ✅ **Comprehensive Logging**: Dual log files with rotating handlersg
- ✅ **Pattern Matching**: Advanced text analysis for product categorization
- ✅ **Error Handling**: Robust exception management and recovery


## 📁 Project Structure

```
Data-Pipelines/
├── Sales_ETL/                    # Main ETL package
│   ├── common/                   # Shared utilities
│   │   ├── config.py            # Database and logging configuration
│   │   ├── logging_config.py    # Logging setup and handlers
│   │   └── utils.py             # Utility functions (phone formatting, etc.)
│   ├── Pipelines/               # Pipeline implementations
│   │   ├── raw_orders/          # Raw order processing pipeline
│   │   │   ├── pipeline.py      # Main pipeline orchestrator
│   │   │   ├── extract.py       # CSV file extraction logic
│   │   │   ├── raw_transform.py # Data transformation rules
│   │   │   ├── load.py          # Database loading operations
│   │   │   ├── config.py        # Pipeline-specific configuration
│   │   │   └── utils.py         # Pipeline utilities
│   │   └── line_item_analysis/  # Product analysis pipeline
│   │       ├── pipeline.py      # Analysis orchestrator
│   │       ├── extract.py       # Data extraction
│   │       ├── lineitem_analysis_transform.py # Analysis transformations
│   │       ├── load.py          # Results loading
│   │       ├── config.py        # Analysis configuration
│   │       └── utils.py         # Analysis utilities
│   ├── assets/                  # Static resources
│   │   └── lineitem_patterns.json # Product pattern definitions
│   └── sql/                     # SQL queries and functions
│       ├── lineitem_analysis_query.sql
│       └── functions/
│           └── connections.py   # Database connection management
├── Expences_ETL/               # Expense processing module
│   └── ETL Expences to DB.py  # Expense ETL implementation
├── tests/                      # Test suite (89% coverage)
│   ├── test_basic.py          # Basic functionality tests
│   ├── fixtures/              # Test data and mocks
│   ├── Sales_ETL/             # Component-specific tests
│   └── run_tests.py           # Test runner
├── logs/                      # Log file directory
├── scripts/                   # Utility scripts
└── .env                       # Environment configuration
```

## 🔧 Usage

### Running Entire Sales Pipeline
```
Navigate to Scripts/run_all_sales_pipe.py and run the script
```
### Running Raw Orders Pipeline
```
Navigate to Scripts/run_raw_sales_pipe.py and run the script
```

### Running Line Item Analysis
```
Navigate to Scripts/run_lineitem_analysis_pipe.py and run the script
```

### Processing Expenses
```python
Coming soon...
```


## 📊 Pipeline Details

### Raw Orders Pipeline
- **Input**: CSV files from Square/Shopify exports
- **Processing**: 
  - Phone number standardization
  - Date/time normalization  
  - Currency and numeric validation
  - Duplicate detection
- **Output**: Partaally cleaned data loaded into a MySQL table for further managment

### Line Item Analysis Pipeline
- **Input**: Raw order data from MySQL server
- **Processing**:
  - Product categorization (garment types, sizes, colors)
  - Pattern matching using JSON configurations
  - Sales volume analysis
  - Customer behavior analysis
- **Output**: Analysis reports and Visualizations

## 🔍 Monitoring & Logging

### Log Files
- **Primary Logs**: Pipeline-specific detailed logging
- **Master Log**: Consolidated system-wide events
- **Rotating Logs**: Automatic log rotation and archival

### Log Levels
- `INFO`: Normal pipeline operations
- `WARNING`: Non-critical issues
- `ERROR`: Pipeline failures and exceptions
- `DEBUG`: Detailed troubleshooting information


## 🚨 Error Handling

- **Graceful Degradation**: Pipelines continue processing valid records when encountering non critical errors
- **Error Logging**: Comprehensive error context and stack traces
- **Data Validation**: Input validation with detailed error reporting
- **Recovery Mechanisms**: Automatic retry logic for transient failures

## 📋 Requirements

```txt
pandas>=1.5.0
SQLAlchemy>=1.4.0
mysql-connector-python>=8.0.0
python-dotenv>=0.19.0
logging>=0.4.9.6
pytest>=7.0.0
pytest-cov>=4.0.0
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---
