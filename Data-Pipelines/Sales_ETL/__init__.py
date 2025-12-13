"""
Sales ETL Pipeline Package

This package contains utilities and functions for processing sales data
from Square and Shopify through the ETL (Extract, Transform, Load) pipeline.

Modules:
    Utils: Phone number formatting and logging utility functions
    config: Configuration classes for paths, database, and pipeline settings
    pipeline: Main ETL pipeline execution and file processing
    extract: CSV file discovery and DataFrame creation
    raw_transform: Data transformation functions for Square and Shopify
    load: Database engine creation and table loading functions

Main Functions:
    - format_phone_number: Format phone numbers to standardized formats
    - log: Legacy logging function
    - run_pipeline: Execute the complete ETL pipeline
    - load_config: Load all configuration settings
"""

# Import key functions and classes for easy access (with graceful failure handling)
try:
    from .common.utils import format_phone_number
    from .common.config import load_config
except ImportError:
    # Gracefully handle missing modules during testing
    format_phone_number = None
    load_config = None

try:
    from .common.config import (
        SquarePathsConfig,
        ShopifyPathsConfig, 
        DatabaseConfig,
        SquareRawPipelineConfig,
        ShopifyRawPipelineConfig
    )
except ImportError:
    # Define placeholder classes if imports fail
    SquarePathsConfig = None
    ShopifyPathsConfig = None
    DatabaseConfig = None
    SquareRawPipelineConfig = None
    ShopifyRawPipelineConfig = None
try:
    from .Pipelines.raw_orders.pipeline import run_pipeline
    from .Pipelines.line_item_analysis.pipeline import process_file
except ImportError:
    run_pipeline = None
    process_file = None

# Define what gets imported with "from Sales_ETL import *" (only available items)
__all__ = []

# Add available functions to __all__
for item in [
    'format_phone_number',
    'run_pipeline', 
    'process_file',
    'load_config',
    'SquarePathsConfig',
    'ShopifyPathsConfig',
    'DatabaseConfig',
    'SquareRawPipelineConfig', 
    'ShopifyRawPipelineConfig'
]:
    if globals().get(item) is not None:
        __all__.append(item)

# Package metadata
__version__ = '1.0.0'
__author__ = 'D3 Design Inc'
__description__ = 'ETL Pipeline for Square and Shopify sales data processing'