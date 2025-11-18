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

# Import key functions and classes for easy access
from .Utils import format_phone_number, log
from .config import (
    load_config,
    SquarePathsConfig,
    ShopifyPathsConfig, 
    DatabaseConfig,
    SquareRawPipelineConfig,
    ShopifyRawPipelineConfig
)
from .pipeline import run_pipeline, process_file

# Define what gets imported with "from Sales-ETL-Pipeline import *"
__all__ = [
    'format_phone_number',
    'log',
    'run_pipeline',
    'process_file',
    'load_config',
    'SquarePathsConfig',
    'ShopifyPathsConfig',
    'DatabaseConfig', 
    'SquareRawPipelineConfig',
    'ShopifyRawPipelineConfig'
]

# Package metadata
__version__ = '1.0.0'
__author__ = 'D3 Design Inc'
__description__ = 'ETL Pipeline for Square and Shopify sales data processing'