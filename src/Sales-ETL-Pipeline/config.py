from dataclasses import dataclass
from pathlib import Path

import os

from dotenv import load_dotenv

# Pull values from .env if present
load_dotenv()

# Classes for path configuration

@dataclass
class SquarePathsConfig:
    input_dir: Path = Path(os.getenv('ELT_Sales_to_DB_square_CSV_input'))
    log_file: Path = Path(os.getenv('ETL_Sales_to_DB_log_file'))
    
@dataclass
class ShopifyPathsConfig:
    input_dir: Path = Path(os.getenv('ETL_Sales_to_DB_shopify_CSV_input'))
    log_file: Path = Path(os.getenv('ETL_Sales_to_DB_log_file'))

# Classes for database configuration

@dataclass
class DatabaseConfig:
    host: str = os.getenv('Mysql_host')
    user: str = os.getenv('Mysql_user')
    password: str = os.getenv('Mysql_password')
    database: str = os.getenv('Mysql_database')
    
# Classes for pipeline configuration

@dataclass
class SquareRawPipelineConfig:
    table_name: str = os.getenv('square_raw')
    csv_pattern: str = '*.csv'
    
@dataclass
class ShopifyRawPipelineConfig:
    table_name: str = os.getenv('shopify_raw')
    csv_pattern: str = '*.csv'
    
def load_config() -> tuple[SquarePathsConfig, ShopifyPathsConfig, DatabaseConfig, SquareRawPipelineConfig, ShopifyRawPipelineConfig]:
    square_paths = SquarePathsConfig()
    shopify_paths = ShopifyPathsConfig()
    db = DatabaseConfig()
    square_pipeline = SquareRawPipelineConfig()
    shopify_pipeline = ShopifyRawPipelineConfig()
    return square_paths, shopify_paths, db, square_pipeline, shopify_pipeline