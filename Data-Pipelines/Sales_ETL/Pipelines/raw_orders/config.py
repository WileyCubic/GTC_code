from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()

#----------------------------------------#
# Classes for path configuration
#----------------------------------------#

@dataclass
class SquarePathsConfig:
    input_dir: Path = Path(os.getenv('ELT_Sales_to_DB_square_CSV_input'))
    
@dataclass
class ShopifyPathsConfig:
    input_dir: Path = Path(os.getenv('ETL_Sales_to_DB_shopify_CSV_input'))

#----------------------------------------#    
# Classes for pipeline configuration
#----------------------------------------#

@dataclass
class SquareRawPipelineConfig:
    table_name: str = os.getenv('square_raw')
    csv_pattern: str = '*.csv'
    
@dataclass
class ShopifyRawPipelineConfig:
    table_name: str = os.getenv('shopify_raw')
    csv_pattern: str = '*.csv'

#----------------------------------------#
# Functions to load configurations
#----------------------------------------#

def path_config() -> tuple[SquarePathsConfig, ShopifyPathsConfig]:
    square_paths = SquarePathsConfig()
    shopify_paths = ShopifyPathsConfig()

    return square_paths, shopify_paths

def pipeline_config() -> tuple[SquareRawPipelineConfig, ShopifyRawPipelineConfig]:
    square_pipeline = SquareRawPipelineConfig()
    shopify_pipeline = ShopifyRawPipelineConfig()
    return square_pipeline, shopify_pipeline
