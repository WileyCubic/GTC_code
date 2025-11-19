from dataclasses import dataclass
from pathlib import Path
import re
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


# Classes for regex patterns

@dataclass
class LineitemPatternsConfig:
    size_pattern: re.Pattern = re.compile(r'\b(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|YOUTH\s*(?:XS|S|M|L|XL|XXL|2XL|3XL)|Regular)\b', re.IGNORECASE)
    color_pattern: re.Pattern = re.compile(r'\b(Black|White|Ash|Grey Heather|White/Blue|Blue|Green|Gold|Gray|Pink)\b', re.IGNORECASE)
    greek_name_pattern: re.Pattern = re.compile(r'''\b(Alpha Chi Omega|Alpha Chi|Alpha Delta Pi|ADPi|Alpha Epsilon Phi|AEPhi|Alpha Phi|APhi|Chi Omega|Chi O|Delta Delta Delta|Tri Delta|
                                Delta Gamma|Dee Gee|DG|Gamma Phi Beta|Gamma Phi|GPhi|Kappa Alpha theta|Theta|Kappa Delta|Kappa Kappa Gamma|Pi Beta phi|Pi Phi|
                                Sigma Sigma Sigma|Tri Sigma|Zeta Tau Alpha|Zeta Tau|Zeta|Penn State|Penn State Theta x Sigma Pi)\b''', re.IGNORECASE)
    greek_garment_patterns: re.Pattern = re.compile(r'''\b(Love You Cherry Much Hoodie|Signature Stitch Hoodie|Tank|Von Font Hoodie|Rhinestone Kiss Hoodie|Cheetah Applique Hoodie|
                                        Appliqué Wide Leg Sweatpants|Appliqué Mock Neck|Christmas Sisterhood Hoodie|Christmas Sisterhood Flannel PJ Short|Hoodie)\b''', re.IGNORECASE)
    square_patterns: re.Pattern = re.compile(r'''\b(Cardigan|Crew|Scoop Neck Sweater|Crew Neck Sweater|Tucker Hat|Racing Stripe Sweater|Hats|Twill Visor|Tee|Sweatshirt|Hoodie|Cardigan Sweater|
                                Zip Hoodie|T-Shirt|Sweatshirt|Baseball Cap|Tunic Sweater)\b''', re.IGNORECASE)

# Functions to load configurations
    
def load_config() -> tuple[SquarePathsConfig, ShopifyPathsConfig, DatabaseConfig, SquareRawPipelineConfig, ShopifyRawPipelineConfig]:
    square_paths = SquarePathsConfig()
    shopify_paths = ShopifyPathsConfig()
    db = DatabaseConfig()
    square_pipeline = SquareRawPipelineConfig()
    shopify_pipeline = ShopifyRawPipelineConfig()
    return square_paths, shopify_paths, db, square_pipeline, shopify_pipeline

def db_config() -> DatabaseConfig:
    db = DatabaseConfig()
    return db

def lineitem_patterns_config() -> LineitemPatternsConfig:
    patterns = LineitemPatternsConfig()
    return patterns