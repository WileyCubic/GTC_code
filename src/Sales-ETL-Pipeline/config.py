from dataclasses import dataclass, field
import json
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
class MysqlDatabaseConfig:
    host: str = os.getenv('Mysql_host')
    user: str = os.getenv('Mysql_user')
    password: str = os.getenv('Mysql_password')
    database: str = os.getenv('Mysql_database')
    
@dataclass
class SqliteDatabaseConfig:
    database_path: str = os.getenv('SQLite_database')
    
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

PATTERN_FILE = Path(os.getenv('Sales_ETL_lineitem_patterns'))


def _load_pattern_lists() -> dict:
    with PATTERN_FILE.open() as pattern_file:
        return json.load(pattern_file)


def _compile_word_pattern(words: list[str]) -> re.Pattern:
    # Build a readable alternation and compile with word boundaries.
    choices = '|'.join(re.escape(word) for word in words)
    return re.compile(rf'\b({choices})\b', re.IGNORECASE)


def _compile_partial_pattern(words: list[str]) -> re.Pattern:
    # Allow garment matches to appear anywhere inside larger tokens (e.g., "hat" in "truckerhat").
    choices = '|'.join(re.escape(word) for word in words)
    return re.compile(rf'({choices})', re.IGNORECASE)


@dataclass
class LineitemPatternsConfig:
    _pattern_lists: dict = field(default_factory=_load_pattern_lists, repr=False)
    size_pattern: re.Pattern = field(init=False)
    color_pattern: re.Pattern = field(init=False)
    organization_pattern: re.Pattern = field(init=False)
    garment_patterns: re.Pattern = field(init=False)

    def __post_init__(self) -> None:
        lists = self._pattern_lists
        self.size_pattern = _compile_word_pattern(lists['sizes'])
        self.color_pattern = _compile_word_pattern(lists['colors'])
        self.organization_pattern = _compile_word_pattern(lists['organization_names'])
        self.garment_patterns = _compile_partial_pattern(lists['garments'])


# Functions to load configurations

def load_config() -> tuple[SquarePathsConfig, ShopifyPathsConfig, MysqlDatabaseConfig, ShopifyPathsConfig, SquareRawPipelineConfig, ShopifyRawPipelineConfig]:
    square_paths = SquarePathsConfig()
    shopify_paths = ShopifyPathsConfig()
    Mysqldb = MysqlDatabaseConfig()
    SqliteDB = SqliteDatabaseConfig()
    square_pipeline = SquareRawPipelineConfig()
    shopify_pipeline = ShopifyRawPipelineConfig()
    return square_paths, shopify_paths, Mysqldb, SqliteDB, square_pipeline, shopify_pipeline

def db_config() -> MysqlDatabaseConfig:
    Mysqldb = MysqlDatabaseConfig()
    return Mysqldb

def lineitem_patterns_config() -> LineitemPatternsConfig:
    patterns = LineitemPatternsConfig()
    return patterns
