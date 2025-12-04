from dataclasses import dataclass, field
import os
from dotenv import load_dotenv
load_dotenv()
from pathlib import Path
import json
import re


#----------------------------------------#
# Classes for path configuration
#----------------------------------------#


@dataclass
class OutputConfig:
    Output_dir: Path = Path(os.getenv('ETL_to_table_daily_output'))

@dataclass 
class InputConfig:
    Input_dir: str = str(Path(os.getenv('ETL_to_table_daily_csv_input')))



#----------------------------------------#
# Classes for database configuration
#----------------------------------------#

@dataclass
class MysqlDatabaseConfig:
    host: str = os.getenv('Mysql_host')
    user: str = os.getenv('Mysql_user')
    password: str = os.getenv('Mysql_password')
    database: str = os.getenv('Mysql_database')
    
@dataclass
class SqliteDatabaseConfig:
    database_path: str = os.getenv('SQLite_database')


#----------------------------------------#
# Classes for regex configuration
#----------------------------------------#

PATTERN_FILE: Path = Path(os.getenv('GTC_formated_tables_patterns')) 

def _load_pattern_lists() -> dict:
    with PATTERN_FILE.open() as pattern_file:
        return json.load(pattern_file)


def _compile_word_pattern(words: list[str]) -> re.Pattern:
    # Build a readable alternation and compile with word boundaries.
    choices = '|'.join(re.escape(word) for word in words)
    return re.compile(rf'\b({choices})\b', re.IGNORECASE)

@dataclass
class LineitemPatternsConfig:
    _pattern_lists: dict = field(default_factory=_load_pattern_lists, repr=False)
    size_pattern: re.Pattern = field(init=False)
    color_pattern: re.Pattern = field(init=False)
    organization_pattern: re.Pattern = field(init=False)

    def __post_init__(self) -> None:
        lists = self._pattern_lists
        self.size_pattern = _compile_word_pattern(lists['sizes'])
        self.color_pattern = _compile_word_pattern(lists['colors'])
        self.organization_pattern = _compile_word_pattern(lists['organization_names'])






#-------------------------------#
# function to load configurations
#-------------------------------#

def path_config() -> tuple[OutputConfig, InputConfig]:
    output_config = OutputConfig()
    input_config = InputConfig()
    return output_config, input_config


def db_config() -> tuple[MysqlDatabaseConfig, SqliteDatabaseConfig]:
    Mysqldb = MysqlDatabaseConfig()
    SqliteDB = SqliteDatabaseConfig()
    return Mysqldb, SqliteDB

def pattern_config() -> LineitemPatternsConfig:
    patterns = LineitemPatternsConfig()
    return patterns

