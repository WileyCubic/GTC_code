from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

#----------------------------------------#
# Classes for path configuration
#----------------------------------------#

@dataclass
class QueryPathsConfig:
    all: Path = Path(os.getenv('all'))
    aquery1: Path = Path(os.getenv('aquery1'))
    aquery2: Path = Path(os.getenv('aquery2'))
    aquery3: Path = Path(os.getenv('aquery3'))
    aquery4: Path = Path(os.getenv('aquery4'))
    aquery5: Path = Path(os.getenv('aquery5'))
    aquery6: Path = Path(os.getenv('aquery6'))
    aquery7: Path = Path(os.getenv('aquery7'))
    aquery8: Path = Path(os.getenv('aquery8'))
    aquery9: Path = Path(os.getenv('aquery9'))
    aquery10: Path = Path(os.getenv('aquery10'))

    
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
# Functions to load configurations
#----------------------------------------#

def load_db_configs() -> tuple[MysqlDatabaseConfig, SqliteDatabaseConfig]:
    mysql_config = MysqlDatabaseConfig()
    sqlite_config = SqliteDatabaseConfig()
    return mysql_config, sqlite_config

def load_query_paths_config() -> QueryPathsConfig:
    query_config =  QueryPathsConfig()
    return query_config