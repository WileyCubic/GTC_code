from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

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
# Class for logging configuration
#----------------------------------------#

@dataclass
class LoggingFileCongigs:
    raw_orders_log: Path = Path(os.getenv('raw_orders_log'))
    lineitem_analysis_log: Path = Path(os.getenv('lineitem_analysis_log'))
    
    Master_sales_ETL: Path = Path(os.getenv('Master_sales_log'))
    Master_expenses_ETL: Path = Path(os.getenv('Master_expences_log'))
    
    Backup_log: Path = Path(os.getenv('Backup_log'))


#----------------------------------------#
# Functions to load configurations
#----------------------------------------#

def db_config() -> tuple[MysqlDatabaseConfig, SqliteDatabaseConfig]:
    Mysqldb = MysqlDatabaseConfig()
    SqliteDB = SqliteDatabaseConfig()
    return Mysqldb, SqliteDB

def logging_file_config() -> LoggingFileCongigs:
    log_files = LoggingFileCongigs()
    return log_files
