import logging
from Utils import logger
from config import MysqlDatabaseConfig
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
import sqlite3

    
logger = logging.getLogger(__name__)
logger.info("Starting ETL pipeline")

#----------------------------------------#
# CREATE CONNECTIONS FUNCTIONS
#----------------------------------------#

# Create MySQL engine

def create_mysql_engine(config: MysqlDatabaseConfig) -> Engine:
    try:
        password = config.password
        url = f'mysql+mysqlconnector://{config.user}:{password}@{config.host}/{config.database}'
        logger.info(f'{config.user} connected to MySQL database')
        return create_engine(url)
    except Exception as e:
        logger.error(f"ERROR: connecting to MySQL database: {e}")
        return None

# Create SQLite connection

def sqlite_connection(db_path: str):
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to SQLite database at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"ERROR: connecting to SQLite database: {e}")
        return None

#----------------------------------------#
# LOAD TABLE FUNCTIONS
#----------------------------------------#

# Load DataFrame into MySQL table REPLACE IF EXISTS

def create_mysql_table_if_replace(df: pd.DataFrame, table_name: str, engine) -> None:
    """Create MySQL table if it does not exist based on DataFrame schema."""
    logger.info(f"Creating table '{table_name}' in MySQL database")
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )
    logger.info(f"Table '{table_name}' created successfully")
    
# Load DataFrame into SQLite table REPLACE IF EXISTS

def sqlite_table_if_replace(df: pd.DataFrame, table_name: str, conn) -> None:
    """Create SQLite table if it does not exist based on DataFrame schema."""
    logger.info(f"Creating table '{table_name}' in SQLite database")
    df.to_sql(
        table_name,
        con=conn,
        if_exists="replace",
        index=False,
    )
    logger.info(f"Table '{table_name}' created successfully")

#----------------------------------------#
# CLOSE CONNECTION FUNCTIONS
#----------------------------------------#

# Close MySQL engine connection

def mysql_close_connection(engine: Engine) -> None:
    try:
        engine.dispose()
        logger.info("MySQL connection closed")
    except Exception as e:
        logger.error(f"ERROR: closing MySQL connection: {e}")
        
# Close SQLite connection
        
def sqlite_close_connection(conn) -> None:
    try:
        conn.close()
        logger.info("SQLite connection closed")
    except Exception as e:
        logger.error(f"ERROR: closing SQLite connection: {e}")