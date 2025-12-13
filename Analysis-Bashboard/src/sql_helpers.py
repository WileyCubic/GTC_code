import logging
from Utils import logger
from config import MysqlDatabaseConfig
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
import sqlite3
from pathlib import Path

    
logger = logging.getLogger(__name__)
logger.info("sql_helpers module loaded")

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

#-----------------------------------------#
# EXECUTE QUERY FUNCTION
#-----------------------------------------#

def query_data(connection, query_file: Path) -> pd.DataFrame:
    logger.info(f'Executing query from file: {query_file.name}')
    with query_file.open('r') as file:
        query = file.read()
    logger.info(f'Running: {query_file.name}')
    try:
        df = pd.read_sql_query(query, connection)
        logger.info(f'Query executed successfully, retrieved {len(df)} records')
    except Exception as e:
        logger.error(f'Error executing query from {query_file.name}: {e}')
    return df

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
