import logging
from common.utils import logger
from common.config import MysqlDatabaseConfig
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
# sqlite cursor FUNCTION
#----------------------------------------#

def sqlite_cursor(conn):
    try:
        cursor = conn.cursor()
        logger.info("SQLite cursor created successfully")
        return cursor
    except Exception as e:
        logger.error(f"ERROR: creating SQLite cursor: {e}")
        return None


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