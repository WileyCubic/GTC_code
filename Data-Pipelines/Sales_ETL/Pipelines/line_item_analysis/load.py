from utils import logger
import logging
import pandas as pd

logger = logging.getLogger(__name__)

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

def create_sqlite_table_if_replace(df: pd.DataFrame, table_name: str, conn) -> None:
    """Create SQLite table if it does not exist based on DataFrame schema."""
    logger.info(f"Creating table '{table_name}' in SQLite database")
    df.to_sql(
        table_name,
        con=conn,
        if_exists="replace",
        index=False,
    )
    logger.info(f"Table '{table_name}' created successfully")

# Load DataFrame into 