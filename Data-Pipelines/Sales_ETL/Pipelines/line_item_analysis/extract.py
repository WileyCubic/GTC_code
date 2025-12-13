from pathlib import Path
from utils import logger
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.info("Starting ETL pipeline")


#----------------------------------------#
# EXTRACTION QUERIES FUNCTIONS
#----------------------------------------#

# Query data from any SQL connection using a query file

def query_data_from_file(connection, query_file: Path) -> pd.DataFrame:
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