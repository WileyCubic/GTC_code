from pathlib import Path
from Utils import logger
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.info("Starting ETL pipeline")

#----------------------------------------#
# EXTRACTION CSV FUNCTIONS
#----------------------------------------#

# Get a list of CSV files from local folder

def get_csv_files(input_dir: Path, pattern: str = "*.csv") -> list[Path]:
    if not input_dir.exists():
        logger.error(f'Input directory does not exist: {input_dir}')
        return []
    return [path for path in input_dir.glob(pattern)]

# Create DataFrame from CSV file(s)

def csv_to_dataframe(csv_path: list[Path]) -> pd.DataFrame:
    logger.info(f'Reading CSV file(s): {len(csv_path)}')
    return pd.concat((pd.read_csv(f) for f in csv_path), ignore_index=True)


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