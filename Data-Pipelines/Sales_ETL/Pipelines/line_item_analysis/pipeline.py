from pathlib import Path
import logging
from utils import logger
from common.config import db_config
from config import path_config, pipeline_config
from extract import query_data_from_file
from load import create_mysql_table_if_replace, create_sqlite_table_if_replace
from lineitem_analysis_transform import transform_lineitem_analysis
from sql.functions.connections import create_mysql_engine, sqlite_connection, sqlite_close_connection, mysql_close_connection

logger = logging.getLogger(__name__)
logger.info("[Starting] ETL pipeline")

    
def process_query(function, query_file: Path, table_name: str, engine, conn) -> None:
    df = query_data_from_file(engine, query_file)
    logger.info(f'Extracted Data for {table_name} from query file {query_file.name}')
    transformed_df = function(df)
    logger.info(f'Transformed Data for {table_name} using {function.__name__}')
    create_sqlite_table_if_replace(transformed_df, table_name=table_name, conn=conn)
    create_mysql_table_if_replace(transformed_df, table_name=table_name, engine=engine)


#----------------------------------------#
# MAIN PIPELINE FUNCTION
#----------------------------------------#

def run_pipeline() -> None:
    
    # Load configurations
    query_paths_cfg = path_config()
    Mysql_cfg, Sqlite_cfg = db_config()
    lineitem_pipeline_cfg = pipeline_config()

    # Load database engine

    engine = create_mysql_engine(Mysql_cfg)
    
    # SQLite connection

    conn = sqlite_connection(Sqlite_cfg.database_path)
    
    # add steps needed for a table to be made for lineitem analysis

    logger.info(f"[Starting] ETL for Lineitem Analysis into '{lineitem_pipeline_cfg.table_name}'")

    process_query(function=transform_lineitem_analysis, query_file=query_paths_cfg.lineitem_analysis, table_name=lineitem_pipeline_cfg.table_name, engine=engine, conn=conn)
    
    logger.info("[Completed] Lineitem Analysis ETL pipeline")

    # Close database connection
    mysql_close_connection(engine)
    sqlite_close_connection(conn)

if __name__ == "__main__":
    run_pipeline()



