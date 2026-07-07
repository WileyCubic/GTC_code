from pathlib import Path
import logging
# from utils import setup_logging
# from Sales_ETL.common.config import db_config, logging_file_config
# from config import path_config, pipeline_config
# from extract import get_csv_files, csv_to_dataframe
# from raw_transform import transform_square, transform_shopify
# from load import create_mysql_table_if_replace, create_sqlite_table_if_replace
# from Sales_ETL.sql.functions.connections import (
#     create_mysql_engine,
#     sqlite_connection,
#     mysql_close_connection,
#     sqlite_close_connection,
# )

# logger = setup_logging(
#     primary_log_file_path = logging_file_config().raw_orders_log,
#     secondary_log_file_path= logging_file_config().Master_sales_ETL,
#     project_name= 'RawOrdersPipeline',
#     force_setup=True
# )

from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()

#----------------------------------------#
# Classes for path configuration
#----------------------------------------#

@dataclass
class SquarePathsConfig:
    input_dir: Path = Path(os.getenv('ELT_Sales_to_DB_square_CSV_input'))
    
@dataclass
class ShopifyPathsConfig:
    input_dir: Path = Path(os.getenv('ETL_Sales_to_DB_shopify_CSV_input'))

#----------------------------------------#    
# Classes for pipeline configuration
#----------------------------------------#

@dataclass
class SquareRawPipelineConfig:
    table_name: str = os.getenv('square_raw')
    csv_pattern: str = '*.csv'
    
@dataclass
class ShopifyRawPipelineConfig:
    table_name: str = os.getenv('shopify_raw')
    csv_pattern: str = '*.csv'

#----------------------------------------#
# Functions to load configurations
#----------------------------------------#

def path_config() -> tuple[SquarePathsConfig, ShopifyPathsConfig]:
    square_paths = SquarePathsConfig()
    shopify_paths = ShopifyPathsConfig()

    return square_paths, shopify_paths

def pipeline_config() -> tuple[SquareRawPipelineConfig, ShopifyRawPipelineConfig]:
    square_pipeline = SquareRawPipelineConfig()
    shopify_pipeline = ShopifyRawPipelineConfig()
    return square_pipeline, shopify_pipeline

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


import pandas as pd

#----------------------------------------#
# EXTRACTION CSV FUNCTIONS
#----------------------------------------#

# Get a list of CSV files from local folder

def get_csv_files(input_dir: Path, pattern: str = "*.csv") -> list[Path]:
    if not input_dir.exists():
        print(f'Input directory does not exist: {input_dir}')
        return []
    return [path for path in input_dir.glob(pattern)]

# Create DataFrame from CSV file(s)

def csv_to_dataframe(csv_path: list[Path]) -> pd.DataFrame:
    print(f'Reading CSV file(s): {len(csv_path)}')
    return pd.concat((pd.read_csv(f) for f in csv_path), ignore_index=True)


import pandas as pd
# from .utils import logger
import logging
import numpy as np
import datetime
from pathlib import Path



# # imports used to test processing functions
# from dotenv import load_dotenv
# import os
# ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
# load_dotenv(ENV_PATH)



# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

# square_data = pd.read_csv(os.getenv('square_raw_data'))
# shopify_data = pd.read_csv(os.getenv('shopify_raw_data'))




#----------------------------------------#
# ADDITIONAL UTILS FUNCTIONS
#----------------------------------------#

def format_phone_number(phone):
    if pd.isna(phone):
        return 0
    phone = str(int(float(phone))) # Convert to string and remove decimal if present
    try:
        if len(phone) == 10:
            return f'({phone[:3]})-{phone[3:6]}-{phone[6:]}'
        elif len(phone) == 11:
            return f'{phone[0]}-({phone[1:4]}) {phone[4:7]}-{phone[7:]}'  
        elif len(phone) == 9:
            return f'({phone[:2]}) {phone[2:5]}-{phone[5:]}'
        elif len(phone) == 12:
            return f'{phone[0:2]}-({phone[2:5]}) {phone[5:8]}-{phone[8:]}'
        else:
            return ValueError
    except Exception as e:
        print(f"Error formatting phone number {phone}: {e}")
        return phone



def transform_square(df):
    # fill na values
    
    df['Order ID'] = df.index + 1 # this is not in the first column of the dataframe
    
    try:                # convert is working
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='raise')
        print('no errors found in order_date conversion')
    except Exception as e:
        print(f'converting order_date: {e}')
        
    df['Order Subtotal'] = df['Order Subtotal'].astype(float).round(2)
    
    df['Order Shipping Price'] = df['Order Shipping Price'].astype(float).round(2).fillna(0)
    
    df['Order Tax Total'] = df['Order Tax Total'].astype(float).round(2).fillna(0)
    
    df['Order Total'] = df['Order Total'].astype(float).round(2)
    
    df['Order Refunded Amount'] = df['Order Refunded Amount'].astype(float).round(2).fillna(0)
    
    try:                # convert is working
        df['Fulfillment Date'] = pd.to_datetime(df['Fulfillment Date'], errors='raise')
        print('no errors found in fulfillment_date conversion')
    except Exception as e:
        print(f'converting fulfillment_date: {e}')
        
    df['Recipient Phone'] = df['Recipient Phone'].apply(format_phone_number).fillna(0)
    
    df['Item Price'] = df['Item Price'].astype(float).round(2)
    
    df['Item Options Total Price'] = df['Item Options Total Price'].astype(float).round(2)
    
    df['Item Total Price'] = df['Item Total Price'].astype(float).round(2)
    
    # Sort by Order Date and reset index
    df.sort_values(by='Order Date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    print('Square data transformation complete')

    return df


def transform_shopify(df):
    try: 
        # dropping cancelled orders and false orders
        df_mask = df['Cancelled at'].isna()
        df = df.loc[df_mask].copy()
        
        # remove unneeded apostrophes 
        
        df['Shipping Zip'] = df['Shipping Zip'].str.replace("'", "", regex=False)
        df['Billing Zip'] = df['Billing Zip'].str.replace("'", "", regex=False)
        
        # fill na values
        df.fillna({
                    'Notes': 'No Notes Given',
                    'Note Attributes': 'No Note Attributes Given',
                    'Billing Company': 'No Company Given',
                    'Shipping Company': 'No Company Given',
                    'Email': 'No Email Given',
                    }, inplace=True)        
        
        # fill na values in with values given under the same order id but in other rows
        
        df['Financial Status'] = df.groupby('Name')['Financial Status'].ffill().bfill()
        df['Paid at'] = df.groupby('Name')['Paid at'].ffill().bfill()
        df['Fulfillment Status'] = df.groupby('Name')['Fulfillment Status'].ffill().bfill()
        df['Fulfilled at'] = df.groupby('Name')['Fulfilled at'].ffill().bfill()
        
        df['Billing Name'] = df.groupby('Name')['Billing Name'].ffill().bfill()
        df['Billing Street'] = df.groupby('Name')['Billing Street'].ffill().bfill()
        df['Billing Address1'] = df.groupby('Name')['Billing Address1'].ffill().bfill()
        df['Billing City'] = df.groupby('Name')['Billing City'].ffill().bfill()
        df['Billing Zip'] = df.groupby('Name')['Billing Zip'].ffill().bfill()
        df['Billing Province'] = df.groupby('Name')['Billing Province'].ffill().bfill()
        df['Billing Country'] = df.groupby('Name')['Billing Country'].ffill().bfill()
        
        df['Shipping Name'] = df.groupby('Name')['Shipping Name'].ffill().bfill()
        df['Shipping Street'] = df.groupby('Name')['Shipping Street'].ffill().bfill()
        df['Shipping Address1'] = df.groupby('Name')['Shipping Address1'].ffill().bfill()
        df['Shipping City'] = df.groupby('Name')['Shipping City'].ffill().bfill()
        df['Shipping Zip'] = df.groupby('Name')['Shipping Zip'].ffill().bfill()
        df['Shipping Province'] = df.groupby('Name')['Shipping Province'].ffill().bfill()
        df['Shipping Country'] = df.groupby('Name')['Shipping Country'].ffill().bfill()
        
        df['Payment Reference'] = df.groupby('Name')['Payment Reference'].ffill().bfill()
        df['Id'] = df.groupby('Name')['Id'].ffill().bfill()
        df['Payment ID'] = df.groupby('Name')['Payment ID'].ffill().bfill()
        df['Payment Reference'] = df.groupby('Name')['Payment Reference'].ffill().bfill()

        # convert date columns
        
        df['Paid at'] = df['Paid at'].apply(datetime.datetime.date)
    
        df['Fulfilled at'] = df['Fulfilled at'].apply(datetime.datetime.date)
        
        df['Created at'] = df['Created at'].apply(datetime.datetime.date)
        
        # format phone numbers
        
        df['Billing Phone'] = df['Billing Phone'].apply(format_phone_number).fillna(0)
        
        df['Shipping Phone'] = df['Shipping Phone'].apply(format_phone_number).fillna(0)
        
        df['Phone'] = df['Phone'].apply(format_phone_number).fillna(0)
        
        #rounding floats to 2 decimal
        
        df['Subtotal'] = df['Subtotal'].astype(float).round(2)
        
        df['Shipping'] = df['Shipping'].astype(float).round(2)
        
        df['Taxes'] = df['Taxes'].astype(float).round(2)
        
        df['Total'] = df['Total'].astype(float).round(2)
        
        df['Lineitem price'] = df['Lineitem price'].astype(float).round(2)

        # Formating tax columns
        
        df.fillna({
            'Tax 1 Name': 'No Name Given',
            'Tax 1 Value': 0, 
            'Tax 2 Name': 'No Name Given',
            'Tax 2 Value': 0,
            'Tax 3 Name': 'No Name Given',
            'Tax 3 Value': 0,
            'Tax 4 Name': 'No Name Given',
            'Tax 4 Value': 0,
            'Tax 5 Name': 'No Name Given',
            'Tax 5 Value': 0
        }, inplace=True)
        df['Tax 1 Value'] = df['Tax 1 Value'].astype(float).round(2)
        df['Tax 2 Value'] = df['Tax 2 Value'].astype(float).round(2)
        df['Tax 3 Value'] = df['Tax 3 Value'].astype(float).round(2)
        df['Tax 4 Value'] = df['Tax 4 Value'].astype(float).round(2)
        df['Tax 5 Value'] = df['Tax 5 Value'].astype(float).round(2)     
        
    except Exception as e:
        print(f'converting: {e}')
    
    
    # Sort by Order Date and reset index
    df.sort_values(by='Name', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    print('Shopify data transformation complete')
    
    return df

# from .utils import logger
import logging
import pandas as pd


#----------------------------------------#
# LOAD TABLE FUNCTIONS
#----------------------------------------#

# Load DataFrame into MySQL table REPLACE IF EXISTS

def create_mysql_table_if_replace(df: pd.DataFrame, table_name: str, engine) -> None:
    """Create MySQL table if it does not exist based on DataFrame schema."""
    print(f"Creating table '{table_name}' in MySQL database")
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"Table '{table_name}' created successfully")
    
# Load DataFrame into SQLite table REPLACE IF EXISTS

def create_sqlite_table_if_replace(df: pd.DataFrame, table_name: str, conn) -> None:
    """Create SQLite table if it does not exist based on DataFrame schema."""
    print(f"Creating table '{table_name}' in SQLite database")
    df.to_sql(
        table_name,
        con=conn,
        if_exists="replace",
        index=False,
    )
    print(f"Table '{table_name}' created successfully")

import logging
# from Sales_ETL.common.config import MysqlDatabaseConfig
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
import sqlite3


#----------------------------------------#
# CREATE CONNECTIONS FUNCTIONS
#----------------------------------------#

# Create MySQL engine

def create_mysql_engine(config: MysqlDatabaseConfig) -> Engine:
    try:
        password = config.password
        url = f'mysql+mysqlconnector://{config.user}:{password}@{config.host}/{config.database}'
        print(f'{config.user} connected to MySQL database')
        return create_engine(url)
    except Exception as e:
        print(f"ERROR: connecting to MySQL database: {e}")
        return None

# Create SQLite connection

def sqlite_connection(db_path: str):
    try:
        conn = sqlite3.connect(db_path)
        print(f"Connected to SQLite database at {db_path}")
        return conn
    except Exception as e:
        print(f"ERROR: connecting to SQLite database: {e}")
        return None

#----------------------------------------#
# sqlite cursor FUNCTION
#----------------------------------------#

def sqlite_cursor(conn):
    try:
        cursor = conn.cursor()
        print("SQLite cursor created successfully")
        return cursor
    except Exception as e:
        print(f"ERROR: creating SQLite cursor: {e}")
        return None


#----------------------------------------#
# CLOSE CONNECTION FUNCTIONS
#----------------------------------------#

# Close MySQL engine connection

def mysql_close_connection(engine: Engine) -> None:
    try:
        engine.dispose()
        print("MySQL connection closed")
    except Exception as e:
        print(f"ERROR: closing MySQL connection: {e}")
        
# Close SQLite connection
        
def sqlite_close_connection(conn) -> None:
    try:
        conn.close()
        print("SQLite connection closed")
    except Exception as e:
        print(f"ERROR: closing SQLite connection: {e}")





print(r"""
 ____  _____  _    ____ _____    ____ ___ ____  _____ _     ___ _   _ _____ 
/ ___||_   _|/ \  |  _ \_   _|  |  _ \_ _|  _ \| ____| |   |_ _| \ | | ____|
\___ \  | | / _ \ | |_) || |    | |_) | || |_) |  _| | |    | ||  \| |  _|  
 ___) | | |/ ___ \|  _ < | |    |  __/| ||  __/| |___| |___ | || |\  | |___ 
|____/  |_/_/   \_\_| \_\|_|    |_|  |___|_|   |_____|_____|___|_| \_|_____|
            """)


def process_file(csv_path: Path, table_name: str, engine, source_name: str, conn) -> None:
    df = csv_to_dataframe(csv_path)
    if source_name == 'Square':
        df = transform_square(df)
    elif source_name == 'Shopify':
        df = transform_shopify(df)
    else:
        print(f"Unknown source name: {source_name}")
        return
    create_sqlite_table_if_replace(df, table_name=table_name, conn=conn)
    create_mysql_table_if_replace(df, table_name=table_name, engine=engine)

#----------------------------------------#
# MAIN PIPELINE FUNCTION
#----------------------------------------#

def run_pipeline() -> None:
    
    # Load configurations
    square_paths_cfg, shopify_paths_cfg = path_config()
    Mysql_cfg, Sqlite_cfg = db_config()
    square_pipeline_cfg, shopify_pipeline_cfg = pipeline_config()

    # Load database engine

    engine = create_mysql_engine(Mysql_cfg)
    
    # SQLite connection

    conn = sqlite_connection(Sqlite_cfg.database_path)
    
    # Process Square CSV files
    
    square_csv_files = get_csv_files(Path(square_paths_cfg.input_dir), square_pipeline_cfg.csv_pattern)
    if not square_csv_files:
        print(f"No Square CSV files found in {square_paths_cfg.input_dir}")
        return
    print(f"[Starting] ETL for {len(square_csv_files)} Square file(s) into '{square_pipeline_cfg.table_name}'")
    process_file(square_csv_files, table_name=square_pipeline_cfg.table_name, engine=engine, source_name='Square', conn=conn)
        
        
    # Process Shopify CSV files
    shopify_csv_files = get_csv_files(Path(shopify_paths_cfg.input_dir), shopify_pipeline_cfg.csv_pattern)
    if not shopify_csv_files:
        print(f"No Shopify CSV files found in {shopify_paths_cfg.input_dir}")
        return
    print(f"[Starting] ETL for {len(shopify_csv_files)} Shopify file(s) into '{shopify_pipeline_cfg.table_name}'")
    process_file(shopify_csv_files, table_name=shopify_pipeline_cfg.table_name, engine=engine, source_name='Shopify', conn=conn)

    print("[Completed] RAW ETL pipeline")
    

    # Close database connection
    mysql_close_connection(engine)
    sqlite_close_connection(conn)

if __name__ == "__main__":
    run_pipeline()



