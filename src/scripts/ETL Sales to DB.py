import pandas as pd
import numpy as np
import sqlite3 
from datetime import datetime
import mysql.connector as mysql
from sqlalchemy import create_engine
import glob 
import os
from dotenv import load_dotenv
load_dotenv()

# NO ORDERS PRIOR TO 08/01/2024
# DO NOT PULL ORDER LOGS PRIOR TO 08/01/2024

# set pandas display for building and testing purposes
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.6f}'.format)

#------------------------------#
# log processing
#------------------------------#


def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')

# log file path
log_file = os.getenv('ETL_Sales_to_DB_log_file')

log('ETL CSV to DB process started')

#------------------------------#
# SQL Database Connections
#------------------------------#

# SQLite Database Connection
try:
    SQLite_connection = sqlite3.connect(os.getenv('SQLite_database'))
    log("Successfully connected to SQLite database")
except Exception as e:
    log(f"ERROR connecting to SQLite database: {e}")

# MySQL Database Connection
try:
    host= os.getenv('Mysql_host')
    user= os.getenv('Mysql_user')
    password= os.getenv('Mysql_password')
    database= os.getenv('Mysql_database')
    log("MySQL environment variables loaded successfully")
except Exception as e:
    log(f"ERROR loading MySQL environment variables: {e}")

try:
    Mysql_connection = mysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    if Mysql_connection.is_connected():
        log("Successfully connected to MySQL database")
except mysql.Error as e:
    log(f"ERROR connecting to MySQL Platform: {e}")

#create sqlalchemy engine for mysql
mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
log("MySQL SQLAlchemy engine created successfully")

# Create a cursor object and MySQL query function

#MySQL database cursor
Mysql_cursor = Mysql_connection.cursor()
log("MySQL cursor created")

#SQLite database cursor
SQLite_cursor = SQLite_connection.cursor()
log("SQLite cursor created")

#------------------------------#
# Drop existing tables if they exist
#------------------------------#

#Drop SQLite tables if it exists 
SQLite_cursor.execute('drop table if exists square_orders')
SQLite_cursor.execute('drop table if exists shopify_orders')
SQLite_connection.commit()
log("Dropped SQLite tables orders if it existed")

# Drop MySQL tables if it exists
Mysql_cursor.execute('drop table if exists square_orders')
Mysql_cursor.execute('drop table if exists shopify_orders')
Mysql_connection.commit()
log("Dropped MySQL tables orders if it existed")

#------------------------------#
# Data Extraction and Transformation
#------------------------------#

#------------------------------#
# Extracting data from csv files
#------------------------------#

# input file paths
try:
    order_drump_square = os.getenv('ELT_Sales_to_DB_square_CSV_input')
    square_csv_files = glob.glob(order_drump_square + '/*.csv')
    square_csv_files
    log(f'Square CSV files found: {len(square_csv_files)}')
except Exception as e:
    log(f"ERROR finding Square CSV files: {e}")

try:
    order_dump_shopify = os.getenv('ETL_Sales_to_DB_shopify_CSV_input')
    shopify_csv_files = glob.glob(order_dump_shopify + '/*.csv')
    shopify_csv_files
    log(f'Shopify CSV files found: {len(shopify_csv_files)}')
except Exception as e:
    log(f"ERROR finding Shopify CSV files: {e}")


# Define a function to transform Recipient Phone Numbers to x (xxx) xxx-xxxx format
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
        log(f'ERROR formatting phone number {phone}: {e}')
        return phone

#------------------------------#
# covert csv files to dataframes
#------------------------------#

# Function to read CSV and convert to DataFrame 
def input_csv_to_df(file_path):
        df = pd.concat((pd.read_csv(f) for f in file_path), ignore_index=True)
        return df

#------------------------------#
# Transformaing dataframes
#------------------------------#

def transform_square(df):
    # fill na values
    
    df['Order ID'] = df.index + 1 # this is not in the first column of the dataframe
    
    try:                # convert is working
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='raise')
        log('no errors found in order_date conversion')
    except Exception as e:
        log(f'ERROR converting order_date: {e}')
        
    df['Order Subtotal'] = df['Order Subtotal'].astype(float).round(2)
    
    df['Order Shipping Price'] = df['Order Shipping Price'].astype(float).round(2).fillna(0)
    
    df['Order Tax Total'] = df['Order Tax Total'].astype(float).round(2).fillna(0)
    
    df['Order Total'] = df['Order Total'].astype(float).round(2)
    
    df['Order Refunded Amount'] = df['Order Refunded Amount'].astype(float).round(2).fillna(0)
    
    try:                # convert is working
        df['Fulfillment Date'] = pd.to_datetime(df['Fulfillment Date'], errors='raise')
        log('no errors found in fulfillment_date conversion')
    except Exception as e:
        log(f'ERROR: converting fulfillment_date: {e}')
        
    df['Recipient Phone'] = df['Recipient Phone'].apply(format_phone_number).fillna(0)
    
    df['Item Price'] = df['Item Price'].astype(float).round(2)
    
    df['Item Options Total Price'] = df['Item Options Total Price'].astype(float).round(2)
    
    df['Item Total Price'] = df['Item Total Price'].astype(float).round(2)
    
    # Sort by Order Date and reset index
    df.sort_values(by='Order Date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    log('Square data transformation complete')

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
        
        df['Paid at'] = pd.to_datetime(df['Paid at'], errors='coerce')
    
        df['Fulfilled at'] = pd.to_datetime(df['Fulfilled at'], errors='coerce')
        
        df['Created at'] = pd.to_datetime(df['Created at'], errors='raise')
        
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
        log(f'ERROR converting: {e}')
    
    
    # Sort by Order Date and reset index
    df.sort_values(by='Name', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    log('Shopify data transformation complete')
    
    return df

#------------------------------#
# Functions realating to Loading data to SQL databeses
#------------------------------#

# Load to SQLite db works for both square and shopify dataframes
def df_to_sqlite(df, table_name):
    df.to_sql(table_name, SQLite_connection, if_exists='replace', index=False)
    log(f'Table {table_name} created in SQLite database.')

# sql query to create orders table
def create_mysql_table(df, table_name):
    df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)
    log(f'Table {table_name} created in MySQL database.')

#------------------------------#
# ETL Process
#------------------------------# 

#shopify

try:
    shopify_df = input_csv_to_df(shopify_csv_files)
    log('Shopify CSV files loaded to df')
    shopify_df = transform_shopify(shopify_df)
    log('Shopify df transformed')
    # df_to_sqlite(shopify_df, 'raw_shopify_orders')
    # log('Shopify df loaded to SQLite')
    create_mysql_table(shopify_df, 'raw_shopify_orders')
    log('Shopify df loaded to MySQL')
except Exception as e:
    log(f"ERROR: in Shopify ELT process: {e}")  
    
#square

try:
    square_df = input_csv_to_df(square_csv_files)
    log('Square CSV files loaded to df')
    square_df = transform_square(square_df)
    log('Square df transformed')
    df_to_sqlite(square_df, 'square_orders')
    log('Square df loaded to SQLite')
    create_mysql_table(square_df, 'raw_square_orders')
    log('Square df loaded to MySQL')
except Exception as e:
    log(f"ERROR: in Square ELT process: {e}")
    
#------------------------------#
# Close database connections
#------------------------------#
def close_connections():
    try:
        SQLite_connection.close()
        log("SQLite connection closed")
    except Exception as e:
        log(f"ERROR: closing SQLite connection: {e}")
    try:
        Mysql_cursor.close()
        Mysql_connection.close()
        log("MySQL connection closed")
    except Exception as e:
        log(f"ERROR: closing MySQL connection: {e}")

close_connections()
log('ETL Expences to DB process completed')

