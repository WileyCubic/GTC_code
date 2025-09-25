import pandas as pd
import numpy as np
import sqlite3 
from datetime import datetime
import mysql.connector as mysql
from sqlalchemy import create_engine
import glob 
import os
import re
from dotenv import load_dotenv
load_dotenv()

# NO ORDERS PRIOR TO 08/01/2024
# DO NOT PULL ORDER LOGS PRIOR TO 08/01/2024

# set pandas display for building and testing purposes
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Optional: Show all columns
pd.set_option('display.width', None)  # Optional: Prevent line wrapping
pd.set_option('display.max_colwidth', None)  # Optional: Show full column content]
pd.set_option('display.float_format', '{:.6f}'.format)  # Optional: Format floats to 2 decimal places

#------------------------------#
# log processing
#------------------------------#

#writing both to daily log and global log
def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')

# log file path
log_file = os.getenv('ETL_CSV_to_DB_log_file')

log('ETL CSV to DB process started')

#------------------------------#
# Extracting data from csv files
#------------------------------#

# input file paths
try:
    order_drump_square = os.getenv('ELT_CSV_to_DB_square_CSV_input')
    square_csv_files = glob.glob(order_drump_square + '/*.csv')
    square_csv_files
    log(f'Square CSV files found: {square_csv_files}')
    print(f'Square CSV files found: {square_csv_files}')
except Exception as e:
    print(f"Error finding Square CSV files: {e}")
    log(f"Error finding Square CSV files: {e}")

try:
    order_dump_shopify = os.getenv('ETL_CSV_to_DB_shopify_CSV_input')
    shopify_csv_files = glob.glob(order_dump_shopify + '/*.csv')
    shopify_csv_files
    log(f'Shopify CSV files found: {shopify_csv_files}')
    print(f'Shopify CSV files found: {shopify_csv_files}')
except Exception as e:
    print(f"Error finding Shopify CSV files: {e}")
    log(f"Error finding Shopify CSV files: {e}")

#------------------------------#
# SQL Database Connections
#------------------------------#

# SQLite Database Connection
try:
    SQLite_connection = sqlite3.connect(os.getenv('SQLite_database'))
    log("Successfully connected to SQLite database")
except Exception as e:
    print(f"Error connecting to SQLite database: {e}")
    log(f"Error connecting to SQLite database: {e}")

# MySQL Database Connection
try:
    host= os.getenv('Mysql_host')
    user= os.getenv('Mysql_user')
    password= os.getenv('Mysql_password')
    database= os.getenv('Mysql_database')
    log("MySQL environment variables loaded successfully")
except Exception as e:
    print(f"Error loading MySQL environment variables: {e}")
    log(f"Error loading MySQL environment variables: {e}")

try:
    Mysql_connection = mysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    if Mysql_connection.is_connected():
        print("Successfully connected to MySQL database")
        log("Successfully connected to MySQL database")
except mysql.Error as e:
    print(f"Error connecting to MySQL Platform: {e}")
    log(f"Error connecting to MySQL Platform: {e}")

#create sqlalchemy engine for mysql
mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
log("MySQL SQLAlchemy engine created successfully")

# Create a cursor object and MySQL query function

#MySQL database cursor
Mysql_cursor = Mysql_connection.cursor()
log("MySQL cursor created")

# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    print(f'Executed: {query}')
    log(f'Executed: {query}')
    return df

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

# Input CSV attributes

square_attribute_list = [
    'Order',
    'Order Name',
    'Order Date',
    'Currency',
    'Order Subtotal', 
    'Order Shipping Price',
    'Order Tax Total',
    'Order Total',
    'Order Refunded Amount',
    'Fulfillment Date',
    'Fulfillment Type',
    'Fulfillment Status',
    'Channels',
    'Fulfillment Location',
    'Fulfillment Notes',
    'Recipient Name',
    'Recipient Email',
    'Recipient Phone',
    'Recipient Address',
    'Recipient Address 2',
    'Recipient Postal Code',
    'Recipient City',
    'Recipient Region',
    'Recipient Country',
    'Item Quantity',
    'Item Name',
    'Item SKU',
    'Item Variation',
    'Item Modifiers',
    'Item Price',
    'Item Options Total Price',
    'Item Total Price'
]

shopify_attribute_list = [
    'Name',
    'Email',
    'Financial Status',
    'Paid at',
    'Fulfillment Status',
    'Fulfilled at',
    'Accepts Marketing',
    'Currency',
    'Subtotal',
    'Shipping',
    'Taxes',
    'Total',
    'Discount Code',
    'Discount Amount',
    'Shipping Method',
    'Created at',
    'Lineitem quantity',
    'Lineitem name',
    'Lineitem price',
    'Lineitem compare at price',
    'Lineitem sku',
    'Lineitem requires shipping',
    'Lineitem taxable',
    'Lineitem fulfillment status',
    'Billing Name',
    'Billing Street',
    'Billing Address1',
    'Billing Address2',
    'Billing Company',
    'Billing City',
    'Billing Zip',
    'Billing Province',
    'Billing Country',
    'Billing Phone',
    'Shipping Name',
    'Shipping Street',
    'Shipping Address1',
    'Shipping Address2',
    'Shipping Company',
    'Shipping City',
    'Shipping Zip',
    'Shipping Province',
    'Shipping Country',
    'Shipping Phone',
    'Notes',
    'Note Attributes',
    'Cancelled at',
    'Payment Method',
    'Payment Reference',
    'Refunded Amount',
    'Vendor',
    'Outstanding Balance',
    'Employee',
    'Location',
    'Device ID',
    'Id',
    'Tags',
    'Risk Level',
    'Source',
    'Lineitem discount',
    'Tax 1 Name',
    'Tax 1 Value',
    'Tax 2 Name',
    'Tax 2 Value',
    'Tax 3 Name',
    'Tax 3 Value',
    'Tax 4 Name',
    'Tax 4 Value',
    'Tax 5 Name',
    'Tax 5 Value',
    'Phone',
    'Receipt Number',
    'Duties',
    'Billing Province Name',
    'Shipping Province Name',
    'Payment ID',
    'Payment Terms Name',
    'Next Payment Due At',
    'Payment References'
]

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
        print(f'Error formatting phone number {phone}: {e}')
        log(f'Error formatting phone number {phone}: {e}')
        return phone

#------------------------------#
# Functions realating to square data extraction and transformation
#------------------------------#

# Function to read CSV and convert to DataFrame 
def square_input_csv_to_df(file_path):
        df = pd.concat((pd.read_csv(f) for f in file_path), ignore_index=True)
        return df

def transform_square(df):
    # fill na values
    
    df['Order ID'] = df.index + 1 # this is not in the first column of the dataframe
    
    try:                # convert is working
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='raise')
        print('no errors found in order_date conversion')
        log('no errors found in order_date conversion')
    except Exception as e:
        print(f'Error converting order_date: {e}')
        log(f'Error converting order_date: {e}')
        
    df['Order Subtotal'] = df['Order Subtotal'].astype(float).round(2)
    
    df['Order Shipping Price'] = df['Order Shipping Price'].astype(float).round(2).fillna(0)
    
    df['Order Tax Total'] = df['Order Tax Total'].astype(float).round(2).fillna(0)
    
    df['Order Total'] = df['Order Total'].astype(float).round(2)
    
    df['Order Refunded Amount'] = df['Order Refunded Amount'].astype(float).round(2).fillna(0)
    
    try:                # convert is working
        df['Fulfillment Date'] = pd.to_datetime(df['Fulfillment Date'], errors='raise')
        print('no errors found in fulfillment_date conversion')
        log('no errors found in fulfillment_date conversion')
    except Exception as e:
        print(f'Error converting fulfillment_date: {e}')
        log(f'Error converting fulfillment_date: {e}')
        
    df['Recipient Phone'] = df['Recipient Phone'].apply(format_phone_number).fillna(0)
    
    df['Item Price'] = df['Item Price'].astype(float).round(2)
    
    df['Item Options Total Price'] = df['Item Options Total Price'].astype(float).round(2)
    
    df['Item Total Price'] = df['Item Total Price'].astype(float).round(2)
    
    # Sort by Order Date and reset index
    df.sort_values(by='Order Date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    log('Square data transformation complete')

    return df

#------------------------------#
# Functions realating to Shopify data extraction and transformation
#------------------------------#

# Function to read CSV and convert to DataFrame 
def shopify_input_csv_to_df(file_path):
        df = pd.concat((pd.read_csv(f) for f in file_path), ignore_index=True)
        return df

def transform_shopify(df):
    try: 
        
        # fill na values
        df.fillna({
                    'Notes': 'No Notes Given',
                    'Note Attributes': 'No Note Attributes Given',
                    'Billing Company': 'No Company Given',
                    'Shipping Company': 'No Company Given',
                    'Email': 'No Email Given',
                    }, inplace=True)        
        
        # convert date columns
        
        df['Paid at'] = pd.to_datetime(df['Paid at'], errors='coerce')
    
        df['Fulfilled at'] = pd.to_datetime(df['Fulfilled at'], errors='coerce')
        
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
        
        df['Created at'] = pd.to_datetime(df['Created at'], errors='raise')
    
        df.drop(df[df['Cancelled at'].notna()].index, inplace=True)
        
    except Exception as e:
        print(f'Error converting: {e}')
        log(f'Error converting: {e}')
    
    
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
    print(f'Table {table_name} created in SQLite database.')
    log(f'Table {table_name} created in SQLite database.')



# sql query to create square orders table
def create_square_orders_table(df):
    df.to_sql('square_orders', mysql_engine, if_exists='replace', index=False)
    print('Table square_orders created in MySQL database.')
    log('Table square_orders created in MySQL database.')

# sql query to create shopify orders table
def create_shopify_orders_table(df):
    df.to_sql('shopify_orders', mysql_engine, if_exists='replace', index=False)
    print('Table shopify_orders created in MySQL database.')
    log('Table shopify_orders created in MySQL database.')

#------------------------------#
# All functions are working
#------------------------------#   


#------------------------------#
# Main Process
#------------------------------# 

#shopify

shopify_input_df = shopify_input_csv_to_df(shopify_csv_files)
log('Shopify CSV files converted to dataframe')

transformed_shopify_df = transform_shopify(shopify_input_df)
log('Shopify dataframe cleaned for loading')

df_to_sqlite(transformed_shopify_df, 'shopify_orders')
log('Shopify dataframe loaded to SQLite database')

create_shopify_orders_table(transformed_shopify_df)
log('Shopify dataframe loaded to MySQL database')

#square

square_input_df = square_input_csv_to_df(square_csv_files)
log('Square CSV files converted to dataframe')

transformed_square_df = transform_square(square_input_df)
log('Square dataframe cleaned for loading')

df_to_sqlite(transformed_square_df, 'square_orders')
log('Square dataframe loaded to SQLite database')

create_square_orders_table(transformed_square_df)
log('Square dataframe loaded to MySQL database')
