import os
import sqlite3
import mysql.connector as mysql
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np
load_dotenv()
from datetime import datetime


log_file = os.getenv('Query_log_file')
#writing both to daily log and global log
def log(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a' )as log:
        log.write(f'{message}, {timestamp}\n')
    print(f'Log entry added: {message}, {timestamp}')

# MySQL Database Connection
try:
    host= os.getenv('Mysql_host')
    user= os.getenv('Mysql_user')
    password= os.getenv('Mysql_password')
    database= os.getenv('Mysql_database')
    log("MySQL environment variables loaded successfully")
except Exception as e:
    log(f"ERROR: loading MySQL environment variables: {e}")

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
    log(f"ERROR: connecting to MySQL Platform: {e}")


# Create a cursor object and MySQL query function

#MySQL database cursor
Mysql_cursor = Mysql_connection.cursor()
log("MySQL cursor created")

# MySQL SQLAlchemy engine
mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
log("MySQL SQLAlchemy engine created successfully")

# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    log(f'Executed: {query}')
    return df

# Query need to get data
shopify_query = '''
select `Paid at` as `Date`,
 `Name` as `Invoice ID`,
 `Lineitem name` as ` Item Description`,
 `Billing Name` as` Customer`,
 `Shipping Country` as `Shipping Country`,
 `Shipping Zip` as `Shipping Postal Code`,
 `Lineitem quantity` as `Number of Units`,
 `Lineitem price` as `Price Per Unit`
from shopify_orders
where `Paid at` >= '2025-07-01' 
and `Paid at` <=  '2025-09-30 23:59:59';
'''

squre_query = '''
select `Order Date` as `Date`,
 `Order` as `Invoice ID`,
 `Item Name` as ` Item Description`,
 `Order Name` as` Customer`, 
 `Recipient Country` as `Shipping Country`,
 `Recipient Postal Code` as `Shipping Postal Code`, 
 `Item Quantity` as `Number of Units`,
 `Item Price` as `Price Per Unit`
from square_orders
where `Order Date` >= '2025-07-01' 
and `Order Date` <=  '2025-09-30 23:59:59'
and `Item Name` like '%ncl%';
'''


def export_to_csv(df):
    #                                  Output Path                  Name of output file
    putput_name = os.path.join(os.getenv('Royalties_report_output'), 'shopify_data.csv')
    try:
        df.to_csv(putput_name, index=False)
        log(f"Data exported to CSV")
    except Exception as e:
        log(f"ERROR: exporting data to CSV: {e}")

# run query and export csv

export_to_csv(sql_query(shopify_query, mysql_engine))


# Close MySQL connection

try:
    Mysql_cursor.close()
    Mysql_connection.close()
    log("MySQL connection closed")
except Exception as e:
    log(f"ERROR: closing MySQL connection: {e}")
    
    
