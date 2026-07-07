import os
import sqlite3
import mysql.connector as mysql
import pandas as pd
from sqlalchemy import create_engine
from logging_utils.env import load_root_env
import numpy as np
load_root_env()
from datetime import datetime


# log_file = os.getenv('Query_log_file')

# def log(message):
#     now = datetime.now()
#     timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
#     with open(log_file, 'a' )as log:
#         log.write(f'{message}, {timestamp}\n')
#     print(f'Log entry added: {message}, {timestamp}')

# MySQL Database Connection
try:
    host= os.getenv('Mysql_host')
    user= os.getenv('Mysql_user')
    password= os.getenv('Mysql_password')
    database= os.getenv('Mysql_database')
    print("MySQL environment variables loaded successfully")
except Exception as e:
    print(f"ERROR: loading MySQL environment variables: {e}")

try:
    Mysql_connection = mysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    if Mysql_connection.is_connected():
        print("Successfully connected to MySQL database")
except mysql.Error as e:
    print(f"ERROR: connecting to MySQL Platform: {e}")


# Create a cursor object and MySQL query function

#MySQL database cursor
Mysql_cursor = Mysql_connection.cursor()
print("MySQL cursor created")

# MySQL SQLAlchemy engine
mysql_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
print("MySQL SQLAlchemy engine created successfully")

# #sqlite Database Connection
# sqlite_conn = sqlite3.connect(os.getenv('SQLite_database'))
# print("Connected to SQLite database")


# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    print(f'Executed: {query}')
    return df

# Query need to get data
query = '''
select `Order Date` as `Date`,
 `Order` as `Invoice ID`,
 `Item Name` as `Item Description`,
 `Order Name` as `Customer`, 
 `Recipient Country` as `Shipping Country`,
 `Recipient Postal Code` as `Shipping Postal Code`, 
 `Item Quantity` as `Number of Units`,
 `Item Price` as `Price Per Unit`,
 'Square' as `Source`
from square_raw
where `Order Date` >= '2026-01-01' -- Start Date
and `Order Date` <=  '2026-03-31 23:59:59' -- End Date
and `Item Name` like '%ncl%'

union all

select `Paid at` as `Date`,
 `Name` as `Invoice ID`,
 `Lineitem name` as `Item Description`,
 `Billing Name` as `Customer`,
 `Shipping Country` as `Shipping Country`,
 `Shipping Zip` as `Shipping Postal Code`,
 `Lineitem quantity` as `Number of Units`,
 `Lineitem price` as `Price Per Unit`,
 'Shopify' as `Source`
from shopify_raw
where `Paid at` >= '2026-01-01' -- Start Date
and `Paid at` <=  '2026-03-31 23:59:59' -- End Date

ORDER BY `Date` ASC
;
'''


def export_to_csv(df):
    #                                  Output Path                  Name of output file
    output_name = os.path.join(os.getenv('Royalty_report_out_dir'), 'Royalties_report_2026_Q1.csv')
    try:
        df.to_csv(output_name, index=False)
        print(f"Data exported to CSV")
    except Exception as e:
        print(f"ERROR: exporting data to CSV: {e}")

# run query and export csv

export_to_csv(sql_query(query, mysql_engine))


# Close MySQL connection

try:
    Mysql_cursor.close()
    Mysql_connection.close()
    print("MySQL connection closed")
except Exception as e:
    print(f"ERROR: closing MySQL connection: {e}")
    
    
