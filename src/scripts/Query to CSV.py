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

#sqlite Database Connection
sqlite_conn = sqlite3.connect(os.getenv('SQLite_database'))
log("Connected to SQLite database")


# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    log(f'Executed: {query}')
    return df

# Query need to get data
query = '''

'''


def export_to_csv(df):
    #                                  Output Path                  Name of output file
    putput_name = os.path.join(os.getenv('Royalties_report_output'), '_____.csv')
    try:
        df.to_csv(putput_name, index=False)
        log(f"Data exported to CSV")
    except Exception as e:
        log(f"ERROR: exporting data to CSV: {e}")

# run query and export csv

export_to_csv(sql_query(query, sqlite_conn))


# Close MySQL connection

try:
    Mysql_cursor.close()
    Mysql_connection.close()
    log("MySQL connection closed")
except Exception as e:
    log(f"ERROR: closing MySQL connection: {e}")
    
    
