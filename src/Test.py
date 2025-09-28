import os
import sqlite3
import mysql.connector as mysql
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np
load_dotenv()
from datetime import datetime


log_file = os.getenv('test_log_file')
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

# Function to execute MySQL queries
def sql_query(query, connection):
    df = pd.read_sql(query, connection)
    print(f'Executed: {query}')
    log(f'Executed: {query}')
    return df

query = "SELECT * FROM ab_expences"
ab_df = sql_query(query, Mysql_connection)
print(ab_df.info())




try:
    Mysql_cursor.close()
    Mysql_connection.close()
    log("MySQL connection closed")
except Exception as e:
    log(f"ERROR: closing MySQL connection: {e}")