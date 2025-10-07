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

# set pandas display for building and testing purposes
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


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
log_file = os.getenv('ETL_Expences_to_DB_log_file')

log('ETL Expences to DB process started')

#------------------------------#
# SQL Database Connections
#------------------------------#

# SQLite Database Connection
try:
    SQLite_connection = sqlite3.connect(os.getenv('SQLite_database'))
    log("Successfully connected to SQLite database")
except Exception as e:
    log(f"ERROR: connecting to SQLite database: {e}")

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
SQLite_cursor.execute('drop table if exists bp_expences')
SQLite_cursor.execute('drop table if exists ab_expences')
SQLite_connection.commit()
log("Dropped SQLite tables orders if it existed")

# Drop MySQL tables if it exists
Mysql_cursor.execute('drop table if exists bp_expences')
Mysql_cursor.execute('drop table if exists ab_expences')
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
    files_bp = os.getenv('ETL_Expences_to_DB_BP_CSV_input')
    bp_csv_files = glob.glob(files_bp + '/*.csv')
    bp_csv_files
    log(f'bp CSV files found: {len(bp_csv_files)}')
except Exception as e:
    log(f"ERROR: finding bp CSV files: {e}")

try:
    files_ab = os.getenv('ETL_Expences_to_DB_AB_CSV_input')
    ab_csv_files = glob.glob(files_ab + '/*.csv')
    ab_csv_files
    log(f'ab CSV files found: {len(ab_csv_files)}')
except Exception as e:
    log(f"ERROR: finding ab CSV files: {e}")

#------------------------------#
# covert csv files to dataframes
#------------------------------#

# works for both bp and ab files
def csv_to_df(files):
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    return df
#------------------------------#
# Transformaing dataframes
#------------------------------#

def transform_bp(df):
    try:
        
        # remove rows not needed
        remove_card_members = [os.getenv('Card_Member_1'), os.getenv('Card_Member_4')]
        remove_descriptions = [os.getenv('PMT_Line_1'), os.getenv('PMT_Line_2')]

        mask = ~df['Card Member'].isin(remove_card_members) & ~df['Description'].isin(remove_descriptions)
        df = df.loc[mask].copy()
        
        # Fill nan values
        
        df.fillna({
            'Address': 'Not Provided',
            'City/State': 'Not Provided',
            'Zip Code': 'Not Provided',
            'Country': 'Not Provided',
            'Reference': 'Not Provided',
            'Category': 'Not Provided',    
        })
        
        # formate date to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        
        # round to 2 decimal places
        df['Amount'] = df['Amount'].astype(float).round(2)
        
        # clean up value formatting
        df.replace({
            'Extended Details': {'\n': ', '},
            'City/State': {'\n': ', '}
        }, regex=True)
        
        df['Account #'] = df['Account #'].replace('-', '', regex=False)
        
        df.sort_values(by='Date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        return df
    
    except Exception as e:
        log(f"ERROR: transforming table: {e}")
        
        
def transform_ab(df):
    try:
        
        # remove rows not needed
        remove_descriptions = [os.getenv('PMT_Line_1'), os.getenv('PMT_Line_2')]
        mask = ~df['Description'].isin(remove_descriptions)
        df = df.loc[mask].copy()
        
        # Fill nan values
        
        df.fillna({
            'Address': 'Not Provided',
            'City/State': 'Not Provided',
            'Zip Code': 'Not Provided',
            'Country': 'Not Provided',
            'Reference': 'Not Provided',
            'Category': 'Not Provided',    
        })
        
        # formate date to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        
        # round to 2 decimal places
        df['Amount'] = df['Amount'].astype(float).round(2)
        
        # clean up value formatting
        df.replace({
            'Extended Details': {'\n': ', '},
            'City/State': {'\n': ', '}
        }, regex=True)
        
        df.sort_values(by='Date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        return df
    
    except Exception as e:
        log(f"ERROR: transforming table: {e}")
  
#------------------------------#
# Load dataframes to SQL databases
#------------------------------#

# Load to SQLite db works for both
def df_to_sqlite(df, table_name):
    df.to_sql(table_name, SQLite_connection, if_exists='replace', index=False)
    log(f'Table {table_name} created in SQLite database.')

# sql query to create bp_expences table
def create_mysql_table(df, table_name):
    df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)
    log(f'Table {table_name} created in MySQL database.')

#------------------------------#
# ETL Process
#------------------------------#

# ETL for bp files
try:
    bp_df = csv_to_df(bp_csv_files)
    log("bp CSV files loaded to df")
    bp_df = transform_bp(bp_df)
    log("bp df transformed")
    df_to_sqlite(bp_df, 'bp_expences')
    log("bp df loaded to SQLite")
    create_mysql_table(bp_df, 'bp_expences')
    log("bp df loaded to MySQL")
except Exception as e:
    log(f"ERROR: in bp ELT process: {e}")

# ETL for ab files
try:
    ab_df = csv_to_df(ab_csv_files)
    log("ab CSV files loaded to df")
    ab_df = transform_ab(ab_df)
    log("ab df transformed")
    df_to_sqlite(ab_df, 'ab_expences')
    log("ab df loaded to SQLite")
    create_mysql_table(ab_df, 'ab_expences')
    log("ab df loaded to MySQL")
except Exception as e:
    log(f"ERROR: in ab ELT process: {e}")
    
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
